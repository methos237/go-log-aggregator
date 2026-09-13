// Package tail is the live-tail subscription registry: it turns the fan-out
// copies the ingest path publishes into per-client streams of matched records.
//
// Every accepted batch is copied to a core NATS subject by the collector that
// received it (queue.Publisher.Fanout), so any node can see any stream's
// records as they arrive. A client's subscription therefore lives entirely on
// the node it connected to: that node subscribes to the narrowest subject the
// selector allows, decodes each batch, and runs the query evaluator over it.
// The hash ring is not consulted. Routing a subscription to the owner of each
// stream would need a second hop to bring matches back to the client's node and
// a cluster-wide mirror of every subscription, and would buy only a spread of
// matching CPU across nodes — real when subscriptions are many, and tail
// clients are people, so they are few. ponytail: local evaluation, add
// owner-side evaluation with forwarding if tail CPU shows up in profiles.
//
// The one rule that is not negotiable lives in Subscription.deliver: a client
// that cannot keep up loses records, counted, and never delays the goroutine
// delivering them. Anything else would let one stalled browser tab back up the
// NATS subscription and, past the client library's pending limit, cost every
// tail on this node — but never ingest, which does not wait on this path at all.
package tail

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

// ErrClosed is returned by Subscribe once the registry is shutting down.
var ErrClosed = errors.New("tail: registry is closed")

// Record is one matched record together with its stream's labels, which a tail
// client has no other way to learn: it never sees the streams table.
type Record struct {
	Labels model.LabelSet
	model.LogRecord
}

// Registry tracks the live subscriptions on this node so shutdown can end them
// and metrics can count them.
type Registry struct {
	src     queue.Subscriber
	prefix  string
	buffer  int
	metrics *Metrics
	log     *slog.Logger

	mu     sync.Mutex
	subs   map[*Subscription]struct{}
	closed bool
}

// New builds a registry over src. prefix is the fan-out subject prefix, buffer
// the per-client record capacity; both come from configuration.
func New(src queue.Subscriber, prefix string, buffer int, metrics *Metrics, log *slog.Logger) *Registry {
	if metrics == nil {
		metrics = NewMetrics(nil)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Registry{
		src:     src,
		prefix:  prefix,
		buffer:  max(buffer, 1),
		metrics: metrics,
		log:     log.With(slog.String("component", "tail")),
		subs:    map[*Subscription]struct{}{},
	}
}

// Subscribe starts delivering records matching q. The caller must Close the
// subscription; Records stays open until then, and Done reports when the
// registry ended it first.
func (r *Registry) Subscribe(q *query.Query) (*Subscription, error) {
	ev, err := query.NewEvaluator(q)
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		reg:  r,
		ev:   ev,
		ch:   make(chan Record, r.buffer),
		done: make(chan struct{}),
	}

	// Subscribed outside the lock: a NATS subscribe can flush the socket the
	// ingest publishes share, and a stalled broker must not pin the registry
	// against every other client and against Close.
	env, service := pinned(q.Selector)
	if s.src, err = r.src.Subscribe(queue.Filter(r.prefix, env, service), s.deliver); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		s.src.Stop()
		return nil, ErrClosed
	}
	r.subs[s] = struct{}{}
	r.metrics.Subscriptions.Inc()
	return s, nil
}

// Close ends every subscription and refuses new ones. Each client's Done fires,
// which is how the WebSocket handler learns to say goodbye.
func (r *Registry) Close() {
	r.mu.Lock()
	r.closed = true
	subs := make([]*Subscription, 0, len(r.subs))
	for s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.Unlock()

	for _, s := range subs {
		s.Close()
	}
}

func (r *Registry) remove(s *Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.subs[s]; ok {
		delete(r.subs, s)
		r.metrics.Subscriptions.Dec()
	}
}

// pinned returns the env and service a selector fixes with =, or "" for one it
// leaves open. Only equality narrows the subject: a regex or negation could
// match values this node has not seen, so those subscribe to the wildcard and
// let the evaluator decide.
func pinned(sel query.Selector) (env, service string) {
	for _, m := range sel.Matchers {
		if m.Op != query.OpEq {
			continue
		}
		switch m.Label {
		case "env":
			env = m.Value
		case "service":
			service = m.Value
		}
	}
	return env, service
}

// Subscription is one client's stream of matched records.
type Subscription struct {
	reg  *Registry
	ev   *query.Evaluator
	src  queue.Subscription
	ch   chan Record
	done chan struct{}
	once sync.Once

	dropped atomic.Int64
}

// Records is the bounded channel of matches. It is never closed — a delivery
// racing Close must not panic — so consumers select on Done as well.
func (s *Subscription) Records() <-chan Record { return s.ch }

// Done is closed once the subscription has ended, by Close or by the registry.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Dropped is how many matched records this client was too slow to receive.
func (s *Subscription) Dropped() int64 { return s.dropped.Load() }

// Close stops delivery. Idempotent.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.src.Stop()
		s.reg.remove(s)
		close(s.done)
	})
}

// deliver is the fan-out handler. It decodes one batch, keeps the records the
// query matches and offers each to the client without waiting: a full buffer
// means the client is behind, and the record is dropped and counted rather
// than the delivery goroutine blocked.
func (s *Subscription) deliver(payload []byte) {
	var batch logaggv1.LogBatch
	if err := proto.Unmarshal(payload, &batch); err != nil {
		// Cannot come from this project's publisher; something else is on the
		// subject. Logged once per batch at debug so a stray publisher is
		// diagnosable without being able to flood the log.
		s.reg.log.Debug("undecodable tail batch", slog.Any("error", err))
		return
	}
	labels := model.LabelSetFromProto(batch.GetLabels())
	if !s.ev.MatchStream(labels) {
		return
	}
	id := labels.ID()
	for _, pb := range batch.GetRecords() {
		rec := model.LogRecordFromProto(id, pb)
		if !s.ev.MatchRecord(&rec) {
			continue
		}
		select {
		case s.ch <- Record{Labels: labels, LogRecord: rec}:
			s.reg.metrics.Delivered.Inc()
		default:
			s.dropped.Add(1)
			s.reg.metrics.Dropped.Inc()
		}
	}
}
