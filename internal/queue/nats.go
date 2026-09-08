package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// ErrNotConnected is reported by the health check while the connection is down.
// It is a readiness failure, not a liveness one: reconnection is automatic, and
// restarting the process would only throw away the backlog it is holding.
var ErrNotConnected = errors.New("not connected to nats")

// Conn is the JetStream implementation of Publisher.
//
// It owns the NATS connection and the stream declaration. One per process: the
// client multiplexes publishes over a single TCP connection, so a pool would add
// sockets without adding throughput.
type Conn struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg config.Queue

	log     *slog.Logger
	metrics *Metrics

	// closing distinguishes an intentional Close from a broker that went away.
	// nats.go fires the disconnect handler for both, and a WARN on every clean
	// shutdown trains an operator to ignore the one that matters.
	closing atomic.Bool
}

// Compile-time proof that the concrete type satisfies the interface the ingest
// path depends on. Cheaper to find here than at the call site.
var _ Publisher = (*Conn)(nil)

// Connect dials NATS, declares the stream and returns a ready publisher.
//
// The stream is declared at startup rather than left to an operator because the
// subject scheme and the retention policy are correctness-relevant: a stream that
// discards old messages instead of refusing new ones would turn backpressure into
// silent data loss. Declaring it in code keeps that decision in one reviewable
// place.
//
// metrics may be nil, which means "build the connection but record nothing".
//
//nolint:gocritic // hugeParam: one copy per process; by value keeps it immutable
func Connect(ctx context.Context, cfg config.Queue, metrics *Metrics, log *slog.Logger) (*Conn, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With(slog.String("component", "queue"))

	// Allocated before dialing because the connection-event handlers need to see
	// this connection's closing flag, and nats.go can fire them during Connect.
	c := &Conn{cfg: cfg, log: log, metrics: metrics}

	nc, err := nats.Connect(cfg.URL, natsOptions(cfg, log, &c.closing)...)
	if err != nil {
		return nil, fmt.Errorf("connect to nats at %s: %w", cfg.URL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	// Bounded independently of ctx: ctx is the process lifetime, and a broker that
	// cannot answer a stream declaration within the connect budget is one this node
	// should fail fast against rather than hang on.
	declareCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	if _, err = js.CreateOrUpdateStream(declareCtx, streamConfig(cfg)); err != nil {
		nc.Close()
		return nil, fmt.Errorf("declare stream %s: %w", cfg.StreamName, err)
	}

	log.Info("queue connected",
		slog.String("url", cfg.URL),
		slog.String("stream", cfg.StreamName),
		slog.String("subjects", cfg.SubjectPrefix+".>"),
		slog.Int64("max_bytes", cfg.StreamMaxBytes),
		slog.Duration("max_age", cfg.StreamMaxAge),
	)
	c.nc, c.js = nc, js
	return c, nil
}

// natsOptions configures reconnection and connection-event logging.
//
// Reconnection is unlimited and the buffer is disabled, which is the important
// pair. nats.go by default buffers publishes made while disconnected and flushes
// them on reconnect; that is an unbounded in-memory queue in the one place this
// design refuses to have one. With the buffer off, a publish during an outage
// fails immediately, the ingest handler refuses the batch, and the agent's own
// disk spool becomes the buffer — where it belongs, since the agent is the only
// component that can afford to lose nothing.
//
//nolint:gocritic // hugeParam: called once per process
func natsOptions(cfg config.Queue, log *slog.Logger, closing *atomic.Bool) []nats.Option {
	return []nats.Option{
		nats.Name("logagg-collector"),
		nats.Timeout(cfg.ConnectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.ReconnectJitter(100*time.Millisecond, time.Second),
		nats.ReconnectBufSize(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if closing.Load() {
				// Our own Close. Logged at debug so a clean shutdown stays quiet and a
				// real disconnect stays a warning worth reading.
				log.Debug("queue disconnected during shutdown")
				return
			}
			log.Warn("queue disconnected", slog.Any("error", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("queue reconnected", slog.String("url", nc.ConnectedUrl()))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			attrs := []any{slog.Any("error", err)}
			if sub != nil {
				attrs = append(attrs, slog.String("subject", sub.Subject))
			}
			log.Error("queue async error", attrs...)
		}),
	}
}

// streamConfig is the stream this collector expects to publish into.
//
// The two decisions worth defending:
//
//   - WorkQueuePolicy: a record is removed once the writer acks it. The queue is a
//     buffer, not an archive — TimescaleDB is the archive — so keeping acked
//     records would consume disk to hold data that is already durable elsewhere.
//     Multiple collector replicas still scale out by binding to the same durable
//     pull consumer, which work-queue retention allows.
//   - DiscardNew: when the stream hits its ceiling, publishes are rejected and the
//     oldest queued records are kept. DiscardOld would bound disk by throwing away
//     records the agent already considers delivered. Rejecting instead propagates
//     the pressure up the chain — ingest returns RESOURCE_EXHAUSTED, the agent
//     backs off and spools — which is the backpressure story in §1 of the roadmap.
//
//nolint:gocritic // hugeParam: called once per process
func streamConfig(cfg config.Queue) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        cfg.StreamName,
		Description: "Log records awaiting a write into TimescaleDB.",
		// One wildcard rather than a subject per service: subjects are created by
		// publishing, and enumerating them would mean updating the stream every time
		// a new service appeared.
		Subjects:  []string{cfg.SubjectPrefix + ".>"},
		Retention: jetstream.WorkQueuePolicy,
		Discard:   jetstream.DiscardNew,
		Storage:   jetstream.FileStorage,
		MaxBytes:  cfg.StreamMaxBytes,
		MaxAge:    cfg.StreamMaxAge,
		// No deduplication window. Publish-side dedup would need a message ID per
		// batch and would still not cover a redelivery to the writer, so dedup lives
		// where it can be complete: the logs_dedup index (see internal/storage).
		Duplicates: 0,
	}
}

// Publish sends payload and waits for JetStream to acknowledge it durably.
//
// The wait is the point. Returning before the ack would let the ingest handler
// tell an agent a batch is safe while it exists only in this process's memory, so
// a crash would lose data the agent has already dropped from its spool.
func (c *Conn) Publish(ctx context.Context, subject string, payload []byte) (PubAck, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.PublishTimeout)
	defer cancel()

	started := time.Now()
	ack, err := c.js.Publish(ctx, subject, payload)
	elapsed := time.Since(started)

	if err != nil {
		c.observePublish(outcomeFailure, elapsed, 0)
		return PubAck{}, fmt.Errorf("publish to %s: %w", subject, err)
	}
	c.observePublish(outcomeSuccess, elapsed, len(payload))
	return PubAck{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// Subject renders the subject for a label set.
func (c *Conn) Subject(env, service string) string {
	return Subject(c.cfg.SubjectPrefix, env, service)
}

// Close flushes anything pending and closes the connection.
//
// Flush before close because a publish whose ack has not arrived yet is not
// durable; dropping the connection first would turn it into a lost batch that
// nobody retries. The flush is bounded by ctx, and a failure is reported rather
// than swallowed so shutdown says so.
func (c *Conn) Close(ctx context.Context) error {
	c.closing.Store(true)
	err := c.nc.FlushWithContext(ctx)
	c.nc.Close()
	if err != nil {
		return fmt.Errorf("flush queue on close: %w", err)
	}
	c.log.Info("queue closed")
	return nil
}

// HealthCheck reports whether this node can reach JetStream.
//
// Connection state alone is not enough: nats.go reports IsConnected for a broker
// whose JetStream subsystem is unhealthy, and publishing is what this node needs
// to work. AccountInfo is one round trip to $JS.API, which is cheap at readiness
// cadence and actually proves the thing being claimed.
func HealthCheck(c *Conn) observability.CheckFunc {
	return func(ctx context.Context) error {
		if !c.nc.IsConnected() {
			return fmt.Errorf("%w: status %s", ErrNotConnected, c.nc.Status())
		}
		if _, err := c.js.AccountInfo(ctx); err != nil {
			return fmt.Errorf("jetstream account info: %w", err)
		}
		return nil
	}
}

func (c *Conn) observePublish(outcome string, took time.Duration, payloadBytes int) {
	if c.metrics == nil {
		return
	}
	c.metrics.PublishDuration.WithLabelValues(outcome).Observe(took.Seconds())
	if payloadBytes > 0 {
		c.metrics.PublishBytes.Add(float64(payloadBytes))
	}
}
