package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/storage"
)

// fakeMessage records which terminal operation was applied.
type fakeMessage struct {
	data    []byte
	subject string

	mu    sync.Mutex
	calls []terminal
	err   error
}

func (m *fakeMessage) Data() []byte                { return m.data }
func (m *fakeMessage) Subject() string             { return m.subject }
func (m *fakeMessage) Header() map[string][]string { return nil }
func (m *fakeMessage) Redeliveries() uint64        { return 1 }
func (m *fakeMessage) Ack() error                  { return m.record(ack) }
func (m *fakeMessage) Nak() error                  { return m.record(nak) }
func (m *fakeMessage) Term() error                 { return m.record(term) }

func (m *fakeMessage) record(op terminal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, op)
	return m.err
}

func (m *fakeMessage) terminals() []terminal {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]terminal(nil), m.calls...)
}

// fakeQueue captures the handler instead of talking to a broker.
type fakeQueue struct {
	handler queue.Handler
	stopped bool
	err     error
}

func (q *fakeQueue) Consume(_ context.Context, h queue.Handler) (queue.Subscription, error) {
	if q.err != nil {
		return nil, q.err
	}
	q.handler = h
	return stopper{q}, nil
}

type stopper struct{ q *fakeQueue }

func (s stopper) Stop() { s.q.stopped = true }

// fakeWriter stands in for storage.Writer, recording shipments and letting a test
// decide what Submit reports and when Ack fires.
type fakeWriter struct {
	mu        sync.Mutex
	shipments []storage.Shipment

	accepted int
	err      error
	// ackWith, when set, is passed to the shipment's Ack closure synchronously,
	// standing in for the write completing.
	ackWith *error
	autoAck bool
}

func (w *fakeWriter) Submit(_ context.Context, sh storage.Shipment) (int, error) {
	w.mu.Lock()
	w.shipments = append(w.shipments, sh)
	accepted, err, autoAck, ackWith := w.accepted, w.err, w.autoAck, w.ackWith
	w.mu.Unlock()

	if err != nil {
		return 0, err
	}
	if autoAck && sh.Ack != nil {
		var writeErr error
		if ackWith != nil {
			writeErr = *ackWith
		}
		sh.Ack(writeErr)
	}
	return accepted, nil
}

func (w *fakeWriter) submitted() []storage.Shipment {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]storage.Shipment(nil), w.shipments...)
}

// deliver builds a consumer, starts it and hands msg to the captured handler.
func deliver(t *testing.T, w *fakeWriter, metrics *Metrics, msg *fakeMessage) *Consumer {
	t.Helper()

	q := &fakeQueue{}
	c, err := NewConsumer(q, w, metrics, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.handler(msg)
	return c
}

// encoded marshals a batch the way the ingest handler does.
func encoded(t *testing.T, batch *logaggv1.LogBatch) []byte {
	t.Helper()

	payload, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return payload
}

// The deferred ack is what makes a crash between write and ack a redelivery rather
// than a loss, so the message must not be acked by Submit returning.
func TestConsumerAcksOnlyWhenTheWriteLands(t *testing.T) {
	t.Parallel()

	batch := validBatch("b1", 3)
	msg := &fakeMessage{data: encoded(t, batch), subject: "logs.dev.checkout"}
	w := &fakeWriter{accepted: 3}

	deliver(t, w, NewMetrics(nil), msg)

	if got := msg.terminals(); len(got) != 0 {
		t.Fatalf("message was terminated with %v before the write completed", got)
	}

	// Now the write lands.
	w.submitted()[0].Ack(nil)
	if got := msg.terminals(); len(got) != 1 || got[0] != ack {
		t.Fatalf("terminals = %v, want one ack", got)
	}
}

func TestConsumerBuildsAShipmentFromTheBatch(t *testing.T) {
	t.Parallel()

	batch := validBatch("b1", 4)
	msg := &fakeMessage{data: encoded(t, batch), subject: "logs.dev.checkout"}
	w := &fakeWriter{accepted: 4, autoAck: true}

	deliver(t, w, NewMetrics(nil), msg)

	shipped := w.submitted()
	if len(shipped) != 1 {
		t.Fatalf("submitted %d shipments, want 1", len(shipped))
	}
	sh := shipped[0]
	if sh.Stream.Labels.Service != "checkout" || sh.Stream.Labels.Env != "dev" {
		t.Errorf("labels = %s, want the batch's labels", sh.Stream.Labels)
	}
	// Recomputed from the labels rather than carried on the wire, so a stream ID can
	// never disagree with the labels it identifies.
	if want := sh.Stream.Labels.ID(); sh.Stream.ID != want {
		t.Errorf("stream ID = %d, want %d", sh.Stream.ID, want)
	}
	if len(sh.Records) != 4 {
		t.Errorf("shipped %d records, want 4", len(sh.Records))
	}
	if sh.Ack == nil {
		t.Error("shipment has no Ack closure, so the message would never be acked")
	}
}

// A failed write must return the batch to the stream. Acking it would lose records
// that were never written.
func TestConsumerNaksAFailedWrite(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("database is on fire")
	msg := &fakeMessage{data: encoded(t, validBatch("b1", 2)), subject: "logs.dev.checkout"}
	w := &fakeWriter{accepted: 2, autoAck: true, ackWith: &writeErr}

	deliver(t, w, NewMetrics(nil), msg)

	if got := msg.terminals(); len(got) != 1 || got[0] != nak {
		t.Fatalf("terminals = %v, want one nak", got)
	}
}

// A refusal from Submit is about this moment, not about the batch, so it is a nak.
func TestConsumerNaksWhenTheWriterRefuses(t *testing.T) {
	t.Parallel()

	msg := &fakeMessage{data: encoded(t, validBatch("b1", 2)), subject: "logs.dev.checkout"}
	w := &fakeWriter{err: storage.ErrWriterClosed}

	deliver(t, w, NewMetrics(nil), msg)

	if got := msg.terminals(); len(got) != 1 || got[0] != nak {
		t.Fatalf("terminals = %v, want one nak", got)
	}
}

// A payload that does not decode fails identically forever. Naking it would spend
// the consumer's ack budget on a message that can never leave the stream.
func TestConsumerTermsAnUndecodableMessage(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	msg := &fakeMessage{data: []byte("not protobuf at all"), subject: "logs.dev.checkout"}
	w := &fakeWriter{}

	deliver(t, w, NewMetrics(reg), msg)

	if got := msg.terminals(); len(got) != 1 || got[0] != term {
		t.Fatalf("terminals = %v, want one term", got)
	}
	if len(w.submitted()) != 0 {
		t.Error("an undecodable message reached the writer")
	}
	if got := counter(t, reg, "logagg_ingest_messages_terminated_total",
		map[string]string{"reason": reasonDecodeFailed}); got != 1 {
		t.Errorf("messages_terminated_total{decode_failed} = %v, want 1", got)
	}
}

func TestConsumerTermsInvalidLabels(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	batch := validBatch("b1", 2)
	batch.Labels.Env = "" // Ingest would have rejected this before publishing.
	msg := &fakeMessage{data: encoded(t, batch), subject: "logs.dev.checkout"}
	w := &fakeWriter{}

	deliver(t, w, NewMetrics(reg), msg)

	if got := msg.terminals(); len(got) != 1 || got[0] != term {
		t.Fatalf("terminals = %v, want one term", got)
	}
	if got := counter(t, reg, "logagg_records_dropped_total", map[string]string{
		"component": observability.ComponentIngest,
		"reason":    reasonInvalidLabels,
	}); got != 2 {
		t.Errorf("records_dropped_total{ingest,invalid_labels} = %v, want 2", got)
	}
}

// Submit only calls Ack when it accepted something, so a batch it accepted nothing
// from has to be acked here or it sits unacked until AckWait and comes back forever.
func TestConsumerAcksABatchTheWriterAcceptedNothingFrom(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	msg := &fakeMessage{data: encoded(t, validBatch("b1", 3)), subject: "logs.dev.checkout"}
	w := &fakeWriter{accepted: 0}

	deliver(t, w, NewMetrics(reg), msg)

	if got := msg.terminals(); len(got) != 1 || got[0] != ack {
		t.Fatalf("terminals = %v, want one ack", got)
	}
	// Not counted here on purpose: Submit already counted these records under the
	// writer's own invalid reason, and RecordsDropped is one family meant to be summed
	// across components, so counting again would report double.
	if got := counter(t, reg, "logagg_records_dropped_total", map[string]string{
		"component": observability.ComponentIngest,
		"reason":    reasonInvalidRecord,
	}); got != 0 {
		t.Errorf("records_dropped_total{ingest,invalid_record} = %v, want 0: the writer already counted them", got)
	}
}

// A broker that cannot be reached at startup must fail startup, not leave a node
// serving a queue it will never drain.
func TestConsumerStartReportsSubscribeFailures(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("no such stream")
	c, err := NewConsumer(&fakeQueue{err: sentinel}, &fakeWriter{}, NewMetrics(nil), nil)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err = c.Start(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("Start err = %v, want %v", err, sentinel)
	}
}

func TestConsumerStopIsSafeBeforeStart(t *testing.T) {
	t.Parallel()

	c, err := NewConsumer(&fakeQueue{}, &fakeWriter{}, NewMetrics(nil), nil)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// Reached whenever startup fails after the consumer is built but before it
	// subscribes, which the shutdown path does not special-case.
	if err = c.Stop(context.Background()); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}
}

func TestConsumerStopEndsTheSubscription(t *testing.T) {
	t.Parallel()

	q := &fakeQueue{}
	c, err := NewConsumer(q, &fakeWriter{}, NewMetrics(nil), nil)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err = c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !q.stopped {
		t.Error("Stop did not stop the subscription, so the consumer keeps pulling")
	}
}

func TestNewConsumerRequiresItsDependencies(t *testing.T) {
	t.Parallel()

	if _, err := NewConsumer(nil, &fakeWriter{}, nil, nil); err == nil {
		t.Error("NewConsumer accepted a nil queue")
	}
	if _, err := NewConsumer(&fakeQueue{}, nil, nil, nil); err == nil {
		t.Error("NewConsumer accepted a nil writer")
	}
}

// The consumer is bound to the real Conn in cmd/collector, so the narrow interfaces
// it declares have to be satisfied by the real types.
func TestRealTypesSatisfyTheConsumerInterfaces(t *testing.T) {
	t.Parallel()

	var _ Consumable = (*queue.Conn)(nil)
	var _ Submitter = (*storage.Writer)(nil)
	_ = time.Now
}

// Stop must wait for a handler that is mid-Submit. Abandoning it would have the
// batch refused by a closing writer, naked, and redelivered -- work discarded when
// it was one call from being queued.
func TestConsumerStopWaitsForInFlightHandlers(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	inSubmit := make(chan struct{})
	w := &blockingWriter{inSubmit: inSubmit, release: release}

	q := &fakeQueue{}
	c, err := NewConsumer(q, w, NewMetrics(nil), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg := &fakeMessage{data: encoded(t, validBatch("b1", 2)), subject: "logs.dev.checkout"}
	go q.handler(msg)
	<-inSubmit

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- c.Stop(ctx)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a handler was still submitting")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err = <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// A handler that never returns must not hold shutdown open past the deadline: the
// batch is unacked, so the queue will bring it back.
func TestConsumerStopGivesUpOnADeadline(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	defer close(release)
	inSubmit := make(chan struct{})
	w := &blockingWriter{inSubmit: inSubmit, release: release}

	q := &fakeQueue{}
	c, err := NewConsumer(q, w, NewMetrics(nil), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg := &fakeMessage{data: encoded(t, validBatch("b1", 1)), subject: "logs.dev.checkout"}
	go q.handler(msg)
	<-inSubmit

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err = c.Stop(ctx); err == nil {
		t.Fatal("Stop reported a clean drain despite a handler that never finished")
	}
}

// blockingWriter holds Submit open until released.
type blockingWriter struct {
	inSubmit chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (w *blockingWriter) Submit(_ context.Context, sh storage.Shipment) (int, error) {
	w.once.Do(func() { close(w.inSubmit) })
	<-w.release
	if sh.Ack != nil {
		sh.Ack(nil)
	}
	return len(sh.Records), nil
}

// A delivery that arrives after Stop has begun must be naked rather than admitted.
// Admitting it would let a busy stream starve the drain; holding it would make the
// batch wait out AckWait for no reason.
func TestConsumerNaksDeliveriesArrivingDuringShutdown(t *testing.T) {
	t.Parallel()

	q := &fakeQueue{}
	w := &fakeWriter{accepted: 1, autoAck: true}
	c, err := NewConsumer(q, w, NewMetrics(nil), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err = c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err = c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	msg := &fakeMessage{data: encoded(t, validBatch("late", 1)), subject: "logs.dev.checkout"}
	q.handler(msg)

	if got := msg.terminals(); len(got) != 1 || got[0] != nak {
		t.Fatalf("terminals = %v, want one nak", got)
	}
	if len(w.submitted()) != 0 {
		t.Error("a delivery arriving during shutdown reached the writer")
	}
}
