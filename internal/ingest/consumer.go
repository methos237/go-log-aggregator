package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/storage"
)

// Consumer drains the queue into the writer.
//
// It lives beside the gRPC server rather than in internal/storage because it is
// the second half of the same path: a record arrives, is validated, is queued, and
// then comes back out to be written. Keeping both halves in one package keeps the
// protobuf decoding next to the encoding it mirrors, and keeps internal/storage
// unaware that a wire format exists at all.
//
// This is the producer the phase 1 writer was built for. Until now Writer.Submit
// had no caller in production.
type Consumer struct {
	queue  Consumable
	writer Submitter

	log     *slog.Logger
	metrics *Metrics
	now     nowFunc

	sub queue.Subscription
}

// Consumable is the queue side of the consumer, narrowed to what it uses so a test
// can drive the handler without a broker.
type Consumable interface {
	Consume(ctx context.Context, h queue.Handler) (queue.Subscription, error)
}

// Submitter is the storage side, narrowed for the same reason.
type Submitter interface {
	Submit(ctx context.Context, sh storage.Shipment) (int, error)
}

// NewConsumer builds a consumer. Call Start to begin draining.
func NewConsumer(q Consumable, w Submitter, metrics *Metrics, log *slog.Logger) (*Consumer, error) {
	if q == nil {
		return nil, errors.New("consumer needs a queue")
	}
	if w == nil {
		return nil, errors.New("consumer needs a writer")
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Consumer{
		queue:   q,
		writer:  w,
		log:     log.With(slog.String("component", "consumer")),
		metrics: metrics,
		now:     time.Now,
	}, nil
}

// Start subscribes and begins delivering messages to the writer.
//
// ctx is the process context. It bounds the subscription setup and is what the
// handler passes to Submit, so a shutdown stops the handler blocking on a writer
// queue that is no longer being drained.
func (c *Consumer) Start(ctx context.Context) error {
	sub, err := c.queue.Consume(ctx, func(msg queue.Message) { c.handle(ctx, msg) })
	if err != nil {
		return err
	}
	c.sub = sub
	return nil
}

// Stop ends the subscription. In-flight messages are left to finish; anything not
// acked returns to the stream and is redelivered, which is the property that makes
// killing a collector mid-write safe.
func (c *Consumer) Stop() {
	if c.sub == nil {
		return
	}
	c.sub.Stop()
	c.log.Info("consumer stopped")
}

// handle turns one message into a shipment and hands it to the writer.
//
// Every path terminates the message exactly once, and which terminator is used is
// the whole design:
//
//   - A payload that cannot be decoded, or whose labels are invalid, is Termed. It
//     will fail identically forever, so redelivering it would spend the consumer's
//     ack budget on a message that can never leave the stream.
//   - A batch the writer refuses is Naked, because the refusal is about this moment
//     — a full queue, a shutdown — not about the batch.
//   - A batch the writer accepts is acked later, from the Ack closure, once the
//     rows are in the hypertable. That deferral is what makes a crash between write
//     and ack a redelivery rather than a loss.
func (c *Consumer) handle(ctx context.Context, msg queue.Message) {
	var batch logaggv1.LogBatch
	if err := proto.Unmarshal(msg.Data(), &batch); err != nil {
		c.log.Error("dropping undecodable message",
			slog.String("subject", msg.Subject()),
			slog.Int("bytes", len(msg.Data())),
			slog.Any("error", err),
		)
		c.metrics.TerminatedMessages.WithLabelValues(reasonDecodeFailed).Inc()
		c.terminate(msg, term)
		return
	}

	labels := model.LabelSetFromProto(batch.GetLabels())
	if err := labels.Validate(); err != nil {
		// Ingest validated these labels before publishing, so reaching here means the
		// payload was corrupted or produced by something that skipped that check.
		c.log.Error("dropping message with invalid labels",
			slog.String("subject", msg.Subject()),
			slog.Any("error", err),
		)
		c.drop(reasonInvalidLabels, len(batch.GetRecords()))
		c.metrics.TerminatedMessages.WithLabelValues(reasonInvalidLabels).Inc()
		c.terminate(msg, term)
		return
	}

	now := c.now()
	stream := model.NewStream(labels, now)
	records := make([]model.LogRecord, 0, len(batch.GetRecords()))
	for _, pb := range batch.GetRecords() {
		records = append(records, model.LogRecordFromProto(stream.ID, pb))
	}

	accepted, err := c.writer.Submit(ctx, storage.Shipment{
		Stream:  stream,
		Records: records,
		// Called by the writer once the rows are durable, or with an error when it
		// gave up. Naking on failure returns the batch to the stream instead of
		// losing it.
		Ack: func(writeErr error) { c.finish(msg, batch.GetBatchId(), writeErr) },
	})
	if err != nil {
		c.log.Warn("writer refused batch",
			slog.String("batch_id", batch.GetBatchId()),
			slog.Int64("stream_id", int64(stream.ID)),
			slog.Any("error", err),
		)
		c.terminate(msg, nak)
		return
	}

	// Submit only invokes Ack when it accepted something, so a batch whose every
	// record failed validation would otherwise sit unacked until AckWait expired and
	// then be redelivered forever. Nothing is durable, but nothing ever will be, so
	// this is an ack rather than a nak.
	if accepted == 0 {
		c.log.Warn("batch had no acceptable records",
			slog.String("batch_id", batch.GetBatchId()),
			slog.Int64("stream_id", int64(stream.ID)),
			slog.Int("records", len(records)),
		)
		c.drop(reasonInvalidRecord, len(records))
		c.terminate(msg, ack)
	}
}

// finish is the writer's callback: ack what landed, nak what did not.
func (c *Consumer) finish(msg queue.Message, batchID string, writeErr error) {
	if writeErr != nil {
		c.log.Warn("write failed, returning batch to the queue",
			slog.String("batch_id", batchID),
			slog.Any("error", writeErr),
		)
		c.terminate(msg, nak)
		return
	}
	c.terminate(msg, ack)
}

// terminal is one of the three ways a message's delivery can end.
type terminal string

const (
	ack  terminal = "ack"
	nak  terminal = "nak"
	term terminal = "term"
)

// terminate applies one of the three terminal operations and reports a failure to
// apply it.
//
// A failed ack is worth a log line rather than a retry: the broker will redeliver
// the batch when AckWait expires, storage will deduplicate the replay, and a retry
// loop here could block the consumer callback indefinitely.
func (c *Consumer) terminate(msg queue.Message, op terminal) {
	var err error
	switch op {
	case ack:
		err = msg.Ack()
	case nak:
		err = msg.Nak()
	case term:
		err = msg.Term()
	}
	if err != nil {
		c.log.Error("terminating message failed",
			slog.String("operation", string(op)),
			slog.String("subject", msg.Subject()),
			slog.Any("error", err),
		)
	}
}

func (c *Consumer) drop(reason string, n int) {
	if n <= 0 {
		return
	}
	c.metrics.RecordsDropped.WithLabelValues(observability.ComponentIngest, reason).Add(float64(n))
}
