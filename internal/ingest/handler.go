package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

// MaxAckDetailLen bounds the human-readable detail returned to an agent. The
// detail is built from validation errors, which quote the offending label or
// field, so without a bound one hostile batch would make the collector echo back
// as much as it sent.
const MaxAckDetailLen = 512

// ackCodeNames are the metric label values for the codes this service returns.
var ackCodeNames = []string{"accepted", "overloaded", "invalid", "internal"}

// Stream is the bidirectional ingest RPC.
//
// One goroutine per stream, and the batches on a stream are handled strictly in
// order. That is not an oversight to be parallelized later: an agent assigns
// monotonic sequence numbers per stream and treats an ack as permission to drop
// everything up to it, so handling batch N+1 before batch N would let a crash lose
// N while N+1 is already acknowledged.
func (s *service) Stream(stream grpc.BidiStreamingServer[logaggv1.LogBatch, logaggv1.Ack]) error {
	s.metrics.ActiveStreams.Inc()
	defer s.metrics.ActiveStreams.Dec()

	ctx := stream.Context()

	for {
		batch, err := stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			// The agent closed its send side. A clean end of stream, not a failure.
			return nil
		case err != nil:
			// Includes the client vanishing and the server shutting the stream down.
			// Returning the error ends the RPC; there is nobody left to ack to.
			return err
		}

		ack := s.handle(ctx, batch)
		if err := stream.Send(ack); err != nil {
			// The batch may well be durable at this point. That is safe rather than
			// wrong: an agent that did not see the ack resends, and storage
			// deduplicates on (stream_id, seq, time).
			return err
		}
	}
}

// handle validates, publishes and then reports one batch.
//
// The ordering is the correctness argument of this whole package: an ack means
// "durable, you may forget it", so it is produced only after the queue has
// acknowledged the publish. Everything that can fail is therefore in front of the
// ack, never behind it.
func (s *service) handle(ctx context.Context, batch *logaggv1.LogBatch) *logaggv1.Ack {
	started := time.Now()
	defer func() { s.metrics.BatchDuration.Observe(time.Since(started).Seconds()) }()

	id := batch.GetBatchId()
	records := batch.GetRecords()
	received := len(records)
	if received > 0 {
		s.metrics.RecordsReceived.Add(float64(received))
	}

	labels := model.LabelSetFromProto(batch.GetLabels())
	if err := labels.Validate(); err != nil {
		// The whole batch goes: without a valid label set there is no stream to
		// attribute the records to, and no subject to publish them on.
		s.drop(reasonInvalidLabels, received)
		return s.reject(id, logaggv1.AckCode_ACK_CODE_INVALID, received, fmt.Sprintf("invalid labels: %v", err))
	}

	// Fingerprint once here. The consumer recomputes it from the labels it receives
	// rather than trusting a wire field, so a stream ID can never disagree with the
	// labels it is supposed to identify.
	streamID := labels.ID()

	valid, firstErr := s.acceptable(streamID, records)
	rejected := received - len(valid)
	s.drop(reasonInvalidRecord, rejected)

	if len(valid) == 0 {
		detail := "no valid records in batch"
		if firstErr != nil {
			detail = fmt.Sprintf("no valid records in batch: %v", firstErr)
		}
		return s.reject(id, logaggv1.AckCode_ACK_CODE_INVALID, received, detail)
	}

	// Re-marshaled rather than forwarding the received bytes, because the invalid
	// records have been filtered out and only the survivors should reach the queue.
	// Letting one bad record through would poison every redelivery of the batch.
	payload, err := proto.Marshal(&logaggv1.LogBatch{
		BatchId: id,
		Labels:  batch.GetLabels(),
		Records: valid,
	})
	if err != nil {
		s.log.Error("encoding batch failed",
			slog.String("batch_id", id),
			slog.Int64("stream_id", int64(streamID)),
			slog.Any("error", err),
		)
		s.drop(reasonEncodeFailed, len(valid))
		return s.reject(id, logaggv1.AckCode_ACK_CODE_INTERNAL, received, "encoding batch failed")
	}

	subject := s.pipeline.queue.Subject(labels.Env, labels.Service)
	if err = s.pipeline.submit(ctx, &job{
		subject: subject,
		payload: payload,
		records: len(valid),
	}); err != nil {
		return s.publishFailed(id, subject, streamID, received, len(valid), err)
	}

	s.metrics.RecordsAccepted.Add(float64(len(valid)))
	return s.ack(&logaggv1.Ack{
		BatchId:  id,
		Code:     logaggv1.AckCode_ACK_CODE_ACCEPTED,
		Accepted: uint32(len(valid)), //nolint:gosec // bounded by MaxRecvMsgSize
		Rejected: uint32(rejected),   //nolint:gosec // bounded by MaxRecvMsgSize
	})
}

// acceptable filters the records that pass validation, returning them and the
// first rejection reason.
//
// Filtering here rather than at the storage layer is what keeps a poison record
// from blocking a stream: one oversized message left in the batch would fail the
// COPY, fail every retry of it, and the batch would be redelivered forever.
//
// Each record is converted to the model purely to validate it, and the consumer
// converts it again after decoding. That duplicated conversion buys one thing
// worth more than the cycles: validation lives in exactly one place, so the agent,
// the loadgen and this handler cannot disagree about what a valid record is.
func (s *service) acceptable(id model.StreamID, records []*logaggv1.LogRecord) ([]*logaggv1.LogRecord, error) {
	now := time.Now()
	var firstErr error

	// Filtered in place: the slice belongs to a protobuf message this handler is
	// done with, and the common case rejects nothing.
	kept := records[:0]
	for _, pb := range records {
		rec := model.LogRecordFromProto(id, pb)
		if err := rec.Validate(now); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			s.log.Debug("dropping invalid record",
				slog.Int64("stream_id", int64(id)),
				slog.Int64("seq", rec.Seq),
				slog.Any("error", err),
			)
			continue
		}
		kept = append(kept, pb)
	}
	return kept, firstErr
}

// publishFailed turns a queue or intake failure into the ack the agent should act
// on.
//
// The mapping mirrors the failure kinds the writer already distinguishes:
// saturation is retryable and means "slow down", an unusable payload is not
// retryable at all, and anything else is a collector-side fault the agent should
// retry without changing its rate.
//
// A shed batch reports OVERLOADED in band rather than failing the RPC with
// RESOURCE_EXHAUSTED. Both say "retry later", but a status code ends the whole
// stream, which would punish every other batch on it for one full moment — and
// ACK_CODE_OVERLOADED exists in the protocol precisely so saturation is a per-batch
// answer. gRPC still returns RESOURCE_EXHAUSTED itself for the transport-level
// case, an agent exceeding MaxRecvMsgSize.
func (s *service) publishFailed(id, subject string, streamID model.StreamID, received, valid int, err error) *logaggv1.Ack {
	code := logaggv1.AckCode_ACK_CODE_INTERNAL
	reason := reasonQueueRefused
	detail := "queue rejected the batch"

	// The queue kinds are tested before the context cases, and the order is
	// load-bearing. Conn.Publish bounds itself with the publish timeout and wraps both
	// ErrUnavailable and context.DeadlineExceeded, so a slow broker looks exactly like
	// a client that hung up if DeadlineExceeded is matched first — and the drop would
	// be counted against the agent instead of the broker. A genuine client cancel
	// arrives as context.Canceled from the pipeline's own select, with no queue kind
	// attached.
	switch {
	case errors.Is(err, errShed):
		code = logaggv1.AckCode_ACK_CODE_OVERLOADED
		reason = reasonBufferFull
		detail = "collector is saturated"
	case errors.Is(err, errPipelineClosed):
		reason = reasonShutdown
		detail = "collector is shutting down"
	case errors.Is(err, queue.ErrOverloaded):
		code = logaggv1.AckCode_ACK_CODE_OVERLOADED
	case errors.Is(err, queue.ErrTooLarge):
		code = logaggv1.AckCode_ACK_CODE_INVALID
		reason = reasonQueueTooLarge
	case errors.Is(err, queue.ErrUnavailable), errors.Is(err, queue.ErrNotConnected):
		reason = reasonQueueRefused
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// The agent hung up mid-batch. Nothing to tell it, but the records are still
		// unaccounted for from this node's point of view.
		reason = reasonClientGone
		detail = "client went away"
	}

	// Warn rather than error: a refused publish is the backpressure chain working,
	// and the agent is being told to retry. The metrics are where a sustained
	// failure is supposed to be noticed.
	s.log.Warn("publishing batch failed",
		slog.String("batch_id", id),
		slog.String("subject", subject),
		slog.Int64("stream_id", int64(streamID)),
		slog.Int("records", valid),
		slog.String("ack_code", ackCodeLabel(code)),
		slog.Any("error", err),
	)
	s.drop(reason, valid)
	// Reported as fully rejected on purpose: nothing was made durable, so every
	// record in the batch is the agent's problem again.
	return s.reject(id, code, received, detail)
}

// reject builds an ack that accepted nothing.
func (s *service) reject(id string, code logaggv1.AckCode, rejected int, detail string) *logaggv1.Ack {
	return s.ack(&logaggv1.Ack{
		BatchId:  id,
		Code:     code,
		Rejected: uint32(rejected), //nolint:gosec // bounded by MaxRecvMsgSize
		Detail:   truncate(detail, MaxAckDetailLen),
	})
}

// ack counts an acknowledgement and returns it, so no path can report a code
// without recording it.
func (s *service) ack(a *logaggv1.Ack) *logaggv1.Ack {
	s.metrics.Acks.WithLabelValues(ackCodeLabel(a.GetCode())).Inc()
	return a
}

func (s *service) drop(reason string, n int) {
	if n <= 0 {
		return
	}
	s.metrics.RecordsDropped.WithLabelValues(observability.ComponentIngest, reason).Add(float64(n))
}

// ackCodeLabel is the short metric label for a code. Derived by hand rather than
// from the generated enum name, because the generated names are long and changing
// one would silently rename a metric label.
func ackCodeLabel(code logaggv1.AckCode) string {
	switch code {
	case logaggv1.AckCode_ACK_CODE_ACCEPTED:
		return "accepted"
	case logaggv1.AckCode_ACK_CODE_OVERLOADED:
		return "overloaded"
	case logaggv1.AckCode_ACK_CODE_INVALID:
		return "invalid"
	default:
		return "internal"
	}
}

// truncate bounds a string, marking that it was cut, without splitting a rune.
//
// The rune boundary is not cosmetic. Detail is built from validation errors that
// quote the sender's own label names and field values, so it can contain multi-byte
// UTF-8; a proto3 string field must be valid UTF-8, and cutting mid-sequence would
// make marshaling the ack fail — turning a rejected batch into a broken stream.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	const ellipsis = "..."
	cut := maxLen - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}
