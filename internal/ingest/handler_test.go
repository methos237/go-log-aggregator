package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

// validBatch builds a batch of n records that pass validation.
func validBatch(id string, n int) *logaggv1.LogBatch {
	now := time.Now()
	records := make([]*logaggv1.LogRecord, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, &logaggv1.LogRecord{
			TimeUnixNano: now.Add(-time.Duration(i) * time.Millisecond).UnixNano(),
			Seq:          int64(i),
			Level:        logaggv1.Level_LEVEL_INFO,
			Message:      fmt.Sprintf("record %d", i),
		})
	}
	return &logaggv1.LogBatch{
		BatchId: id,
		Labels:  &logaggv1.LabelSet{Service: "checkout", Host: "host-1", Env: "dev"},
		Records: records,
	}
}

// exchange sends one batch on a fresh stream and returns the ack.
func exchange(t *testing.T, client logaggv1.LogServiceClient, batch *logaggv1.LogBatch) *logaggv1.Ack {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Stream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err = stream.Send(batch); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	return ack
}

func TestStreamAcceptsAValidBatch(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(nil))

	ack := exchange(t, client, validBatch("b1", 3))

	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s (%s), want ACCEPTED", ack.GetCode(), ack.GetDetail())
	}
	if ack.GetBatchId() != "b1" {
		t.Errorf("batch_id = %q, want b1: the agent keys its spool on this", ack.GetBatchId())
	}
	if ack.GetAccepted() != 3 || ack.GetRejected() != 0 {
		t.Errorf("accepted/rejected = %d/%d, want 3/0", ack.GetAccepted(), ack.GetRejected())
	}

	published := pub.Published()
	if len(published) != 1 {
		t.Fatalf("published %d messages, want 1", len(published))
	}
	// The subject is derived from the batch's own labels, not from configuration.
	if want := queue.Subject("logs", "dev", "checkout"); published[0].Subject != want {
		t.Errorf("subject = %q, want %q", published[0].Subject, want)
	}

	var decoded logaggv1.LogBatch
	if err := proto.Unmarshal(published[0].Payload, &decoded); err != nil {
		t.Fatalf("published payload does not decode: %v", err)
	}
	if len(decoded.GetRecords()) != 3 {
		t.Errorf("published %d records, want 3", len(decoded.GetRecords()))
	}
	if decoded.GetLabels().GetService() != "checkout" {
		t.Error("published batch lost its labels; the consumer needs them to fingerprint")
	}
}

// The tail copy carries exactly the bytes made durable, and only those: a batch
// the queue refused was never accepted, so nobody tailing should see it.
func TestStreamFansOutOnlyAcceptedBatches(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(nil))

	if ack := exchange(t, client, validBatch("b1", 3)); ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s (%s), want ACCEPTED", ack.GetCode(), ack.GetDetail())
	}
	fanned := pub.Fanned()
	if len(fanned) != 1 {
		t.Fatalf("fanned out %d batches, want 1", len(fanned))
	}
	if want := queue.Subject("tail", "dev", "checkout"); fanned[0].Subject != want {
		t.Errorf("tail subject = %q, want %q", fanned[0].Subject, want)
	}
	if published := pub.Published(); !bytes.Equal(fanned[0].Payload, published[0].Payload) {
		t.Error("tail copy differs from the durable payload; the two must never disagree on which records were accepted")
	}

	pub.FailWith(queue.ErrUnavailable)
	if ack := exchange(t, client, validBatch("b2", 3)); ack.GetCode() == logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatal("refused publish was acked as ACCEPTED")
	}
	if n := len(pub.Fanned()); n != 1 {
		t.Errorf("fanned out %d batches after a refused publish, want still 1", n)
	}
}

// The ordering property the whole package exists for: the ack must not be sent
// until the publish has been acknowledged.
func TestStreamAcksOnlyAfterThePublishCompletes(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	published := make(chan struct{})
	pub := &queuetest.Publisher{}
	pub.OnPublish(func(context.Context, string, []byte) error {
		close(published)
		<-release
		return nil
	})

	_, client := startWith(t, pub, NewMetrics(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.Stream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err = stream.Send(validBatch("b1", 1)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	acks := make(chan *logaggv1.Ack, 1)
	go func() {
		got, recvErr := stream.Recv()
		if recvErr == nil {
			acks <- got
		}
		close(acks)
	}()

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never published the batch")
	}

	// Held inside Publish: an ack now would be a promise the queue has not made.
	select {
	case ack := <-acks:
		t.Fatalf("acked with %v while the publish was still in flight", ack)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if ack := <-acks; ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s, want ACCEPTED once the publish returned", ack.GetCode())
	}
}

func TestStreamRejectsInvalidLabels(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(nil))

	batch := validBatch("b1", 2)
	batch.Labels.Service = "" // Validate requires service, host and env.

	ack := exchange(t, client, batch)

	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_INVALID {
		t.Fatalf("code = %s, want INVALID", ack.GetCode())
	}
	if ack.GetAccepted() != 0 || ack.GetRejected() != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 0/2", ack.GetAccepted(), ack.GetRejected())
	}
	if ack.GetDetail() == "" {
		t.Error("detail is empty; the agent has no way to see what was wrong")
	}
	// Nothing durable happened, so nothing may be queued.
	if pub.Count() != 0 {
		t.Errorf("published %d messages for a batch with invalid labels", pub.Count())
	}
}

// A poison record must not reach the queue: left in the batch it would fail the
// writer's COPY, fail every retry, and be redelivered forever.
func TestStreamFiltersInvalidRecords(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(nil))

	batch := validBatch("b1", 3)
	batch.Records[1].TimeUnixNano = 0                                     // ErrMissingTime
	batch.Records[2].Message = strings.Repeat("x", model.MaxMessageLen+1) // ErrMessageTooLong

	ack := exchange(t, client, batch)

	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s (%s), want ACCEPTED: one record was valid", ack.GetCode(), ack.GetDetail())
	}
	if ack.GetAccepted() != 1 || ack.GetRejected() != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 1/2", ack.GetAccepted(), ack.GetRejected())
	}

	var decoded logaggv1.LogBatch
	if err := proto.Unmarshal(pub.Published()[0].Payload, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decoded.GetRecords()) != 1 {
		t.Fatalf("published %d records, want only the valid one", len(decoded.GetRecords()))
	}
}

func TestStreamRejectsABatchWithNoValidRecords(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(nil))

	batch := validBatch("b1", 2)
	for _, r := range batch.Records {
		r.TimeUnixNano = 0
	}

	ack := exchange(t, client, batch)

	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_INVALID {
		t.Fatalf("code = %s, want INVALID", ack.GetCode())
	}
	if pub.Count() != 0 {
		t.Errorf("published %d messages for a batch with nothing valid in it", pub.Count())
	}
}

// The ack code is what decides whether the agent backs off, retries or drops, so
// each queue failure kind has to map to the right one.
func TestStreamMapsQueueFailuresToAckCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want logaggv1.AckCode
	}{
		{
			name: "a full stream tells the agent to slow down",
			err:  fmt.Errorf("publish: %w: %w", queue.ErrOverloaded, errors.New("max bytes")),
			want: logaggv1.AckCode_ACK_CODE_OVERLOADED,
		},
		{
			name: "an unusable payload is not retryable",
			err:  fmt.Errorf("publish: %w: %w", queue.ErrTooLarge, errors.New("max payload")),
			want: logaggv1.AckCode_ACK_CODE_INVALID,
		},
		{
			name: "an unreachable broker is a collector-side fault",
			err:  fmt.Errorf("publish: %w: %w", queue.ErrUnavailable, errors.New("timeout")),
			want: logaggv1.AckCode_ACK_CODE_INTERNAL,
		},
		{
			// The shape Conn.Publish actually returns on a publish timeout: it bounds
			// itself with the publish timeout, so the error carries both the queue kind
			// and DeadlineExceeded. Matching the context first would report a slow
			// broker as a client that hung up, and count the drop against the agent.
			name: "a publish timeout is the broker's fault, not the client's",
			err:  fmt.Errorf("publish: %w: %w", queue.ErrUnavailable, context.DeadlineExceeded),
			want: logaggv1.AckCode_ACK_CODE_INTERNAL,
		},
		{
			name: "an unclassified failure is internal, not overload",
			err:  errors.New("something unexpected"),
			want: logaggv1.AckCode_ACK_CODE_INTERNAL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pub := &queuetest.Publisher{}
			pub.FailWith(tt.err)
			_, client := startWith(t, pub, NewMetrics(nil))

			ack := exchange(t, client, validBatch("b1", 4))

			if ack.GetCode() != tt.want {
				t.Fatalf("code = %s, want %s", ack.GetCode(), tt.want)
			}
			// Nothing was made durable, so the whole batch is the agent's problem again.
			if ack.GetAccepted() != 0 || ack.GetRejected() != 4 {
				t.Errorf("accepted/rejected = %d/%d, want 0/4", ack.GetAccepted(), ack.GetRejected())
			}
		})
	}
}

func TestStreamHandlesManyBatchesInOrder(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.Stream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	const batches = 8
	for i := 0; i < batches; i++ {
		if err = stream.Send(validBatch(fmt.Sprintf("b%d", i), 2)); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	// Acks come back in send order, which is what lets an agent treat an ack as
	// permission to forget everything up to that batch.
	for i := 0; i < batches; i++ {
		ack, recvErr := stream.Recv()
		if recvErr != nil {
			t.Fatalf("Recv %d: %v", i, recvErr)
		}
		if want := fmt.Sprintf("b%d", i); ack.GetBatchId() != want {
			t.Fatalf("ack %d is for %q, want %q: acks must stay in order", i, ack.GetBatchId(), want)
		}
	}
	if pub.Count() != batches {
		t.Errorf("published %d batches, want %d", pub.Count(), batches)
	}
}

func TestStreamCountsRecordsAndAcks(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	pub := &queuetest.Publisher{}
	_, client := startWith(t, pub, NewMetrics(reg))

	batch := validBatch("b1", 3)
	batch.Records[0].TimeUnixNano = 0

	if ack := exchange(t, client, batch); ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s (%s), want ACCEPTED", ack.GetCode(), ack.GetDetail())
	}

	if got := counter(t, reg, "logagg_ingest_records_received_total", nil); got != 3 {
		t.Errorf("records_received_total = %v, want 3 (counted before validation)", got)
	}
	if got := counter(t, reg, "logagg_ingest_records_accepted_total", nil); got != 2 {
		t.Errorf("records_accepted_total = %v, want 2", got)
	}
	if got := counter(t, reg, "logagg_records_dropped_total", map[string]string{
		"component": observability.ComponentIngest,
		"reason":    reasonInvalidRecord,
	}); got != 1 {
		t.Errorf("records_dropped_total{ingest,invalid_record} = %v, want 1", got)
	}
	if got := counter(t, reg, "logagg_ingest_acks_total", map[string]string{"code": "accepted"}); got != 1 {
		t.Errorf("acks_total{accepted} = %v, want 1", got)
	}
}

// counter reads one counter value, matching every supplied label.
func counter(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matches(m.GetLabel(), labels) {
				continue
			}
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %s%v was not found", name, labels)
	return 0
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	// The detail quotes validation errors, which quote the agent's own data, so the
	// bound is what stops one hostile batch from being echoed back whole.
	long := strings.Repeat("z", MaxAckDetailLen*2)
	got := truncate(long, MaxAckDetailLen)

	if len(got) != MaxAckDetailLen {
		t.Errorf("len = %d, want %d", len(got), MaxAckDetailLen)
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("truncation is not marked, so a cut string looks like the whole one")
	}
	if short := truncate("fine", MaxAckDetailLen); short != "fine" {
		t.Errorf("a short string was altered: %q", short)
	}
}

// End to end for the shedding path: a saturated collector answers OVERLOADED in
// band and keeps the stream open, rather than failing the RPC and taking every
// other batch on that stream down with it.
func TestStreamShedsWhenSaturated(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	defer close(release)

	pub := &queuetest.Publisher{}
	pub.OnPublish(func(context.Context, string, []byte) error {
		<-release
		return nil
	})

	reg := prometheus.NewRegistry()
	cfg := config.Ingest{Addr: "127.0.0.1:0", MaxRecvMsgBytes: 4 << 20, BufferSize: 1, PublishWorkers: 1}
	_, client := startCfg(t, cfg, pub, NewMetrics(reg))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// One stream handles its batches in order, so saturating the pipeline takes
	// several streams: one batch occupies the publisher, one fills the buffer, and
	// the rest have nowhere to go.
	const streams = 6
	acks := make(chan *logaggv1.Ack, streams)
	for i := 0; i < streams; i++ {
		go func(i int) {
			stream, err := client.Stream(ctx)
			if err != nil {
				return
			}
			if err = stream.Send(validBatch(fmt.Sprintf("b%d", i), 2)); err != nil {
				return
			}
			ack, err := stream.Recv()
			if err == nil {
				acks <- ack
			}
		}(i)
	}

	// At least one has to be shed while the publisher is held.
	var shed *logaggv1.Ack
	deadline := time.After(10 * time.Second)
	for shed == nil {
		select {
		case ack := <-acks:
			if ack.GetCode() == logaggv1.AckCode_ACK_CODE_OVERLOADED {
				shed = ack
			}
		case <-deadline:
			t.Fatal("no batch was shed while the pipeline was saturated")
		}
	}

	if shed.GetAccepted() != 0 || shed.GetRejected() != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 0/2", shed.GetAccepted(), shed.GetRejected())
	}
	if got := counter(t, reg, "logagg_records_dropped_total", map[string]string{
		"component": observability.ComponentIngest,
		"reason":    reasonBufferFull,
	}); got == 0 {
		t.Error("records_dropped_total{ingest,ingest_buffer_full} was not incremented")
	}
	if got := counter(t, reg, "logagg_ingest_acks_total", map[string]string{"code": "overloaded"}); got == 0 {
		t.Error("acks_total{overloaded} was not incremented")
	}
}
