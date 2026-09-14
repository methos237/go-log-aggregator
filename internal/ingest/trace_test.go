package ingest

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

// The agent's trace must reach the publish: the batch carries it in, the
// pipeline hands it to the queue, and the queue puts it on the message.
func TestHandleCarriesBatchTraceContextToPublish(t *testing.T) {
	// Global propagator, set once for the package: production sets it in
	// observability.NewTracer, which unit tests never call.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	published := make(chan trace.SpanContext, 1)
	pub := &queuetest.Publisher{}
	pub.OnPublish(func(ctx context.Context, _ string, _ []byte) error {
		published <- trace.SpanContextFromContext(ctx)
		return nil
	})
	_, client := startWith(t, pub, NewMetrics(nil))

	batch := validBatch("b1", 1)
	batch.TraceContext = map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
	if ack := exchange(t, client, batch); ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s (%s), want ACCEPTED", ack.GetCode(), ack.GetDetail())
	}

	select {
	case sc := <-published:
		if got, want := sc.TraceID().String(), "0af7651916cd43dd8448eb211c80319c"; got != want {
			t.Errorf("published trace id = %s, want %s (the agent's)", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publish never happened")
	}
}
