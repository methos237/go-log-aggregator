package queue

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// A broker may hand the key back in another case than it was written; the
// carrier must still find it, or every consumer-side span becomes a new trace.
func TestHeaderCarrierGetIgnoresCase(t *testing.T) {
	t.Parallel()

	want := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	prop := propagation.TraceContext{}
	h := HeaderCarrier{}
	prop.Inject(trace.ContextWithSpanContext(context.Background(), want), h)

	recased := HeaderCarrier{}
	for k, v := range h {
		recased["X-"+k] = v // wrong prefix: must not match
		recased[toggleCase(k)] = v
	}
	got := trace.SpanContextFromContext(prop.Extract(context.Background(), recased))
	if got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() {
		t.Fatalf("extracted %v, want %v", got, want)
	}
}

func toggleCase(s string) string {
	b := []byte(s)
	for i, c := range b {
		switch {
		case c >= 'a' && c <= 'z':
			b[i] = c - 'a' + 'A'
		case c >= 'A' && c <= 'Z':
			b[i] = c - 'A' + 'a'
		}
	}
	return string(b)
}
