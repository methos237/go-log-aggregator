package observability

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

func TestNewTracerDisabledInstallsNoProvider(t *testing.T) {
	before := otel.GetTracerProvider()
	shutdown, err := NewTracer(context.Background(), config.Tracing{}, "logagg-test", "n1")
	if err != nil {
		t.Fatalf("NewTracer: %v", err)
	}
	if otel.GetTracerProvider() != before {
		t.Error("disabled tracing replaced the global provider")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	// The propagator is installed regardless, so an unsampled node still
	// forwards trace context.
	if fields := otel.GetTextMapPropagator().Fields(); len(fields) == 0 {
		t.Error("no propagator installed")
	}
}

func TestNewTracerEnabledInstallsSDKProvider(t *testing.T) {
	cfg := config.Tracing{Enabled: true, Endpoint: "127.0.0.1:1", Insecure: true, SampleRatio: 1}
	shutdown, err := NewTracer(context.Background(), cfg, "logagg-test", "n1")
	if err != nil {
		t.Fatalf("NewTracer: %v", err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("global provider = %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
	}
	// Nothing was recorded, so shutdown has nothing to flush and must not
	// block on the unreachable endpoint.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}
