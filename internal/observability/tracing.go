package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

// NewTracer installs the process-wide tracer provider and propagator and
// returns the function that flushes and stops it.
//
// The provider is registered globally rather than passed around because the
// instrumentation that produces most spans (gRPC interceptors, the pgx tracer)
// reads otel.GetTracerProvider itself. Disabled tracing installs nothing: the
// global stays the API's no-op provider, so every otel.Tracer call in the tree
// costs a nil check and no allocation. The propagator is installed either way,
// so a disabled node still forwards a trace context it did not sample.
//
// service is the service.name resource attribute ("logagg-collector",
// "logagg-agent"); node becomes service.instance.id so a five-replica cluster's
// spans stay attributable to the replica that produced them.
func NewTracer(ctx context.Context, cfg config.Tracing, service, node string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	// The exporter dials lazily, so an endpoint that is down at startup costs a
	// logged export failure later rather than a failed boot: tracing is never
	// worth refusing to ingest logs over.
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
		semconv.ServiceVersion(version.Version),
		semconv.ServiceInstanceID(node),
	))
	if err != nil {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// ParentBased so a child honors the root's decision: the agent decides
		// once and the collector, writer and copy all follow or all skip.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
