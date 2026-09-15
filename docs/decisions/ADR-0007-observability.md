# ADR-0007: Observability

**Status:** accepted (phase 7)

- **Refines:** ADR-0003 (the ingest batch now carries trace context)
- **Refines:** ADR-0002 (the writer's batch is a traced unit)

## Context

Phase 7 makes the roadmap's observability section true: the full metric set,
one trace from the agent's send to the Postgres commit, and dashboards that are
populated the moment `make dev` returns. Metrics were mostly there; the audit
filled four gaps and renamed nothing that a dashboard could already have been
reading. Tracing was new, and three of its hops had a choice to make.

## Decisions

### OTLP to Jaeger, not a Jaeger exporter

The roadmap said "Jaeger exporter". The OpenTelemetry Go SDK removed its
Jaeger exporter in 2023, and Jaeger has accepted OTLP natively since. The
collector and agent export OTLP over gRPC to whatever listens on
`LOGAGG_TRACING_ENDPOINT`; in the dev stack that is the Jaeger v2 all-in-one.
Nothing in the code knows it is Jaeger, which is the point: the same binary
exports to Tempo or an OpenTelemetry Collector by changing one address.

Tracing is off by default and installs nothing when off. The global provider
stays the API's no-op, so every `tracer.Start` in the tree is a nil check. The
propagator is installed either way, so a node that does not sample still
forwards a context it received.

### Trace context rides in the batch, because a stream has no envelope

The agent ships batches over one long-lived bidirectional gRPC stream. The
usual instrumentation, a stats handler or interceptor, produces one span per
stream, which for a stream that lives hours is one useless span. The unit the
rest of the pipeline sees is the batch, so the batch is the span, and the
W3C `traceparent` travels in a new `LogBatch.trace_context` map field. The
collector's ingest span becomes a child of the agent's ship span, which is
what makes the trace start at the agent rather than at the collector's door.

The alternative, per-message gRPC metadata, does not exist on a stream.
Opening a stream per batch would have given it back at the cost of the
streaming design ADR-0003 chose for backpressure.

### Through the queue as message headers, with our own carrier

The publish injects the current span context into the NATS message headers
and the consumer extracts it, so `writer.batch` on whichever node drains the
message is a child of `jetstream.publish` on whichever node published it. That
hop is what turns two traces into one.

The carrier is `queue.HeaderCarrier`, not the SDK's `propagation.HeaderCarrier`.
The SDK's is `http.Header` underneath and canonicalizes keys on `Get`, so it
only ever looks for `Traceparent`. Against a real broker the key did not
round-trip with its case intact: written through the SDK carrier, it came back
from JetStream as `traceparent`, the canonicalizing `Get` could not see it, and
every writer span became a new trace. The unit tests, which never cross a
broker, passed; the integration test caught it. `HeaderCarrier.Get` matches
case-insensitively, so whichever side changes the case, the context survives.

### The writer's batch has one parent and many links

A flush merges many publishes into one transaction, and a span can have one
parent. `writer.batch` is parented on the first traced shipment in the batch
and linked to every other one. The first trace therefore shows the whole
agent-to-Postgres path as a tree, which is the exit criterion; every other
shipment's trace reaches the write through a link, which Jaeger renders as a
reference. The alternative, one write span per shipment, would have described
work that does not happen: there is one `COPY`, not fifty.

`pg.copy` sits inside `writer.batch` and covers the staging copy, the insert
from staging and the commit, since that is the unit that either lands or does
not.

### Metrics: names as shipped, one honest rename

The audit added `ingest_bytes_total`, `service` and `level` labels on
`ingest_records_accepted_total`, `queue_pending` from the same consumer
metadata the redelivery counter already read, and `query_duration_seconds`
with `query_rows_returned`, observed by an `executor.Instrumented` runner so a
single-node and a clustered query are measured the same way.

The roadmap called the last one `rows_scanned`. What the database scanned is
not visible from the client; only what came back is. The metric says what it
measures. Likewise the `queue_` prefix stands in for the planned `jetstream_`
so the queue package keeps one subsystem.

The compression ratio had no source at all, so a collector reads
`hypertable_compression_stats` on each scrape. It is a catalog aggregate, not a
chunk scan, and a slow database degrades to a missing sample and a debug log,
never a failed scrape.

### Dashboards are code

One dashboard, provisioned from `deploy/grafana/dashboards/`, with datasources
and the provider from `deploy/grafana/provisioning/`. Prometheus finds
collectors through Compose DNS so `make dev-scale` replicas appear as they
join. Series with fixed identities (percentiles, queues, levels, outcomes) get
fixed colors from a palette validated for color-vision deficiency; series keyed
on node names use Grafana's by-name palette so a color follows its node when
the set changes. Grafana runs with anonymous admin on loopback, which is the
"zero clicks" and is only acceptable because nothing off-box can reach it.

## Consequences

- `internal/observability` grows `NewTracer`; `internal/query/executor` grows
  `Metrics` and `Instrumented`; `internal/queue` grows `HeaderCarrier` and
  `Message.Header`; `internal/storage` grows the compression collector and a
  `Shipment.Context`. The proto gains one field.
- The agent's own logs now flow through it in the dev stack: the compose agent
  follows the collector container over a read-only Docker socket with JSON
  extraction on, and an extracted `level` field sets the record level.
- Every `make dev*` target applies the observability overlay. The core compose
  file still runs alone for anyone who does not want three more containers.
- Measured on a laptop through the real stack: a five-span trace,
  `agent.ship` through `pg.copy`, in about 5 to 11 ms end to end.
