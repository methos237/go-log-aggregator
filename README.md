# Log Aggregator

[![CI](https://github.com/methos237/go-log-aggregator/actions/workflows/ci.yml/badge.svg)](https://github.com/methos237/go-log-aggregator/actions/workflows/ci.yml)

A distributed log aggregator built with Go, NATS JetStream and TimescaleDB.

An agent tails log files next to your programs and streams new lines to a cluster of
collectors over gRPC. A collector validates each batch, publishes it to a durable queue,
and only then tells the agent it is safe to forget. Writer workers drain the queue into
TimescaleDB hypertables with `COPY`, and a unique index turns any replay into a no-op.
Collectors find each other by gossip and share query work over a consistent hash ring,
so nodes can join, leave or be killed while ingest continues. Queries are written in a
small LogQL-shaped language that compiles to parameterized SQL, and a live tail streams
matching lines over WebSocket as they arrive. Every hop carries a trace, every component
exposes metrics, and everything is covered by tests that run against real TimescaleDB
and NATS in Docker.

**Delivery semantics: at-least-once end to end, deduplicated at the storage layer.**
Not exactly-once: a crash between a database write and its acknowledgement replays the
batch, and a unique index makes the replay a no-op.

## Ten-minute tour

```bash
make dev                                        # TimescaleDB 5432, NATS 4222, collector 8080/9090/9095, Prometheus 9091, Grafana 3000, Jaeger 16686
make build                                      # bin/collector bin/agent bin/logctl bin/loadgen
echo "hello from logctl" | ./bin/logctl send -addr 127.0.0.1:9095 -service demo
./bin/loadgen -addr 127.0.0.1:9095 -records 200000          # synthetic load; prints ack percentiles, exits non-zero on any rejection
export LOGAGG_HTTP_AUTH_TOKEN=dev-token                      # what deploy/docker-compose.yml sets
./bin/logctl query -since 15m '{service="demo"}'
./bin/logctl query '{env="dev"} | count_over_time(1m) by (level)'   # answered from a continuous aggregate
./bin/logctl tail '{service="demo"} |= "hello"'              # WebSocket live tail; Ctrl-C to stop
make dev-agent                                  # add the agent and a sidecar that writes and rotates a log file for it
make dev-scale N=5                              # five collectors behind an nginx proxy on the same 8080 and 9095
curl -s -H "Authorization: Bearer dev-token" localhost:8080/v1/cluster | jq '.members[].name, .ring'
make chaos                                      # kill two of five collectors mid-ingest, assert every line landed with no gaps
```

Open Grafana at http://127.0.0.1:3000 to watch ingest move between collectors during
`make chaos`, and Jaeger at http://127.0.0.1:16686 for one trace from the agent's send
to the Postgres commit.

## Architecture

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│ agent 1 │  │ agent 2 │  │ agent N │   tail files, checkpoint per file, batch by size and time
└────┬────┘  └────┬────┘  └────┬────┘
     │  gRPC bidi stream (protobuf, batched, ack'd after the JetStream publish; mTLS optional)
     └────────────┼────────────┘
                  ▼
   ┌───────────────────────────────────────────┐
   │ collector × N     memberlist gossip +     │
   │                   consistent hash ring    │
   │  ingest: validate, fingerprint labels,    │
   │          shed when the queue is full      │
   │     │                                     │
   │     ▼ publish            ┌──────────────┐ │
   │  NATS JetStream ────────>│ writer pool  │ │──COPY──> TimescaleDB hypertable
   │  NATS core tail.> ──────>│ tail fan-out │ │──WS────> /v1/tail
   │                          └──────────────┘ │
   │  query: DSL → parameterized SQL, fan out  │<──SQL──── TimescaleDB
   │         by ring owner, merge by time      │
   └───────────────────────────────────────────┘
        │ /metrics, OTLP traces
        ▼
   Prometheus · Grafana · Jaeger
```

Each collector runs every role. The ring divides *reads*: any collector accepts any
agent and writes through the shared JetStream stream, and a query fans out to whichever
collectors own the matching streams. A collector that dies mid-batch leaves its
unacknowledged messages in JetStream for a survivor to write.

| Binary | Role | Listens on |
|---|---|---|
| `collector` | gRPC ingest, JetStream writer pool, query API, live tail, cluster membership | 8080 HTTP, 9095 ingest, 9096 peer, 7946 gossip, 9090 admin |
| `agent` | Tails files, Docker containers or stdin; checkpoints on ack; spools through outages | 9090 admin |
| `logctl` | Operator CLI: `send`, `query`, `tail`, `version` | – |
| `loadgen` | Paced synthetic load with ack latency percentiles and a JSON summary | – |

| Package | Owns |
|---|---|
| `internal/agent` | Sources, multiline joining, field extraction, batching, spool, checkpoint store |
| `internal/ingest` | gRPC front door: validate, fingerprint labels, publish, then ack; the one client shared by agent, `logctl` and `loadgen` |
| `internal/queue` | JetStream publish and pull consumers, overload and too-large classification |
| `internal/storage` | Connection pool, embedded migrations, `streams` dimension table, `COPY` writer pool |
| `internal/query` | Lexer, parser and planner from DSL to parameterized SQL; `executor` shapes rows for the API |
| `internal/cluster` | memberlist gossip, consistent hash ring, query fan-out to peers |
| `internal/tail` | Live-tail subscription registry over NATS core fan-out |
| `internal/httpapi` | `/healthz`, `/readyz`, `/v1/query`, `/v1/labels`, `/v1/cluster`, `/v1/tail` behind a bearer token |
| `internal/observability` | Logging, Prometheus metrics, OpenTelemetry tracing, admin listener with pprof |
| `internal/tlsx` | Mutual TLS credentials for every gRPC hop |

### Record flow

| Hop | Transport | Guarantee |
|---|---|---|
| file → agent | tail with per-file checkpoint | offset advances only on ack; rotation and truncation detected |
| agent → collector | gRPC bidirectional stream, batches of records | ack after the JetStream publish; unacked batches resent on reconnect |
| collector → JetStream | publish, one message per batch | durable before the agent is told; full stream sheds with a retryable error |
| JetStream → writer | pull consumer, `ack_wait` bounded | a killed writer's batch is redelivered to a survivor |
| writer → TimescaleDB | `COPY` into the hypertable | unique `(stream_id, seq, time)` index; replay falls back to staging + `ON CONFLICT DO NOTHING` |
| ingest → live tail | NATS core `tail.>` fan-out | best effort; a slow client loses records and is told how many |

Every record carries a `stream_id`, a 64-bit hash of its canonical label set, so every
node derives the same id with no coordination. `seq` is the agent's per-stream sequence
and is what makes a replay detectable.

Ports, all bound to loopback in development:

| Port | Listener | Notes |
|---|---|---|
| 8080 | Public HTTP API | health, `/v1/query`, `/v1/labels`, `/v1/cluster`, `/v1/tail` |
| 9090 | Admin | Prometheus metrics and pprof. Keep this off any public network. |
| 9095 | gRPC ingest | mTLS when configured; plaintext by default |
| 9096 | gRPC peer | query fan-out between collectors; mTLS or loopback unless opted out |
| 7946 | memberlist gossip | UDP and TCP; never published to the host |
| 5432 | TimescaleDB | development credentials only |
| 4222 | NATS | 8222 serves its monitoring endpoint |
| 3000 | Grafana | anonymous viewer, dashboards provisioned from `deploy/grafana/` |
| 9091 | Prometheus | scrapes every collector and agent through Compose DNS |
| 16686 | Jaeger | traces; collectors and the agent export OTLP to it on 4317 |

## Stack

Go 1.27 · gRPC + Protocol Buffers (`buf`) · NATS JetStream · TimescaleDB 2.22 on PostgreSQL 17 · pgx · hashicorp/memberlist · coder/websocket · OpenTelemetry + Prometheus · Grafana · Jaeger · testcontainers-go · golangci-lint v2 · ast-grep · Docker Compose · GitHub Actions

Prerequisites: Docker with Compose v2. Go 1.27 for `make build` and the tests; the
services themselves are built inside Docker, so `make dev` works with Docker alone.

## Agent

The agent tails local sources (files, Docker containers, stdin), joins multiline
records, extracts fields, batches by size and time, and ships over one gRPC stream. A
source's progress is durable only once the collector has acknowledged it: offsets are
advanced by the checkpoint store, driven by acks, not by reads. A restart or a network
blip therefore loses nothing, and a rotated or truncated file is picked up where the old
one ended.

Backpressure runs the opposite way to the collector's. The file is already durable on
disk, so a full channel simply stops the reader; records are dropped only where the
bound is genuinely hard, an over-long line or a full on-disk spool. During a collector
outage the agent spools to disk and replays on reconnect.

```bash
make dev-agent                 # agent plus a sidecar that writes numbered lines and rotates its file
make demo-restart-collector    # restart the collector under the running agent
make demo-rotate-log           # force an out-of-band rotation in the sidecar
make e2e-agent                 # both, then assert every numbered line is in the database with no gaps
```

The dev agent also follows the collector's own container, so the aggregator ingests its
own logs.

## Ingest

The collector's gRPC front door validates each record, fingerprints its label set into
a `stream_id`, and publishes the batch to JetStream. Only after the publish succeeds is
the agent acked, so an acked record is durable in the broker before anything touches
the database. When the stream is full the collector sheds with a retryable status
instead of blocking, because there is a waiting client to say "slow down" to. A message
that cannot be decoded is terminated rather than retried, since retrying a corrupt
payload can never succeed.

Two settings are checked against each other at startup, since getting them wrong is
silent. `LOGAGG_INGEST_MAX_RECV_BYTES` plus the room a publish's trace headers take must
not exceed the broker's `max_payload`, or a batch above it would be accepted, validated,
then refused as unsendable; the collector verifies this against the live broker.
`LOGAGG_QUEUE_ACK_WAIT` must exceed the writer's worst case, or a batch still being
written gets redelivered.

## Storage

Label sets are deduplicated into a `streams` dimension table keyed by the 64-bit hash
of their canonical encoding. Log rows are narrow and reference it. `logs` is a
TimescaleDB hypertable with 1 hour chunks, columnar compression segmented by stream,
30 day retention, and per-minute and per-hour count aggregates that the query planner
reads when an aggregation can be answered from them exactly. Old chunks are compressed,
then dropped, by TimescaleDB's own background jobs. Compression measured at 5.3×
(382 to 72 bytes per row).

The write path is a direct `COPY` into the hypertable, guarded by a unique
`(stream_id, seq, time)` index. A redelivered batch trips that index, and the writer
then redoes it through a session-local staging table and
`INSERT ... ON CONFLICT DO NOTHING`, so a replay is a no-op instead of a duplicate or
an error, and the normal case pays for one pass. `LOGAGG_WRITER_COPY_MODE=staging`
makes every batch take the second path, for a deployment where replay is routine. The
migrations in [`migrations/`](migrations/) carry the reasoning inline, including the
TimescaleDB behaviours that shaped the schema.

## Query language

A LogQL-shaped language compiled to parameterized SQL. A query is a stream selector,
then optional line filters, parser stages and label filters, then an optional
aggregation:

```
{service="api", env!="dev"} |~ "timeout|deadline"
{service=~"api-.*", level>="warn"} != "healthcheck"
{service="api"} | json | status >= 500
{service="api"} | logfmt | route = "/v1/query" | rate(5m) by (level)
```

The planner resolves the selector against the `streams` table first, so the hypertable
scan is a `stream_id = ANY($1)` range. `level` is a record column, not a stream label.
Label filters read the fields the agent extracted, or the fields of the nearest `json`,
`logfmt` or `regexp` stage. When an aggregation needs only counts by level or by
`service`, `host` or `env`, the planner answers it from a continuous aggregate and
reports which relation it read as `source`.

Every literal, including label names, becomes a `$n` argument. Tests assert the
generated SQL contains no user bytes, an ast-grep rule forbids concatenated SQL at any
query call, and `make fuzz` runs the lexer, parser and planner for a minute each. The
full reference, with every operator and a worked example of each, is
[`docs/query-language.md`](docs/query-language.md).

### HTTP API

All `/v1` routes need `Authorization: Bearer <LOGAGG_HTTP_AUTH_TOKEN>`. With no token
configured they refuse every request.

| Method | Path | Body / result |
|---|---|---|
| `POST` | `/v1/query` | `{query, start, end, limit, direction}` returns `records` or `points`, plus `source`, `start`, `end`, `streams`, `truncated`, `warnings`, `elapsed_ms` |
| `GET` | `/v1/labels` | label names for autocomplete |
| `GET` | `/v1/labels/{name}/values` | distinct values of one label |
| `GET` | `/v1/cluster` | members, ring shares and ownership; `?arcs=1` for every range |
| `GET` | `/v1/tail?query=` | WebSocket; one JSON record per message with the stream's `labels` and, when the client fell behind, a `dropped` count |

`start` and `end` are RFC 3339 and default to the last hour. For an aggregation the
server widens them to whole buckets and echoes the result back. `limit` defaults to
1000 and is capped by `LOGAGG_HTTP_QUERY_MAX_ROWS`; `truncated` is true when more rows
matched. `direction` is `backward` (default) or `forward`. `LOGAGG_HTTP_QUERY_TIMEOUT`
bounds each request. In a cluster, `warnings` lists any member whose share of the
streams could not be searched.

`logctl` exits 0 on success, 2 for a usage error and 1 for anything the cluster
refused or could not reach; rows go to stdout and everything else to stderr, so
`logctl query -json ... | jq` works.

## Cluster

Collectors find each other with `hashicorp/memberlist` gossip and build a consistent
hash ring over their names, 128 virtual nodes each. Every collector writes through
JetStream, so the ring divides reads, not data: whichever collector takes a query
resolves the matching streams, splits the ids by owner, ships each owner the query
text and its share of the ids to compile and scan itself, and merges the rows. A peer
that cannot be reached becomes a `warnings` entry in the response instead of a failed
query.

```bash
make dev-scale N=5                       # five collectors behind the proxy
curl -s -H "Authorization: Bearer dev-token" http://127.0.0.1:8080/v1/cluster | jq
make chaos                               # kill two mid-ingest, assert zero gaps
```

![Two of five collectors killed under load: membership drops to three, the ring
rebalances, ingest moves to a survivor, nothing is dropped](docs/images/rebalance.gif)

The recording is the overview dashboard while `loadgen` pushes a steady 3 000
records/s through the proxy and two collectors are killed twenty-five seconds
apart, each the one carrying the ingest stream at the time. The client reconnects
through the proxy to a survivor, the membership timeline steps from five to three,
the ring ownership panel spikes as the survivors take over the dead nodes' ranges,
and the drop counter stays at zero.

The chaos test runs the agent through the proxy, kills two collectors outright while
lines are in flight, then asserts every numbered line landed with no gaps, that the
ring settled on the survivors, and that a fanned-out count agrees with the database. It
waits out the queue's `ack_wait`, since a killed collector's unacknowledged deliveries
are redelivered only after it expires. Kill, not stop: a graceful shutdown withdraws
readiness, leaves the ring and drains its consumer, and none of that happens here.

`make dev` publishes fixed host ports for its single collector. `make dev-scale N=5`
puts an nginx proxy on the same 8080 and 9095 in front of N collectors, resolving
them through Compose's DNS on every connection, so the host addresses never change
and a killed replica drops out of rotation within seconds. Only the admin port stays
per replica, on 9190-9199; `make dev-ps` shows which is which.

## Live tail

```bash
./bin/logctl tail '{service="demo", level>="warn"} |= "hello"'
```

The ingest path publishes a second copy of each batch to a NATS core subject, and a
per-collector registry matches it against every open `/v1/tail` subscription with an
in-memory evaluator from the query package; an integration test keeps that evaluator
in agreement with the SQL the planner emits, so a tail and a query see the same records. `tail` takes a
selector and line filters; parser stages and aggregations need `query`. A client that
falls behind loses records rather than slowing ingest, and is told how many it missed.
Core NATS rather than JetStream because a tail is a window on the present, not a
durable subscription, and nothing needs replaying to a client that reconnects.

## Observability

Every collector and agent exposes Prometheus metrics on its admin port, all labeled
by `node`: ingest records by service and level, bytes, drops by component and reason,
queue depth and wait, JetStream backlog and redeliveries, batch size, write latency,
query latency by source relation, compression ratio, cluster membership and ring
changes, live tail subscriptions and drops. `make dev` starts Prometheus, Grafana and
Jaeger alongside the stack, and Grafana comes up with this dashboard provisioned:

![Grafana overview dashboard](docs/images/grafana-overview.png)

Tracing is OpenTelemetry, exported over OTLP to Jaeger in the dev stack, and one trace
covers a batch from the agent's send to the Postgres commit:

```
agent.ship ──> collector.ingest ──> jetstream.publish ──> writer.batch ──> pg.copy
  (agent)         (collector)          (collector)        (any collector)
```

![One trace from agent to Postgres in Jaeger](docs/images/jaeger-trace.png)

The agent's span context travels inside the batch, since a long-lived gRPC stream
has no per-message metadata, and then in the NATS message headers, so the write span
joins the trace on whichever collector drains the message. A writer batch merges
many publishes into one transaction, so its span is a child of the first shipment's
trace and linked to the rest. Tracing is off unless `LOGAGG_TRACING_ENABLED=true`;
`LOGAGG_TRACING_ENDPOINT` and `LOGAGG_TRACING_SAMPLE_RATIO` do what they say.

## Security

The admin listener is separate from the public one, and configuration validation
refuses to start if they share an address, because pprof is unauthenticated and
exposes heap contents. Every `/v1` route requires a bearer token and refuses all
requests when none is configured.

Ingest speaks plaintext when no TLS files are configured, which is what lets `make dev`
run with no setup. Enabling TLS requires all three of a server certificate, its key,
and a client CA. Configuring the first two without the third is refused at startup
rather than served as one-way TLS, which would encrypt the connection while letting
anyone who can reach the port write logs into the cluster.

```bash
make certs         # writes certs/ (gitignored): ca, server, client
make certs-verify  # show what those certificates claim

LOGAGG_INGEST_TLS_CERT_FILE=certs/server.pem \
LOGAGG_INGEST_TLS_KEY_FILE=certs/server-key.pem \
LOGAGG_INGEST_TLS_CLIENT_CA_FILE=certs/ca.pem \
  go run ./cmd/collector
```

Without TLS, ingest binds loopback by default and refuses a routable address unless
`LOGAGG_INGEST_ALLOW_PLAINTEXT=true` is set. The compose stack sets it, because a
container has to bind every interface for Docker to forward to it, and publishes the
port on `127.0.0.1` only. Plaintext ingest also logs at WARN, since it is an
unauthenticated write path.

A verified client certificate authorises writing anything, not writing as a particular
service; ingest is one trust domain. Per-service authorisation is deferred rather than
half-implemented: a certificate-to-service mapping without rotation and revocation
behind it would be a policy the development CA cannot honour. These certificates are
for local development only: a CA whose private key sits in your working tree signs
them, they last a year, and nothing can revoke them. `certs/` and `*.pem` are
gitignored.

## Benchmarks

Measured on a laptop, so read the deltas, not the absolutes: Apple M2 Pro, Docker
Desktop VM with 10 CPUs and 7.65 GiB, TimescaleDB 2.22 on PG 17 with image defaults,
data on a Docker volume. The load shape is 3 000 000 records of 200 bytes in batches
of 500 across 64 streams on 4 gRPC streams, unpaced, into one collector. "Client" is
the rate the collector accepted; "sustained" is accepted records over client time plus
the drain into the database, which is the number that matters when the client outruns
the writer. Single runs unless a range is given; the noise floor on this machine is
about 20%.

| State | Client rec/s | Sustained rec/s | Ack p99 (ms) | Write per 5 000-row batch (ms) |
|---|---|---|---|---|
| Baseline (phase 7 code, image defaults) | 65 374 / 64 083 | 33 186 / 27 133 | 1 380 / 1 100 | 588 / 726 |
| 1. `max_wal_size` 1 GB → 4 GB | 102 568 | 30 042 | 903 | 654 |
| 2. Fewer allocations (−37% bytes) | 52 059 | 34 995 | 4 552 | 564 |
| 3. `SET LOCAL work_mem` in the staging insert | 91 569 / 62 786 / 70 699 | 44 397 / 36 200 / 34 817 | 1 563 / 5 358 / 2 015 | 437 / 552 / 562 |
| 4. Direct `COPY` with staging fallback | 90 267 / 62 739 / 118 349 | 47 070 / 32 031 / 50 883 | 985 / 2 916 / 691 | 390 / 555 / 385 |
| 5. Drop the default `time` index | 183 553 | **55 903** | 496 | 357 |

![Sustained throughput after each optimization](docs/benchmarks/charts/optimization-log.svg)

The writer is the bottleneck throughout: the client pushes 90–120k records/s into
JetStream and the database absorbs 30–55k/s. Every change above, what it was expected
to do, what it did, and the flame graphs behind it are in
[`docs/benchmarks/`](docs/benchmarks/README.md), along with the defaults it changed.
Reproduce a run with `make bench-run NAME=mine`; `loadgen -rate`, `-ramp` and
`-duration` pace a run by hand.

## Configuration

Every setting has a default that makes the compose stack work unconfigured. Override
with `LOGAGG_`-prefixed environment variables, for example `LOGAGG_LOG_LEVEL=debug`,
`LOGAGG_HTTP_ADDR=:9000` or `LOGAGG_WRITER_BATCH_SIZE=10000`.
[`internal/config/config.go`](internal/config/config.go) has the full list. Invalid
values are reported all at once at startup rather than one per restart.

## Design decisions

- **Ack after the publish, not after the write.** An agent's record is durable in
  JetStream before the agent is told to forget it, so a collector can die at any point
  after the ack without loss. Acking after the database write would tie ingest latency
  to write latency and make the writer pool a per-connection concern.
- **At-least-once plus a dedup index, not exactly-once.** A unique
  `(stream_id, seq, time)` index is a few lines of SQL, works with any broker
  semantics, and survives offset resets. Exactly-once between a broker and a database
  is a stretch item with a lot of machinery for a guarantee the index already gives.
- **Direct `COPY` with a staging fallback.** The benchmark measured direct `COPY` at
  roughly 1.3× the staging path, and a replay is rare. The normal case pays for one
  pass; a dedup hit redoes the batch through staging and `ON CONFLICT DO NOTHING`.
- **Shed at ingest, block at the agent.** The collector has a waiting client to say
  "slow down" to, so a full queue returns a retryable error. The agent's only unbounded
  resource is a file already durable on disk, so a full channel stops the reader.
- **The ring divides reads, not data.** Every collector writes through one JetStream
  stream, so ingest never depends on membership and a dead collector's batch is
  redelivered to a survivor. Only query fan-out consults the ring, and a missing peer
  is a warning, not a failed query.
- **Stream ids from a hash, not a sequence.** A 64-bit hash of the canonical label set
  means every node derives the same id with no coordination and no round trip, at the
  cost of a dimension table upsert that is idempotent by construction.
- **Parameterized SQL only, enforced three ways.** Every literal becomes a `$n`
  argument; tests assert no user bytes reach the SQL; an ast-grep rule fails lint on
  any concatenated query; and the planner gets its placeholders from one function that
  also records their values, so a literal `$n` in the compiler is itself a lint error.
- **Continuous aggregates when they are exact.** Counts by level or by a stream label
  come from per-minute and per-hour materializations; anything a parser stage touches
  scans the hypertable. The response says which relation answered.
- **Core NATS for live tail, JetStream for ingest.** A tail is a window on the present
  and nothing needs replaying to a client that reconnects; a slow tail client is
  dropped rather than allowed to slow ingest.
- **All-or-nothing mTLS.** A server certificate without a client CA is refused rather
  than served as one-way TLS, which would encrypt a write path anyone could reach.
- **Horizontal distribution in Go, not in the database.** Timescale deprecated
  distributed hypertables, so gossip, the ring and fan-out live in the collector, which
  is the point of the project.
- **testcontainers-go, not mocks, for anything that touches Postgres or NATS.**
  Integration tests run against the same TimescaleDB and NATS images the stack runs,
  each test in its own freshly migrated database and its own JetStream stream. CI does
  the same.
- **AGPL rather than MIT.** This is network server software: anyone who runs a modified
  version as a service has to offer their users the corresponding source.

## Tests

446 tests and 4 fuzz targets. `make ci` runs what CI enforces: gofmt, `go mod tidy`,
golangci-lint over the integration-tagged code too, protobuf lint and generated-code
freshness, the ast-grep architectural invariants, unit tests under the race detector
with a coverage summary, integration tests, `govulncheck`, the collector image, and a
Compose smoke test that starts the stack and checks health, readiness and metrics.

| Level | Where | What |
|---|---|---|
| Unit (408) | every package | agent sources, rotation and truncation, checkpointing, multiline and field extraction; ingest validation and fingerprinting; lexer, parser and planner golden SQL with no user bytes; ring ownership and rebalancing; HTTP handlers and auth; metric and trace wiring |
| Fuzz | `internal/query`, `internal/model` | lexer, parser and compiler never panic on arbitrary input; a canonical label set always decodes |
| Integration (38, testcontainers) | `test/integration` | migrations create the hypertable, dedup index, policies and aggregates and round-trip down; the writer inserts one million records, deduplicates a full and a partial replay, and survives concurrent writers across chunks; ingest to TimescaleDB end to end; killing the collector loses no acked record; overload sheds instead of growing; agent survives rotation, truncation, restart and a spooled outage; tail delivers at sub-second latency and a stalled client does not slow ingest; the tail evaluator agrees with SQL; peer execution and coordinator fan-out |
| Stack | `make e2e-agent`, `make chaos` | real Compose stack: restart and rotation under the agent with no gaps; two of five collectors killed mid-ingest with no gaps and a settled ring |
| Architecture | `make lint-arch` | ast-grep: no unbuffered channel carries data, no concatenated SQL at a query call, no literal `$n` in the compiler |

Integration tests start their own TimescaleDB container. For a faster inner loop, point
them at a running `make dev` stack; each test still gets its own freshly migrated
database, and `LOGAGG_TEST_NATS_URL` does the same for the broker:

```bash
LOGAGG_TEST_DB_DSN='postgres://logagg:logagg@127.0.0.1:5432/logagg?sslmode=disable' \
  LOGAGG_TEST_RECORDS=50000 make test-integration
```

`LOGAGG_TEST_RECORDS` scales the throughput test down from its default of one million.

## Development

```bash
make test              # unit tests, no Docker needed
make test-race         # unit tests under the race detector
make test-integration  # integration tests against a real TimescaleDB and NATS
make lint              # golangci-lint, including the integration-tagged tests
make lint-arch         # architectural invariants (ast-grep)
make cover             # coverage report
make ci                # everything CI enforces
make fuzz              # fuzz the query lexer, parser and planner (FUZZ_TIME=60s)
make proto             # regenerate protobuf code (pinned buf + plugins)
make migrate           # apply migrations to DB_DSN
make migrate-status    # print the applied schema version
make certs             # development mTLS material (gitignored)
make help              # every target
```

`make proto` needs no protoc: `buf` and the plugins are Go modules fetched at pinned
versions, and the generated code is committed so a clean clone builds with only a Go
toolchain. The project was built in nine phases, each one GitHub issue and one pull
request, and the reasoning behind each design decision is recorded next to the code it
shaped: package documentation, migration comments and the pull requests.

## Out of scope, on purpose

Not a Loki or Elasticsearch replacement. No multi-tenancy, and no RBAC beyond a static
API token. No cross-region replication. No log parsing or enrichment plugin system:
field extraction is regex at the agent and `json`, `logfmt` and `regexp` stages at
query time. No TimescaleDB multi-node, since Timescale deprecated distributed
hypertables and the distribution lives in the Go layer. No Kubernetes manifests or
Helm chart, and no public cloud demo; everything runs under Docker Compose via
`make dev`. Each would layer on top of the pipeline shown here without changing it.
All nine planned phases have landed.

## License

Copyright (c) 2026 James Polk. Licensed under the
[GNU Affero General Public License v3.0](LICENSE).
