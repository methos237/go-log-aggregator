# go-log-aggregator

A distributed log aggregator written in Go, backed by TimescaleDB.

Agents stream logs over gRPC to a cluster of collectors. Collectors buffer through NATS
JetStream, batch-write into TimescaleDB hypertables, and answer queries written in a
small log query language. Collectors find each other by gossip and share work over a
consistent hash ring, so nodes can join and leave while ingest continues.

Delivery is at-least-once end to end, with duplicates removed at the storage layer.

## What it does, in plain terms

Programs write logs: one line per event, thousands per second across a fleet of
machines. When something breaks, you want to search those lines from one place, filter
them by which service and machine produced them, and chart how often something happened
over time. This project is the machinery for that.

An **agent** runs next to your programs, tails their log files, and ships new lines to
the cluster. It remembers where it left off, so a restart or a network blip loses
nothing and a rotated log file is picked up where the old one ended.

A **collector** receives those lines. It checks them, files them under the labels that
identify their source (service, host, environment), and hands them to a durable queue.
Workers drain the queue and write the lines to the database in large batches, which is
what lets a small cluster keep up with a busy fleet. If a collector dies mid-write, the
queue still holds the lines and another worker writes them.

The **database** is PostgreSQL with the TimescaleDB extension, which stores
time-ordered rows in time-sliced chunks. Old chunks are compressed, then dropped after
thirty days. Per-minute and per-hour counts are kept up to date in the background, so a
chart over a week reads a few thousand summary rows instead of millions of raw ones.

The **query language** is how you ask questions. `{service="api", level>="warn"} |=
"timeout"` means: warnings and errors from the api service whose text contains
"timeout". You can pull fields out of JSON or key=value lines and filter on them, or
turn the matches into a rate over time. Every query runs against the database through
a fixed set of parameterized statements, so nothing you type can become SQL.

Several collectors form a **cluster**. They learn about each other by gossip, divide
the streams between them with a hash ring, and forward queries to whichever node holds
the data. A **live tail** lets you watch matching lines arrive as they happen.

Two command-line tools ship with it: `logctl` for sending test lines, running queries,
and inspecting the cluster, and `loadgen` for pushing synthetic load through the
pipeline to see what it can take.

## Status

Phases 1 through 7 of 9 are complete: the schema and write path, the ingest service,
the agent, the query language with its HTTP API and `logctl query`, the cluster
layer (gossip membership, hash ring, query fan-out, a proxy for the scaled stack, and
a chaos test that kills two of five collectors mid-ingest), live tail over
WebSocket with `logctl tail`, and observability: the full metric set, one trace from
the agent's send to the Postgres commit, and Grafana dashboards provisioned from git.
Phase 8 adds benchmarks, phase 9 polish. Each phase is one GitHub issue
and one pull request, and every design decision that shaped the code is written up in
[`docs/decisions/`](docs/decisions/).

## Quickstart

Requires Docker. Go is only needed to run tests and linters locally.

```bash
make dev          # build and start the stack, wait until healthy
make dev-logs     # follow logs
make dev-scale N=3  # run 3 collectors behind a proxy on the same host ports
make dev-down     # stop, keeping data
make dev-nuke     # stop and delete volumes
```

Then:

```bash
curl -s http://127.0.0.1:8080/healthz          # liveness
curl -s http://127.0.0.1:8080/readyz           # readiness, per-dependency detail
curl -s http://127.0.0.1:9090/metrics | head   # Prometheus metrics
open http://127.0.0.1:3000/dashboards           # Grafana, provisioned, no login
open http://127.0.0.1:16686                     # Jaeger
```

`make help` lists every target.

Push some records through it:

```bash
make build
echo "hello from logctl" | ./bin/logctl send -addr 127.0.0.1:9095 -service demo
./bin/loadgen -addr 127.0.0.1:9095 -records 200000    # synthetic load
```

`loadgen` prints accepted and rejected counts and exits non-zero if anything was
rejected, so you can use it as a check as well as a demo. The configurable-rate load
generator arrives in phase 8.

Then query them back:

```bash
export LOGAGG_HTTP_AUTH_TOKEN=dev-token           # what deploy/docker-compose.yml sets
./bin/logctl query -since 15m '{service="demo"}'
./bin/logctl query '{service="demo", level>="warn"} |= "hello"'
./bin/logctl query '{service="demo"} | count_over_time(1m) by (level)'
```

Or watch them arrive:

```bash
./bin/logctl tail '{service="demo", level>="warn"} |= "hello"'
```

`tail` takes a selector and line filters; parser stages and aggregations need `query`.
A client that falls behind loses records rather than slowing ingest, and is told how
many it missed.

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
grammar is the package doc of `internal/query`; the reasoning is in
[`ADR-0004`](docs/decisions/ADR-0004-query-compiler.md).

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

## Architecture

```
agents ──gRPC stream──> collectors ──> NATS JetStream ──> writer pool ──> TimescaleDB
                            │                                                   ▲
                            ├── memberlist gossip + consistent hash ring         │
                            ├── tail: NATS core `tail.>` ──> WebSocket /v1/tail  │
                            └── query: DSL ──> parameterized SQL ────────────────┘
```

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
| 3000 | Grafana | anonymous admin, dashboards provisioned from `deploy/grafana/` |
| 9091 | Prometheus | scrapes every collector and agent through Compose DNS |
| 16686 | Jaeger | traces; collectors and the agent export OTLP to it on 4317 |

The admin listener is separate from the public one, and configuration validation
refuses to start if they share an address, because pprof is unauthenticated and
exposes heap contents.

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
`LOGAGG_TRACING_ENDPOINT` and `LOGAGG_TRACING_SAMPLE_RATIO` do what they say. The dev
agent also follows the collector's own container, so `logagg` ingests its own logs.
[ADR-0007](docs/decisions/ADR-0007-observability.md) has the reasoning.

## Cluster

Collectors find each other with `hashicorp/memberlist` gossip and build a consistent
hash ring over their names, 128 virtual nodes each. Every collector writes through
JetStream, so the ring divides reads, not data: whichever collector takes a query
resolves the matching streams, splits the ids by owner, ships each owner the query
text and its share of the ids to compile and scan itself, and merges the rows. A peer that cannot be reached becomes a
`warnings` entry in the response instead of a failed query.

```bash
make dev-scale N=5                       # five collectors behind the proxy
curl -s -H "Authorization: Bearer dev-token" http://127.0.0.1:8080/v1/cluster | jq
make chaos                               # kill two mid-ingest, assert zero gaps
```

`GET /v1/cluster` shows the members, what each advertised, and its share of the key
space; `?arcs=1` adds every owned range. The chaos test runs the agent through the
proxy, kills two collectors outright while lines are in flight, then asserts every
numbered line landed with no gaps, that the ring settled on the survivors, and that a
fanned-out count agrees with the database. It waits out the queue's `ack_wait`,
since a killed collector's unacknowledged deliveries are redelivered only after it
expires. The reasoning is in [`ADR-0005`](docs/decisions/ADR-0005-cluster-layer.md).

`make dev` publishes fixed host ports for its single collector. `make dev-scale N=5`
puts an nginx proxy on the same 8080 and 9095 in front of N collectors, resolving
them through Compose's DNS on every connection, so the host addresses never change
and a killed replica drops out of rotation within seconds. Only the admin port stays
per replica, on 9190-9199; `make dev-ps` shows which is which.

## Configuration

Every setting has a default that makes the compose stack work unconfigured. Override
with `LOGAGG_`-prefixed environment variables, for example `LOGAGG_LOG_LEVEL=debug`,
`LOGAGG_HTTP_ADDR=:9000` or `LOGAGG_WRITER_BATCH_SIZE=10000`.
[`internal/config/config.go`](internal/config/config.go) has the full list. Invalid
values are reported all at once at startup rather than one per restart.

Two settings are checked against each other, since getting them wrong is silent.
`LOGAGG_INGEST_MAX_RECV_BYTES` must not exceed the broker's `max_payload`, or a batch
above it would be accepted, validated, then refused as unsendable. `LOGAGG_QUEUE_ACK_WAIT`
must exceed the writer's worst case, or a batch still being written gets redelivered.
The collector verifies the first against the live broker at startup and the second in
configuration validation.

### mTLS on the ingest port

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
service; ingest is one trust domain. ADR-0003 §7 explains why that is deferred rather
than half-implemented.

These certificates are for local development only. A CA whose private key sits in your
working tree signs them, they last a year, and nothing can revoke them. A real
deployment gets certificates from a CA with rotation and revocation, and keys that were
never on a laptop. `certs/` and `*.pem` are gitignored.

## Development

```bash
make test              # unit tests, no Docker needed
make test-race         # unit tests under the race detector
make test-integration  # integration tests against a real TimescaleDB
make lint              # golangci-lint, including the integration-tagged tests
make cover             # coverage report
make ci                # everything CI enforces
make fuzz              # fuzz the query lexer, parser and planner (FUZZ_TIME=60s)
make proto             # regenerate protobuf code (pinned buf + plugins)
make migrate           # apply migrations to DB_DSN
make migrate-status    # print the applied schema version
make lint-arch         # architectural invariants (ast-grep)
make certs             # development mTLS material (gitignored)
```

Go 1.27, golangci-lint v2. `make proto` needs no protoc: `buf` and the plugins are Go
modules fetched at pinned versions, and the generated code is committed so a clean
clone builds with only a Go toolchain.

Integration tests start their own TimescaleDB container via testcontainers-go. For a
faster inner loop, point them at a running `make dev` stack; each test still gets its
own freshly migrated database:

```bash
LOGAGG_TEST_DB_DSN='postgres://logagg:logagg@127.0.0.1:5432/logagg?sslmode=disable' \
  LOGAGG_TEST_RECORDS=50000 make test-integration
```

`LOGAGG_TEST_RECORDS` scales the throughput test down from its default of one million.
`LOGAGG_TEST_NATS_URL` does for the broker what `LOGAGG_TEST_DB_DSN` does for the
database; each test still isolates itself with its own JetStream stream, durable
consumer and subject prefix.

`make lint-arch` checks invariants golangci-lint cannot express, using
[ast-grep](https://ast-grep.github.io) rules in [`.ast-grep/rules/`](.ast-grep/rules):
no unbuffered channel may carry data, since every queue in the pipeline is bounded from
configuration and instrumented; no SQL may be concatenated or formatted at a query
call; and no literal `$n` may appear in the query compiler, where placeholders come
from one function that also records their values.

## Data model

Label sets are deduplicated into a `streams` dimension table keyed by a 64-bit hash of
their canonical encoding, so every node derives the same `stream_id` with no
coordination. Log rows are narrow and reference it. `logs` is a TimescaleDB hypertable
with 1 hour chunks, columnar compression segmented by stream, 30 day retention, and
per-minute and per-hour count aggregates that the query planner reads when an
aggregation can be answered from them exactly.

[`ADR-0003`](docs/decisions/ADR-0003-ingest-path.md) covers the ingest path: why the
ack comes after the JetStream publish, why a full buffer sheds instead of waiting, and
why a corrupt message is terminated rather than retried.

The write path is `COPY` into a session-local staging table followed by
`INSERT ... ON CONFLICT DO NOTHING` against a unique `(stream_id, seq, time)` index,
which makes a redelivered batch a no-op instead of a duplicate or an error.
[`ADR-0002`](docs/decisions/ADR-0002-schema-and-write-path.md) has the full reasoning,
including the TimescaleDB behaviours that shaped it.

## Non-goals

This is not a Loki or Elasticsearch replacement. No multi-tenancy, no RBAC beyond a
static API token, no cross-region replication, no plugin system, no TimescaleDB
multi-node. [`docs/decisions/`](docs/decisions/) gives the rationale for each.

## License

Copyright (c) 2026 James Polk. Licensed under the
[GNU Affero General Public License v3.0](LICENSE).

AGPL rather than MIT because this is network server software: anyone who runs a
modified version as a service has to offer their users the corresponding source.
