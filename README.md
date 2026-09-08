# go-log-aggregator

Concurrent, distributed log aggregator written in Go, backed by TimescaleDB.

Agents stream logs over gRPC to a cluster of collectors. Collectors buffer through NATS
JetStream, batch-write into TimescaleDB hypertables, and serve queries written in a
purpose-built log query language. Collectors find each other by gossip and share work
over a consistent hash ring, so nodes can join and leave while ingest continues.

**Delivery semantics: at-least-once end to end, deduplicated at the storage layer.**
Not exactly-once.

> **Status: phase 1 of 9 complete.** Foundations (configuration, logging, metrics,
> health probes, graceful shutdown, container image, dev stack) plus the data model,
> schema and write path: protobuf wire format, TimescaleDB hypertable with compression
> and continuous aggregates, and a batching writer pool that deduplicates redelivered
> records. The gRPC ingest service, agent, query compiler and cluster layer land in
> phases 2–5. The full plan is in [`.aidocs/ROADMAP.md`](.aidocs/ROADMAP.md).

## Quickstart

Requires Docker. Go is only needed to run tests and linters locally.

```bash
make dev          # build and start the stack, wait until healthy
make dev-logs     # follow logs
make dev-scale N=3  # run 3 collectors (host ports move to a range; see make dev-ps)
make dev-down     # stop, keeping data
make dev-nuke     # stop and delete volumes
```

Then:

```bash
curl -s http://127.0.0.1:8080/healthz          # liveness
curl -s http://127.0.0.1:8080/readyz           # readiness, per-dependency detail
curl -s http://127.0.0.1:9090/metrics | head   # Prometheus metrics
```

`make help` lists every target.

Push some records through it:

```bash
make build
echo "hello from logctl" | ./bin/logctl send -addr 127.0.0.1:9095 -service demo
./bin/loadgen -addr 127.0.0.1:9095 -records 200000    # synthetic load
```

`loadgen` prints accepted/rejected counts and exits non-zero if anything was
rejected, so it works as a check and not only as a demo. Both are deliberately
minimal this phase; the configurable-rate load generator arrives in phase 8.

## Architecture

```
agents ──gRPC stream──> collectors ──> NATS JetStream ──> writer pool ──> TimescaleDB
                            │                                                 ▲
                            ├── memberlist gossip + consistent hash ring       │
                            └── query: DSL ──> parameterized SQL ──────────────┘
```

Ports, all bound to loopback in development:

| Port | Listener | Notes |
|---|---|---|
| 8080 | Public HTTP API | health now; query and live tail in phases 4 and 6 |
| 9090 | Admin | Prometheus metrics and pprof. **Never expose this.** |
| 9095 | gRPC ingest | phase 2 |
| 9096 | gRPC peer | query fan-out, phase 5 |
| 7946 | memberlist gossip | phase 5 |
| 5432 | TimescaleDB | development credentials only |
| 4222 | NATS | 8222 serves its monitoring endpoint |

The admin listener is deliberately separate from the public one, and configuration
validation refuses to start if they share an address: pprof is unauthenticated and
exposes heap contents.

`make dev` publishes fixed host ports, so a single collector is always on 8080.
`make dev-scale` swaps in a port range instead, because fixed ports cannot be shared
between replicas — Compose assigns from that range in arbitrary order, so use
`make dev-ps` to find which replica is where. Phase 5 puts a reverse proxy on a stable
port in front of the cluster.

## Configuration

Every setting has a default that makes the compose stack work unconfigured. Override
with `LOGAGG_`-prefixed environment variables — for example `LOGAGG_LOG_LEVEL=debug`,
`LOGAGG_HTTP_ADDR=:9000`, `LOGAGG_INGEST_MAX_RECV_BYTES=8MB`. See
[`internal/config/config.go`](internal/config/config.go) for the full list; invalid
values are reported all at once at startup rather than one per restart.

### mTLS on the ingest port

Ingest speaks plaintext when no TLS files are configured, which is what makes
`make dev` work with no setup. Enabling it requires all three of a server
certificate, its key, and a client CA — configuring the first two without the third
is refused at startup rather than served as one-way TLS, which would encrypt the
connection while letting anyone who can reach the port write logs into the cluster.

```bash
make certs         # writes certs/ (gitignored): ca, server, client
make certs-verify  # show what those certificates actually claim

LOGAGG_INGEST_TLS_CERT_FILE=certs/server.pem \
LOGAGG_INGEST_TLS_KEY_FILE=certs/server-key.pem \
LOGAGG_INGEST_TLS_CLIENT_CA_FILE=certs/ca.pem \
  go run ./cmd/collector
```

**These certificates are for local development only.** They are self-signed by a CA
whose private key sits in your working tree, they last a year, and nothing can revoke
them. A real deployment gets certificates from a CA with a rotation and revocation
story, and keys that were never on a laptop. `certs/` and `*.pem` are gitignored:
never commit key material, self-signed included, because it teaches the habit.

## Development

```bash
make test              # unit tests, no Docker needed
make test-race         # unit tests under the race detector
make test-integration  # integration tests against a real TimescaleDB
make lint              # golangci-lint
make cover             # coverage report
make ci                # everything CI enforces
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
faster inner loop, point them at an already-running `make dev` stack — each test still
gets its own freshly migrated database:

```bash
LOGAGG_TEST_DB_DSN='postgres://logagg:logagg@127.0.0.1:5432/logagg?sslmode=disable' \
  LOGAGG_TEST_RECORDS=50000 make test-integration
```

`LOGAGG_TEST_RECORDS` scales the throughput test down from its default of one million.
`LOGAGG_TEST_NATS_URL` does for the broker what `LOGAGG_TEST_DB_DSN` does for the
database; each test still isolates itself with its own JetStream stream, durable
consumer and subject prefix.

`make lint-arch` checks architectural invariants that golangci-lint cannot express,
using [ast-grep](https://ast-grep.github.io) rules in
[`.ast-grep/rules/`](.ast-grep/rules). Currently one: no unbuffered channel may carry
data, because every queue in this pipeline is bounded from configuration and
instrumented with a depth gauge and a wait histogram. Signal channels
(`chan struct{}`) are exempt.

## Data model

Label sets are deduplicated into a `streams` dimension table keyed by a 64-bit hash of
their canonical encoding, so every node derives the same `stream_id` with no
coordination. Log rows are narrow and reference it. `logs` is a TimescaleDB hypertable
with 1 hour chunks, columnar compression segmented by stream, 30 day retention, and
per-minute and per-hour count aggregates that the query planner will choose between in
phase 4.

The write path is `COPY` into a session-local staging table followed by
`INSERT ... ON CONFLICT DO NOTHING` against a unique `(stream_id, seq, time)` index.
That is what makes at-least-once delivery safe: a redelivered batch is a no-op instead
of a duplicate or an error. The full reasoning, including the TimescaleDB behaviours
that shaped it, is in
[`ADR-0002`](docs/decisions/ADR-0002-schema-and-write-path.md).

## Non-goals

Not a Loki or Elasticsearch replacement. No multi-tenancy, no RBAC beyond a static API
token, no cross-region replication, no plugin system, no TimescaleDB multi-node.
Rationale for each in [`docs/decisions/`](docs/decisions/).

## License

Copyright (c) 2026 James Polk. Licensed under the
[GNU Affero General Public License v3.0](LICENSE).

AGPL rather than MIT because this is network server software: anyone who runs a
modified version as a service has to offer their users the corresponding source.
