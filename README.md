# go-log-aggregator

Concurrent, distributed log aggregator written in Go, backed by TimescaleDB.

Agents stream logs over gRPC to a cluster of collectors. Collectors buffer through NATS
JetStream, batch-write into TimescaleDB hypertables, and serve queries written in a
purpose-built log query language. Collectors find each other by gossip and share work
over a consistent hash ring, so nodes can join and leave while ingest continues.

**Delivery semantics: at-least-once end to end, deduplicated at the storage layer.**
Not exactly-once.

> **Status: phase 0 of 9 complete.** Configuration, logging, metrics, health probes,
> graceful shutdown, the container image, and the development stack are in place.
> Ingest, storage, the query compiler, and the cluster layer land in phases 1–5. The
> full plan is in [`.aidocs/ROADMAP.md`](.aidocs/ROADMAP.md).

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

## Development

```bash
make test        # unit tests
make test-race   # unit tests under the race detector
make lint        # golangci-lint
make cover       # coverage report
make ci          # everything CI enforces
```

Go 1.27, golangci-lint v2.

## Non-goals

Not a Loki or Elasticsearch replacement. No multi-tenancy, no RBAC beyond a static API
token, no cross-region replication, no plugin system, no TimescaleDB multi-node.
Rationale for each in [`.aidocs/decisions/`](.aidocs/decisions/).

## License

Copyright (c) 2026 James Polk. Licensed under the
[GNU Affero General Public License v3.0](LICENSE).

AGPL rather than MIT because this is network server software: anyone who runs a
modified version as a service has to offer their users the corresponding source.
