# ADR-0001: Architecture baseline

- **Status:** Accepted
- **Date:** 2026-09-04
- **Supersedes:** none

## Context

A concurrent, distributed log aggregator in Go on TimescaleDB, built as a portfolio
project. The audience is a reviewer with limited time, so the design has to make its
engineering depth legible quickly while still being something one person can finish.
Four decisions shape everything downstream: how logs arrive, what "distributed"
means, what the read side looks like, and how far the ops story goes.

## Decision

### 1. Ingest: gRPC bidirectional streaming → NATS JetStream → writer pool → TimescaleDB

Agents stream batches over gRPC. Collectors validate, fingerprint the label set, and
publish to JetStream. A separate writer pool consumes, batches, and writes with
`pgx.CopyFrom`.

Rejected:

- **HTTP + syslog + OTLP multi-protocol front door.** Broader compatibility, but the
  work becomes parsing breadth rather than concurrency depth.
- **Kafka-first, service as consumer only.** Kafka would then be doing the
  distributed work the project claims to demonstrate, and it is heavy to run locally.
- **gRPC with a hand-rolled WAL, no broker.** Fewest moving parts, and writing a WAL
  is impressive — but it is the most likely component to be subtly wrong and the
  hardest for a reviewer to verify quickly.

Consequence: **at-least-once delivery, deduplicated at the storage layer** via a
unique index on `(stream_id, seq, time)`. Not exactly-once. This is stated plainly in
the README rather than glossed over.

### 2. Distribution: memberlist gossip + consistent hash ring, no consensus

Collector nodes discover each other with SWIM gossip (`hashicorp/memberlist`) and use
a consistent hash ring with virtual nodes to own log streams. Ownership routes
**query fan-out**, not writes — writes go through JetStream and any node can write.

*Amended 2026-09-16:* this originally said ownership also routed tail subscriptions.
ADR-0006 chose NATS core fan-out for tail instead, evaluated on whichever node the
client connected to; the ring is not consulted.

Rejected:

- **Raft (`hashicorp/raft`).** Highest ceiling, but consensus plus rebalance plus the
  tests to trust it would consume most of the project budget, and TimescaleDB already
  provides the durable state a Raft log would be replicating.
- **Stateless replicas behind a load balancer.** Most production-sane, but reduces
  "distributed" to "three copies of the same process", which undersells the title.

Also relevant: **TimescaleDB multi-node / distributed hypertables are deprecated**
(verify against the pinned version). Horizontal distribution therefore has to live in
the Go layer regardless — which happens to be the point.

### 3. Read side: custom query DSL + REST + WebSocket tail + Grafana

A LogQL-shaped language compiled by a hand-written lexer, recursive-descent parser,
AST, and planner that emits parameterized TimescaleDB SQL, reading a continuous
aggregate instead of the raw hypertable when it can prove the answer is identical
(the exactness rule is ADR-0004 §6, not a range heuristic). Grafana reads the same
hypertables for dashboards.

Rejected:

- **REST search API only.** Ships far faster and is genuinely useful, but every
  backend portfolio has a REST API; almost none have a query compiler. The compiler is
  also the most testable code in the repo (pure logic, fuzzable, golden files).
- **DSL with no Grafana.** Saves a container, loses the visual proof that makes a repo
  memorable in a 30-second skim.

Hard constraint recorded here because it is a security property, not a preference:
**no user input is ever concatenated into SQL.** Literals become `$n` placeholders;
only planner-chosen identifiers from a fixed allow-list are written into the query
text, and a test asserts the generated SQL contains no bytes from the input.

### 4. Ops: Docker Compose everywhere, with deep observability and published benchmarks

Everything runs in containers. `make dev` brings up TimescaleDB, NATS, collectors,
and later Grafana, Prometheus and Jaeger, with provisioned dashboards. Benchmarks are
committed with before/after numbers and rendered flame graphs; raw `.pprof` files are
not, since a profile is only readable against the binary that produced it.

Rejected:

- **Kubernetes manifests and a Helm chart.** Broadens the keyword list but is mostly
  YAML, and two deployment paths drift apart unless both are maintained. Deferred to
  stretch goals.
- **A public cloud demo.** Strong hook, but it costs money, needs secret management,
  and breaks unattended.

## Consequences

- One binary (`collector`) hosts ingest, query, and cluster membership. No
  microservice split: it would triple the ops surface and demonstrate nothing extra.
- Every bounded queue in the system needs a depth gauge and a wait-time histogram.
  Unbounded channels are treated as a defect.
- The admin listener (metrics + pprof) is a separate port from the public API, and
  configuration validation refuses to let them be the same. pprof is unauthenticated
  and exposes heap contents.
- Base images are pinned by digest so builds are reproducible and a re-pointed
  upstream tag cannot change what ships.

## Deferred

Considered, wanted, and left out so the nine phases would finish. Each is a
candidate follow-on, none is promised:

- An OTLP logs receiver alongside gRPC ingest.
- Alerting rules evaluated over the continuous aggregates, with a webhook sink.
- Hinted handoff for tail subscriptions during a rebalance.
- Adaptive load shedding driven by observed write latency.
- An object-storage tier for aged-out chunks.
- A terminal UI for tail and query.
- Kubernetes manifests and a Helm chart (see §4).
- A Terraform module; researched and declined in ADR-0011, which holds the scoped
  design should the decision be revisited.
