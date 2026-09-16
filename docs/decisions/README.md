# Architecture decision records

One record per decision that shaped the code, each with the alternatives that were
rejected and why. Records are numbered in the order they were written, not in the
order the decisions were made; ADR-0009 and ADR-0010 were backfilled in phase 9 from
code comments and the project plan. A later record that changes an earlier one adds
an *Amended* note to the earlier one rather than rewriting it, so the history stays
readable.

| ADR | Decision | Phase |
|---|---|---|
| [0001](ADR-0001-architecture-baseline.md) | Architecture baseline: gRPC to JetStream to TimescaleDB, gossip plus a hash ring, a query compiler, Compose everywhere | 0 |
| [0002](ADR-0002-schema-and-write-path.md) | Schema and write path: streams dimension table, dedup key, COPY and staging, policies, migrations | 1 |
| [0003](ADR-0003-ingest-path.md) | Ingest path: ack after publish, shed instead of block, terminate corrupt messages, mTLS trust model, shutdown order | 2 |
| [0009](ADR-0009-agent.md) | The agent: ack-only checkpoints, byte-offset sequence numbers, rotation by identity, spool as a buffer not a WAL | 3 |
| [0004](ADR-0004-query-compiler.md) | Query compiler: two statements not a join, placeholders for every literal, reserved `level`, continuous aggregate selection | 4 |
| [0005](ADR-0005-cluster-layer.md) | Cluster layer: memberlist, ring from names, ship the query not the SQL, degrade with warnings, a proxy for the scaled stack | 5 |
| [0006](ADR-0006-live-tail.md) | Live tail: NATS core fan-out, in-memory evaluator, slow clients lose records | 6 |
| [0007](ADR-0007-observability.md) | Observability: OpenTelemetry over OTLP, trace context in the batch, dashboards as code | 7 |
| [0008](ADR-0008-benchmark-driven-defaults.md) | Benchmark-driven defaults: direct COPY, `work_mem`, WAL sizing, dropped index, `ILIKE` stays | 8 |
| [0010](ADR-0010-license.md) | AGPL-3.0 over MIT | 9 |
| [0011](ADR-0011-terraform-module.md) | Terraform module: researched, and what was decided | 9 |

Several records cite "the roadmap" or "the project plan". That is the author's
working plan, kept outside the repository; everything a reader needs from it has
been copied into the record that cites it or into the README.
