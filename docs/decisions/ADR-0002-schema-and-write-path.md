# ADR-0002: Schema and write path

- **Status:** Accepted
- **Date:** 2026-09-04
- **Supersedes:** none
- **Refines:** ADR-0001 (decision 1: ingest and delivery semantics)

## Context

Phase 1 fixes the data model and the code that writes to it. This is the least
reversible part of the project: the schema is what every later phase queries, the
stream fingerprint is persisted in every row, and the level enum's integers are stored
on disk. Getting it wrong is expensive in a way that getting the query planner wrong is
not.

Two constraints from ADR-0001 drive most of what follows:

- Delivery is **at-least-once**. A crash between the TimescaleDB write and the
  JetStream ack causes redelivery, so the storage layer has to absorb duplicates
  rather than reject them.
- Horizontal distribution lives in the Go layer, so several collectors write to one
  TimescaleDB concurrently. Every write-path decision has to hold under concurrency,
  not just in a single-writer benchmark.

Everything below was verified against the pinned image
(`timescale/timescaledb` 2.22.1, PostgreSQL 17.6) rather than written from memory, and
the integration suite in `test/integration` re-verifies it on every run.

## Decision

### 1. Labels live in a dimension table, keyed by a content hash

`streams` holds one row per distinct label set; `logs` carries a `stream_id BIGINT`.
`service`, `host` and `env` are promoted to columns because every query filters on
them; everything else is JSONB.

The key is `xxhash64` of a **canonical, length-prefixed** encoding of the label set,
reinterpreted as `int64` because PostgreSQL has no unsigned 64-bit type.

- **Content hash rather than a sequence.** Every collector must derive the same ID for
  the same labels with no coordination. A sequence would need a round trip per new
  stream and a shared allocator.
- **Length-prefixed rather than delimited.** With any delimiter, `{service: "a",
  host: "b|c"}` and `{service: "a|b", host: "c"}` encode identically and silently merge
  into one stream. No byte is safe as a delimiter because label values are arbitrary
  strings. `FuzzLabelSetCanonicalIsDecodable` proves injectivity by decoding the
  encoding back.
- **Sorted extra labels**, so Go's randomized map iteration cannot change an ID.

Rejected: a JSONB label blob on every log row. It repeats the same few hundred bytes
across millions of rows and forces every selector to scan the hot table instead of the
small dimension table. The `stream_id = ANY($1)` shape this enables is the read side's
whole performance story (§4 of the roadmap).

### 2. Dedup key is `(stream_id, seq, time)`, and the write path is COPY-then-INSERT

TimescaleDB requires the partitioning column in every unique index on a hypertable, so
the dedup key includes `time`. The practical consequence, stated plainly: dedup only
suppresses a record replayed with an *identical* timestamp. That is exactly the
redelivery case, because the agent assigns the timestamp once and spools it.

`COPY` is roughly an order of magnitude faster than multi-row `INSERT`, but it has no
`ON CONFLICT` clause, so copying straight into `logs` would abort the whole batch on
the first redelivered record. The writer therefore:

1. upserts the batch's streams and **commits** — its own transaction, for the reason in
   §3,
2. `COPY`s into a session-local `TEMP TABLE ... ON COMMIT DELETE ROWS`,
3. `INSERT ... SELECT ... ORDER BY stream_id, seq, time ... ON CONFLICT DO NOTHING`,
4. commits.

The temp table is created with `IF NOT EXISTS`, so it costs one catalog write per pooled
connection rather than one per batch, and it is not WAL-logged.

Rejected: plain `CopyFrom` into `logs` with duplicates tolerated (no unique index).
It is faster, but it moves deduplication to read time forever, and every query would
have to be written defensively. Phase 8 benchmarks the two paths and publishes the
delta; this ADR only fixes the correctness default.

The gap between rows copied and rows inserted is exported as
`logagg_storage_rows_deduplicated_total`, which makes the real-world redelivery rate a
number rather than a guess.

### 3. The stream upsert is its own transaction

The least obvious decision here, and the one that took two attempts to get right.

The integration suite — not reasoning — surfaced a reproducible deadlock
(`SQLSTATE 40P01`) under concurrent writers, severe enough to lose roughly one batch in
four and to fail on CI what passed locally. Two distinct causes, found in that order:

**First, lock order within the upsert.** `ON CONFLICT DO UPDATE` locks one row per
conflicting stream in arrival order, and the arrival order came from iterating a Go map,
which is deliberately randomized. Two writers sharing some streams took those locks in
opposite orders. Fixed by imposing a total order: streams sorted by `stream_id` before
the upsert, and the staging insert `ORDER BY stream_id, seq, time` (which also improves
index locality, since consecutive rows land on the same B-tree pages).

**That was necessary but not sufficient,** and the give-away was a deadlock reported for
a batch containing *one* stream, where no sort can matter. The real cycle was between
two different lock types held by one transaction:

- `ON CONFLICT DO UPDATE` takes an **exclusive** row lock on the stream row and holds it
  until commit — which, in a shared transaction, means for the whole duration of a
  5000-row `COPY`.
- The insert into `logs` creates hypertable chunks, and TimescaleDB serializes chunk
  creation on its own catalog lock.

Two writers therefore acquire `{stream row, chunk catalog}` in whichever order they
happen to reach them, and PostgreSQL breaks the cycle by killing one.

The fix is to commit the stream upsert before opening the records transaction. The
exclusive row lock is then held for microseconds and released before any chunk lock is
taken, and the records transaction needs only `KEY SHARE` on the stream row for its
foreign key check — a shared lock that every concurrent writer can hold at once.

The cost is that a crash between the two commits leaves a stream row with no records.
That is harmless: `streams` is a dimension table, and a row there asserts "this label set
was seen", which was true. The reverse order — records first — would violate the foreign
key and lose the batch, which is why it is this way round and not the other.

Retries with exponential backoff and full jitter remain, since deadlock is a retryable
class, but they were never the fix: at three attempts they still lost batches. A retry
budget is not a substitute for a lock order.

`TestConcurrentWritersSpanningChunksDoNotDeadlock` is the regression test. It pins the
exact interleaving — several writers, one shared stream, a refresh interval short enough
that every batch upserts it, and records spread over 48 hours so nearly every batch also
creates chunks — and it fails within seconds against the shared-transaction version.

### 4. Streams are cached in a bounded LRU, and `last_seen` is refreshed lazily

Without a cache every batch would upsert every stream it touches: a wasted round trip
for a thousand-record single-stream batch, and a hot row that every node contends on.
The cache stores *when this process last wrote the row*, not the labels, because the
only question it answers is "must I write this again".

- **Bounded**, because label cardinality is controlled by whoever is shipping logs.
- **Shared across workers**, because a per-worker cache multiplies the miss rate by the
  worker count.
- **Populated only after commit.** Caching on send would let a rolled-back batch leave
  the cache claiming a stream exists, and the next batch's log rows would fail their
  foreign key.
- `last_seen` is rewritten at most once per `StreamRefreshInterval` (default 5 minutes)
  and guarded by `GREATEST(...)`, so a late write can never move it backwards. This is
  an explicit freshness-for-writes trade, and `/v1/labels` is its only consumer.

### 5. Invalid records are dropped at Submit, not at flush

One oversized message would fail the `COPY` for its whole batch and then fail every
retry of that batch: a single bad record blocking a stream indefinitely, which is the
classic poison-message failure. `Writer.Submit` validates each record, compacts the bad
ones out, counts them under `logagg_records_dropped_total{reason="invalid_record"}`,
and returns how many were accepted — which is exactly what phase 2 puts in
`Ack.accepted`.

Validation limits (message length, label and field counts and sizes, timestamp window)
live in `internal/model` rather than in the ingest handler, so the gRPC service, the
loadgen and any future receiver enforce the same rules.

Timestamps outside `[now - 7d, now + 15m]` are **rejected, not clamped**. Silently
rewriting a timestamp makes the data lie, and a clock that is years off is a
configuration bug the operator needs to see. The window also bounds how many chunks
one misconfigured agent can create.

### 6. Backpressure, not load shedding, at the writer

`Submit` blocks when the bounded queue is full. The queue is the buffer and JetStream
depth is what an operator alerts on; shedding belongs at ingest (phase 2), where there
is still an unacknowledged client to tell to slow down. Nothing here uses an unbounded
channel.

Acks fire after the commit, never on receipt. That is the entire reason at-least-once
works end to end: a crash before the write lands means no ack, which means redelivery,
which the dedup key absorbs.

### 7. Migrations are embedded and run at startup

`golang-migrate` over an `embed.FS`, applied by the collector itself at boot
(`LOGAGG_DB_MIGRATE_ON_START`, default on). golang-migrate takes a PostgreSQL advisory
lock, so N replicas booting together are safe: one migrates and the rest observe "no
change". The same binary exposes `-migrate`, `-migrate-down` and `-migrate-status` for
`make migrate`, so the distroless image needs no second tool.

Each migration file is sent as one multi-statement query, which PostgreSQL wraps in an
implicit transaction — so a failure rolls the whole file back. `MultiStatementEnabled`
is deliberately left off to keep that property.

Down migrations exist and are exercised (`up → down → up`) by the integration suite.
Not because a production rollback is likely, but because a migration that is never
reversed is a migration whose ordering constraints are untested — and both of ours have
real ones (decompress every chunk before disabling compression; drop the hierarchical
aggregate before its parent).

## TimescaleDB specifics, verified against 2.22.1

These are the details phase 1 exists to hit early rather than in phase 8.

| Behaviour | Consequence |
|---|---|
| `CREATE MATERIALIZED VIEW ... WITH DATA` cannot run inside a transaction block | Both continuous aggregates are created `WITH NO DATA`; the refresh policy backfills. Required anyway given migrations run as one implicit transaction. |
| Continuous aggregates default to `materialized_only = true` since 2.13 | Both views are switched to `false`. Otherwise a query against `logs_rate_1m` silently omits everything since the last refresh — a minutes-wide hole at exactly the end of the range a log query asks about. Costs query time, buys a correct answer, and matters because the phase 4 planner may substitute the view for the raw table. |
| Unique indexes on a hypertable must include the partitioning column | Dedup key is `(stream_id, seq, time)`. |
| `alter_job()` on the refresh policy of a *hierarchical* continuous aggregate fails with "multiple refresh policies are not supported for hierarchical continuous aggregates" | `logs_rate_1h` is built from `logs_rate_1m`, and both have refresh policies — which `add_continuous_aggregate_policy` accepts but `alter_job` then rejects. Refreshing works; only in-place retuning does not. To change the schedule, remove and re-add the policy, which is what a migration does anyway. Accepted: the hierarchy makes the hourly rollup read thousands of pre-aggregated rows instead of millions of raw ones, which is worth more than in-place tuning. |
| Timescale's per-database job scheduler runs the same policies the tests invoke by hand | Concurrent runs of one job both fail ("chunk is already compressed"), and the scheduler's advisory locks deadlock a schema rollback. Integration tests stop the scheduler for their database first. What remains under test is the policy procedures and their configuration; whether Timescale's scheduler fires on time is Timescale's concern. |
| Timescale advises against space partitioning a single-node hypertable | One dimension (time, 1 hour chunks). `compress_segmentby = 'stream_id'` provides the per-stream locality instead. |

## Consequences

- The level enum's integers and the stream fingerprint algorithm are now **on-disk
  format**. They may be appended to, never renumbered or changed. Tests assert both.
- Extra labels may not be named `service`, `host` or `env`, and must match
  `[a-zA-Z_][a-zA-Z0-9_]*` — the same shape the phase 4 grammar uses for labels, so
  every stored label is addressable by a query.
- `config.Writer.Workers` must not exceed `config.DB.MaxConns`; configuration
  validation enforces it. A worker holds a pooled connection for its whole
  transaction, so more workers than connections just moves the queue somewhere less
  visible.
- Generated protobuf code is committed. A clean clone builds with only a Go toolchain;
  `make proto` regenerates it with pinned `buf` and plugin versions, and
  `make proto-check` fails if the committed output is stale.
- `buf`'s `RPC_REQUEST_STANDARD_NAME` and `RPC_RESPONSE_STANDARD_NAME` lint rules are
  disabled. `LogBatch` and `Ack` are domain types used outside the RPC (the agent
  spools batches and correlates acks by `batch_id`); wrapping them in single-field
  envelopes would name nothing.
- Integration tests need Docker. They are behind the `integration` build tag so
  `go test ./...` stays fast, and they accept `LOGAGG_TEST_DB_DSN` to reuse a running
  `make dev` stack instead of starting a container.
- Measured on the phase 1 exit criterion (laptop Docker, single node, no tuning):
  **1,000,000 records in 18.8s, ~53k records/sec**, batch size 5000, 4 workers, 20
  streams. Recorded as a starting point, not as a benchmark; phase 8 publishes real
  numbers with a hardware baseline.
