# ADR-0008: Benchmark-driven defaults for the write path and storage

- **Status:** Accepted
- **Date:** 2026-09-15
- **Supersedes:** none
- **Refines:** ADR-0002 (schema and write path), §2 and §4 of the roadmap

## Context

Phases 1–7 chose defaults by reasoning. Phase 8 built a reproducible load
generator and harness (`cmd/loadgen`, `deploy/bench.sh`), measured, and changed
what the numbers said to change. This ADR records the decisions; the numbers,
methodology, hardware baseline and every run behind them live in
[`docs/benchmarks/`](../benchmarks/README.md), which is the evidence for each
section below and is not repeated here.

The measured environment is one laptop: Docker Desktop's Linux VM hosting
Postgres, JetStream and the collector on one virtualized disk. Absolute figures
are a floor; the decisions rest on ratios between runs and on failure modes that
appeared or disappeared, not on the absolute numbers.

## Decisions

### 1. The writer COPYs straight into the hypertable; staging is the fallback

ADR-0002 §2 chose COPY-into-staging then `INSERT … ON CONFLICT DO NOTHING` so a
redelivered batch is a no-op, and deferred the plain-COPY comparison to this
phase. Measured on the same build, three runs each alternating: direct COPY
sustained a median of 47k records/s against 36k for staging, with a median write
time per 5 000-row batch of 390 ms against 552 ms. Roughly 1.3×.

`LOGAGG_WRITER_COPY_MODE` therefore defaults to `direct`. COPY has no `ON
CONFLICT`, so a replayed record aborts the COPY with `unique_violation`; the
writer catches exactly that code, counts it in
`logagg_storage_direct_copy_fallbacks_total`, and redoes the batch through the
staging path. Correctness under at-least-once delivery is unchanged — the
integration suite runs the full-replay and partial-replay tests in both modes —
and the cost moved to where it belongs: a replay pays for two passes, a normal
batch pays for one.

The staging insert's `ORDER BY stream_id, seq, time` was also the cluster-wide
lock order that keeps two writers from deadlocking on the dedup index
(ADR-0002 §3). COPY has no `ORDER BY`, so the writer now sorts each batch by that
key before either path runs; both get the same order and the index locality
that comes with it.

`staging` remains selectable for a deployment where redelivery is routine rather
than exceptional; in that regime the fallback counter will say so. The mode is
required, not defaulted, in `config.Writer`, so the shipped default lives in one
place.

Rejected: keeping staging as the default because it is "safer". Both modes give
the same rows; the fallback is the safety, and the counter makes its cost
visible.

### 2. The staging insert sets `work_mem` for its own transaction

`SET LOCAL work_mem = '64MB'` before the `INSERT … ORDER BY`. The image's 2 MB
`work_mem` holds about 7 000 rows of 200-byte messages; larger batches sorted on
disk, which is why a 20 000-row batch had cost 8× a 5 000-row one instead of 4×.
Measured: 25% off the default batch, 3× off the 20 000-row batch, and temp files
per run fell from hundreds to a handful. `LOCAL`, so it never leaks past the
transaction and never touches the server's setting.

Rejected: raising `work_mem` server-wide. It applies to every sort in every
query, and the query path did not ask for it.

### 3. Postgres: `max_wal_size = 4GB`, `checkpoint_timeout = 15min`, a 60 s stop grace

Passed as `command` flags to the `timescaledb` service in
`deploy/docker-compose.yml`, overriding the 1 GB that `timescaledb-tune` leaves.
The baseline wrote 5.7 GB of WAL for 1.2 GB of table because WAL-triggered
checkpoints never stopped and re-armed full-page writes on the same index pages;
after the change, 1.8 GB and one checkpoint per run. The larger effect was not on
the writer but on the broker: JetStream shares the disk, and the checkpoint
writeback was stalling its file store for up to 28 s, which surfaced as 5 s
publish timeouts and `ACK_CODE_INTERNAL` acks to agents. Those stopped.

The cost is crash-recovery time proportional to `max_wal_size`, and that is why
`stop_grace_period` is 60 s: Compose's 10 s default SIGKILLed Postgres
mid-checkpoint, and the next start replayed WAL long enough for the healthcheck to
give up. A deployment that is not on Compose should carry both settings across.

Rejected: `wal_compression = lz4`. With full-page images gone there was nothing
to compress; WAL stayed at 1.8 GB and the run was slower.

### 4. Batch size stays 5 000 and workers stay 4

Swept 1 000 to 20 000 rows and 4 against 8 workers. Below 5 000 the commit rate
(two transactions per batch) saturated the shared disk and produced publish
timeouts on every run; above it, once decision 2 removed the sort spill, the
per-row cost was flat. Eight workers made every batch 3.4× slower and produced
the most publish timeouts of any run: concurrent COPYs into one chunk contend on
the same index pages, and the extra commits are what stall the broker. Nothing
beat the defaults by more than the run-to-run noise, and the two knobs that
looked promising were measurably worse.

### 5. Storage: drop the default `time` index; keep 1-hour chunks, with a rule

Migration 0004 drops `logs_time_idx`, the `(time DESC)` index `create_hypertable`
adds by default. `logs_time_level (time DESC, level)` from 0001 serves every
time-ordered scan the default one did, so it was a duplicate ordering costing one
B-tree per chunk (20% of index bytes) and one index update per row. Measured at
about 8% off the mean write with every `ORDER BY time DESC` plan moving to the
surviving index.

Compression measured at 5.3× (1 112 MB to 209 MB for 3.05M rows; 382 to 72 bytes
per row), which is the number 0002 promised. The 1-hour chunk interval stays,
now with the sizing rule it was waiting for: keep the chunk being written to,
indexes included, near a quarter of `shared_buffers`, which at 382 bytes per row
and a 2 GB buffer pool is about 1.3M rows per chunk, or a steady 300 rows/s. A
deployment ingesting faster shortens the interval with
`set_chunk_time_interval`; no migration is needed.

Rejected: shrinking the interval to match the benchmark's 50k rows/s. That rate
is a stress test; sizing the schema to it would give a real deployment a chunk
every few seconds.

### 6. Substring search stays `ILIKE`; the trigram index is opt-in; `tsvector` is out

§2.4 asked for `ILIKE` against `pg_trgm` GIN against `tsvector` GIN, measured.
Three needles by frequency, two query shapes, 3.05M rows:

- `tsvector` matches words, not substrings. `seq=2999999` matched nothing, and a
  common word took 7× longer than a sequential scan. Wrong semantics for `|=`;
  out.
- The trigram index turned the one query it helps, a needle present in a
  handful of rows, from 1 478 ms to 31 ms (46×). The planner ignores it for
  anything common, correctly. Its cost while present: 36% of the table on disk, a
  two-minute build per 3M rows, and a **5.8× slower write path**. It also does
  not apply to compressed chunks, so it covers only the uncompressed window.
- `ILIKE` with no index keeps ingest at full speed and answers common searches
  through the time index in milliseconds.

The default stays `ILIKE`, unindexed. The query compiler is unchanged: an
`ILIKE` predicate uses a trigram index automatically when one exists, so the
opt-in is a documented `CREATE INDEX CONCURRENTLY … USING GIN (message
gin_trgm_ops)` for a deployment that wants request-ID lookups over the recent
window and accepts the write cost. Shipping that index in a migration was
rejected because it would make every deployment pay 6× on writes for a query
shape most never run.

## Consequences

- The delivery guarantee of ADR-0001 is unchanged; the dedup index is still what
  makes replay safe, it is just consulted lazily.
- Every default above is an environment variable or a Compose flag, so a
  deployment that measures differently can change it without a code change, and
  `docs/benchmarks/README.md` says how to measure.
- The harness is part of the tree. A future change to the write path is expected
  to come with a `docs/benchmarks/runs/` entry, in the same shape, on stated
  hardware.
