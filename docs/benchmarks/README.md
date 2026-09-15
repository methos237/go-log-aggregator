# Benchmarks

Measured numbers for the ingest path, the method that produced them, and the
optimization log: what was changed, what it was expected to do, and what it
measurably did. Everything a number depends on is committed next to it, so a
reader can check the claim rather than trust it.

Raw `.pprof` files are deliberately **not** committed. A profile is only
interpretable with the exact binary that produced it, and four profiles per run
across a dozen runs turns a clone into a download. The rendered flame graphs and
the `pprof -top` text are committed instead; if a raw profile ever needs to
survive, it is attached to the pull request.

## How a run works

One run is one invocation of `deploy/bench.sh` (`make bench-run NAME=...`), and
one run produces one directory under [`runs/`](runs/). The script fixes the
method so every run is comparable to every other:

1. **Fresh collector.** The collector container is recreated, so counters, heap
   and stream cache start from zero and the run's environment is exactly what
   `deploy/docker-compose.bench.yml` passes through. The observability overlay is
   not applied: trace export is work done for someone else.
2. **Clean table, warm process.** `logs` is truncated, then 50 000 records are
   sent unpaced and allowed to drain, so connection pools, staging tables and
   the first chunk exist before the clock starts.
3. **The run.** `loadgen` sends the stated shape. Throughput is what loadgen
   reports: accepted records over wall time, measured at the client.
4. **The drain.** The harness then waits until the row count in TimescaleDB has
   grown by the accepted count. That wait is how far behind the writer was when
   the client finished, which is the end-to-end figure the client cannot see.

Numbers and profiles come from **separate runs**. Profiling (a CPU sample plus
block and mutex sampling in the runtime) costs throughput, so a `PROFILE=1` run
is for reading flame graphs and a `PROFILE=0` run is for reading numbers.

### The baseline load shape

Unless a run says otherwise it sends **3 000 000 records** of **200 bytes** in
**batches of 500** across **64 streams** on **4 concurrent gRPC streams**,
unpaced, from the host to a single collector. Messages are drawn from a fixed
vocabulary with a Zipf word distribution, deterministic in the sequence number,
so two runs put the same bytes on the wire.

### What a run directory holds

| File | What it is |
|------|------------|
| `run.json` | Everything: hardware, commit, collector environment, the loadgen summary, end-to-end drain, and the writer's own counters (rows, retries, mean write time, mean batch size) |
| `loadgen.txt`, `loadgen.json` | The client's view: accepted/rejected, records per second, ack latency p50/p95/p99 |
| `metrics-before.txt`, `metrics-after.txt` | The collector's `/metrics`, filtered to `logagg_`, either side of the run |
| `cpu.svg`, `alloc.svg`, `heap.svg`, `block.svg`, `mutex.svg` | Flame graphs (profiled runs only). CPU covers the first 20 s of the run; the others are cumulative since the collector started |
| `*-top.txt` | `pprof -top` for the same profiles, for reading without a browser |

### Reading the two throughput numbers

- **Client records/s** (`loadgen.records_per_sec`) is how fast the collector
  *accepted* records. Acceptance means the batch was validated and published to
  JetStream. It is bounded by the ingest path and the broker.
- **Sustained records/s** (`end_to_end.sustained_records_per_sec`) is accepted
  records over client time *plus* drain. It is bounded by the writer and the
  database, and it is the number that matters when the client is faster than
  the writer, because the difference is queue growth.

Ack latency is send-to-ack at the client: ingest plus JetStream publish, not
the database write. On an unpaced run it mostly measures queueing in gRPC flow
control; use `RATE=` and `DURATION=` for a latency measurement.

## Hardware baseline

Every run records its own hardware in `run.json`. The numbers in this document
were measured on:

| | |
|---|---|
| Machine | Apple M2 Pro, 10 cores, 16 GiB |
| OS | macOS 26.6.2 |
| Docker | Docker Desktop 29.4.3, Linux VM with 10 CPUs and 7.65 GiB |
| Database | `timescale/timescaledb:2.22.1-pg17`, image defaults (`timescaledb-tune` at first boot), data on a Docker volume |
| Broker | `nats:2.12-alpine`, JetStream file storage on a Docker volume |
| Go | 1.27.1, `linux/arm64` binary in a distroless image |
| Client | `loadgen` on the host, over the published loopback port |

Two caveats that anyone comparing against these numbers should know. The
collector, the database and the broker share one VM, and the client shares the
host with them, so this measures the whole stack on one laptop rather than any
component in isolation. And Docker Desktop's disk is virtualized, so anything
fsync-bound (the database commit, JetStream's file store) is slower here than on
bare metal. Ratios between runs are the point; the absolute figures are a floor.

## Optimization log

Each entry states the change, the hypothesis, the runs that test it, and the
delta. Entries are appended in the order the work happened, including the ones
that did not pay off.

<!-- template
### N. Title

**Change.** What was different, in one or two sentences, with a link to the
commit or the config knob.

**Hypothesis.** What it was expected to do and why.

**Runs.** `runs/<before>` against `runs/<after>`.

| | before | after | delta |
|---|---|---|---|
| Client records/s | | | |
| Sustained records/s | | | |
| Drain (s) | | | |
| Ack p99 (ms) | | | |
| Mean write (ms) | | | |

**Result.** What the numbers say, and the decision taken because of them.
-->
