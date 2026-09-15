#!/usr/bin/env bash
# Phase 8 benchmark harness. One invocation is one run, and one run is one
# directory under docs/benchmarks/runs/<NAME>/ holding everything a reader needs
# to check the number: the loadgen summary, the collector's own metrics before and
# after, the hardware and the exact configuration, and (for PROFILE=1) rendered
# flame graphs. Raw .pprof files go to /tmp and stay out of the tree -- see
# docs/benchmarks/README.md for why.
#
# Methodology, fixed here so every run is comparable to every other:
#
#   1. The collector container is recreated for every run, so counters, the heap
#      and the stream cache all start from zero and the run's environment is
#      exactly what deploy/docker-compose.bench.yml passes through.
#   2. The logs hypertable is truncated, then a short unpaced warm-up runs and
#      drains, so connection pools, staging tables and the first chunk exist
#      before the clock starts.
#   3. loadgen runs with the given shape. Throughput is what loadgen reports:
#      accepted records over wall time, measured at the client.
#   4. The harness then waits until the row count in TimescaleDB has grown by the
#      accepted count. That wait is the "drain": how far behind the writer was when
#      the client finished, which is the end-to-end number the client cannot see.
#
# Numbers and profiles come from separate runs. Profiling (a 20s CPU sample plus
# block and mutex sampling in the runtime) costs throughput, so a PROFILE=1 run
# is for reading flame graphs and a PROFILE=0 run is for reading numbers.
set -euo pipefail

NAME=${NAME:?set NAME=<run name>, e.g. NAME=baseline}
# Load shape. The defaults are the baseline shape; every other run in
# docs/benchmarks/ states what it changed.
RECORDS=${RECORDS:-3000000}
BATCH=${BATCH:-500}
STREAMS=${STREAMS:-64}
SENDERS=${SENDERS:-4}
MESSAGE_SIZE=${MESSAGE_SIZE:-200}
RATE=${RATE:-0}
DURATION=${DURATION:-}
RAMP=${RAMP:-}
WARMUP_RECORDS=${WARMUP_RECORDS:-50000}
PROFILE=${PROFILE:-0}
# BUILD=0 reuses the collector image from the last build, so a sweep of config
# knobs can run while the working tree is being edited without the image changing
# under it. Anything that changes code must build.
BUILD=${BUILD:-1}
CPU_PROFILE_SECONDS=${CPU_PROFILE_SECONDS:-20}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-600}
OUT=${OUT:-docs/benchmarks/runs/$NAME}
RAW=${RAW:-/tmp/logagg-bench/$NAME}
ADMIN=${ADMIN:-http://127.0.0.1:9090}
INGEST=${INGEST:-127.0.0.1:9095}
# Pinned FlameGraph commit; the scripts are fetched once into bin/ (gitignored).
FLAMEGRAPH_COMMIT=${FLAMEGRAPH_COMMIT:-cd9ee4c4449775a2f867acf31c84b7fe4b132ad5}

COMPOSE="docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.bench.yml"

say() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

psql_q() {
	$COMPOSE exec -T timescaledb psql -U logagg -d logagg -At -c "$1"
}
row_count() { psql_q "SELECT count(*) FROM logs"; }

now() { perl -MTime::HiRes=time -e 'printf "%.2f", time'; }

# wait_for_rows blocks until logs holds at least $1 rows and echoes how long it
# took, in seconds to two decimals. Polled every 200ms, so the drain figure is
# good to about a quarter of a second.
wait_for_rows() {
	local want=$1 started have
	started=$(now)
	while :; do
		have=$(row_count)
		[ "$have" -ge "$want" ] && break
		if [ "$(echo "$(now) $started" | awk '{print ($1 - $2 > '"$DRAIN_TIMEOUT"')}')" = 1 ]; then
			fail "only $have of $want rows landed within ${DRAIN_TIMEOUT}s"
		fi
		# A dead collector never drains; say so now instead of at the timeout.
		[ "$(docker inspect -f '{{.State.Running}}' logagg-collector-1 2>/dev/null)" = true ] ||
			fail "collector stopped mid-run with $have of $want rows landed"
		sleep 0.2
	done
	echo "$(now) $started" | awk '{printf "%.2f", $1 - $2}'
}

# wait_for_settle waits until the row count stops moving, for the warm-up whose
# exact count does not matter.
wait_for_settle() {
	local prev=-1 cur
	while :; do
		cur=$(row_count)
		[ "$cur" = "$prev" ] && break
		prev=$cur
		sleep 2
	done
}

# hardware prints the baseline as JSON: what this run's numbers were measured on.
hardware() {
	local cpu cores mem os
	if command -v sysctl >/dev/null 2>&1 && sysctl -n machdep.cpu.brand_string >/dev/null 2>&1; then
		cpu=$(sysctl -n machdep.cpu.brand_string)
		cores=$(sysctl -n hw.ncpu)
		mem=$(sysctl -n hw.memsize)
		os="macOS $(sw_vers -productVersion)"
	else
		cpu=$(lscpu | awk -F: '/Model name/ {gsub(/^ +/, "", $2); print $2}')
		cores=$(nproc)
		mem=$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo)
		os=$(uname -sr)
	fi
	# The VM's memory state matters as much as its size: a swapping VM produces
	# numbers that measure the swap, and the first baseline attempt did exactly
	# that with a neighbouring project's containers still running.
	local vm_free vm_swap
	read -r vm_free vm_swap <<<"$(docker run --rm alpine sh -c \
		'free -m | awk "/^Mem:/ {f=\$7} /^Swap:/ {s=\$3} END {print f, s}"' 2>/dev/null || echo "null null")"
	jq -n \
		--arg cpu "$cpu" --argjson cores "$cores" --argjson mem "$mem" --arg os "$os" \
		--argjson vm_free "$vm_free" --argjson vm_swap "$vm_swap" \
		--argjson other_containers "$(docker ps --format '{{.Names}}' | grep -vc '^logagg-' || true)" \
		--arg go "$(go version | awk '{print $3}')" \
		--arg docker "$(docker version --format '{{.Server.Version}}')" \
		--argjson docker_cpus "$(docker info --format '{{.NCPU}}')" \
		--argjson docker_mem "$(docker info --format '{{.MemTotal}}')" \
		'{cpu: $cpu, cores: $cores, memory_bytes: $mem, os: $os, go: $go,
		  docker: $docker, docker_vm_cpus: $docker_cpus, docker_vm_memory_bytes: $docker_mem,
		  docker_vm_available_mb: $vm_free, docker_vm_swap_used_mb: $vm_swap,
		  other_containers_running: $other_containers}'
}

# metric_delta prints after-minus-before for a metric, summed over its label sets
# when it has any, or 0 when the collector never exported it.
metric_delta() {
	local name=$1 b a
	b=$(awk -v n="$name" '$1 == n || index($1, n "{") == 1 {s += $2} END {print s + 0}' "$OUT/metrics-before.txt")
	a=$(awk -v n="$name" '$1 == n || index($1, n "{") == 1 {s += $2} END {print s + 0}' "$OUT/metrics-after.txt")
	echo "$a $b" | awk '{printf "%.6g", $1 - $2}'
}

# flamegraph renders one pprof profile as an SVG flame graph and a text top list.
# $1 profile file, $2 output stem, $3 sample index name for the top list, $4 the
# 1-based column of that sample type in `pprof -raw` output, $5 the unit.
#
# The awk step exists because stackcollapse-go.pl only understands the two-column
# raw format of a CPU profile; heap, block and mutex profiles carry several sample
# types per stack, so the wanted column is copied into the first two positions.
flamegraph() {
	local in=$1 stem=$2 index=$3 col=$4 unit=$5
	go tool pprof -sample_index="$index" -top -nodecount=30 "$in" >"$stem-top.txt" 2>/dev/null || true
	go tool pprof -raw "$in" 2>/dev/null |
		awk -v col="$col" '/^ *[0-9]+( +[0-9]+)*: / {
			split($0, a, ":"); n = split(a[1], v, " "); print v[col], v[col] ":" a[2]; next
		} { print }' |
		perl bin/flamegraph/stackcollapse-go.pl 2>/dev/null |
		perl bin/flamegraph/flamegraph.pl --title "$NAME: $(basename "$stem")" --countname "$unit" --width 1400 >"$stem.svg" 2>/dev/null ||
		{ rm -f "$stem.svg"; echo "  (no $(basename "$stem") flame graph: empty profile)"; }
}

fetch_flamegraph_scripts() {
	[ -f bin/flamegraph/flamegraph.pl ] && return
	mkdir -p bin/flamegraph
	for f in flamegraph.pl stackcollapse-go.pl; do
		curl -fsSL "https://raw.githubusercontent.com/brendangregg/FlameGraph/$FLAMEGRAPH_COMMIT/$f" -o "bin/flamegraph/$f"
	done
}

command -v jq >/dev/null || fail "jq is required"
mkdir -p "$OUT" "$RAW"

say "building loadgen"
go build -o bin/loadgen ./cmd/loadgen

say "starting the stack (fresh collector, bench overlay)"
if [ "$PROFILE" = 1 ]; then
	# 1 in 100 mutex events and every block over 100µs: enough to see contention,
	# cheap enough that the profile still looks like the collector.
	export LOGAGG_ADMIN_BLOCK_PROFILE_RATE=${LOGAGG_ADMIN_BLOCK_PROFILE_RATE:-100000}
	export LOGAGG_ADMIN_MUTEX_PROFILE_FRACTION=${LOGAGG_ADMIN_MUTEX_PROFILE_FRACTION:-100}
	fetch_flamegraph_scripts
fi
# Anything else from the dev stack competes for the same VM. Stopped, not removed.
for s in agent logwriter proxy prometheus grafana jaeger; do
	docker stop "logagg-$s-1" >/dev/null 2>&1 || true
done
# A database recreated with a new server flag may spend a while in crash
# recovery (the larger max_wal_size gets, the longer), and Compose reports that
# as unhealthy rather than waiting. Give it a few tries before giving up.
for attempt in 1 2 3 4 5 6; do
	$COMPOSE up -d --wait --wait-timeout 240 timescaledb nats && break
	[ "$attempt" = 6 ] && fail "database did not become healthy"
	sleep 20
done
build_flag=(--build)
[ "$BUILD" = 0 ] && build_flag=()
$COMPOSE up -d "${build_flag[@]}" --force-recreate --wait --wait-timeout 240 collector
# Only logs, not streams: the collector caches which stream rows exist, and
# truncating streams under it would fail every batch on its foreign key. The
# collector has just migrated the schema, so the table exists even on a fresh
# volume.
psql_q "TRUNCATE logs" >/dev/null

say "warming up (${WARMUP_RECORDS} records)"
bin/loadgen -addr "$INGEST" -records "$WARMUP_RECORDS" -batch-size "$BATCH" \
	-streams "$STREAMS" -senders "$SENDERS" -message-size "$MESSAGE_SIZE" >/dev/null
wait_for_settle
rows_before=$(row_count)
curl -fsS "$ADMIN/metrics" | grep '^logagg_' >"$OUT/metrics-before.txt"

say "running: $NAME"
cpu_pid=
if [ "$PROFILE" = 1 ]; then
	curl -fsS "$ADMIN/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" -o "$RAW/cpu.pprof" &
	cpu_pid=$!
	sleep 1
fi
args=(-addr "$INGEST" -batch-size "$BATCH" -streams "$STREAMS" -senders "$SENDERS"
	-message-size "$MESSAGE_SIZE" -rate "$RATE" -out "$OUT/loadgen.json")
if [ -n "$DURATION" ]; then
	args+=(-duration "$DURATION")
else
	args+=(-records "$RECORDS")
fi
[ -n "$RAMP" ] && args+=(-ramp "$RAMP")
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# A non-zero exit (refused records) is a result to record, not a reason to stop:
# the accepted count is still exact and the drain still measures the writer.
loadgen_exit=0
bin/loadgen "${args[@]}" 2>&1 | tee "$OUT/loadgen.txt" || loadgen_exit=${PIPESTATUS[0]}

accepted=$(jq -r .accepted "$OUT/loadgen.json")
# The collector's footprint at its fullest: the client has stopped and the writer
# is still holding everything it has not yet written.
collector_mem_mb=$(docker stats --no-stream --format '{{.MemUsage}}' logagg-collector-1 | awk '{v=$1; if (v ~ /GiB/) {sub(/GiB/,"",v); v*=1024} else sub(/MiB/,"",v); printf "%d", v}')
say "draining: waiting for $accepted rows to land"
drain_sec=$(wait_for_rows $((rows_before + accepted)))
echo "drained in ${drain_sec}s"

if [ -n "$cpu_pid" ]; then
	wait "$cpu_pid" || echo "CPU profile did not complete"
	for p in heap allocs block mutex goroutine; do
		curl -fsS "$ADMIN/debug/pprof/$p" -o "$RAW/$p.pprof"
	done
fi
curl -fsS "$ADMIN/metrics" | grep '^logagg_' >"$OUT/metrics-after.txt"
rows_after=$(row_count)

if [ "$PROFILE" = 1 ]; then
	say "rendering flame graphs"
	flamegraph "$RAW/cpu.pprof" "$OUT/cpu" samples 1 samples
	flamegraph "$RAW/allocs.pprof" "$OUT/alloc" alloc_space 2 bytes
	flamegraph "$RAW/heap.pprof" "$OUT/heap" inuse_space 4 bytes
	flamegraph "$RAW/block.pprof" "$OUT/block" delay 2 ns
	flamegraph "$RAW/mutex.pprof" "$OUT/mutex" delay 2 ns
	echo "raw profiles: $RAW (not committed)"
fi

say "writing $OUT/run.json"
# The collector's own view of the run, from its counters: how many rows it wrote,
# how long each batch took, and whether anything was retried or deduplicated.
write_n=$(metric_delta logagg_write_duration_seconds_count)
write_sum=$(metric_delta logagg_write_duration_seconds_sum)
jq -n \
	--arg name "$NAME" --arg started "$started_at" \
	--arg commit "$(git rev-parse --short HEAD)" \
	--argjson hardware "$(hardware)" \
	--argjson env "$(env | grep -E '^(LOGAGG|PG)_' | jq -Rn '[inputs | capture("(?<key>[^=]+)=(?<value>.*)")] | from_entries')" \
	--argjson loadgen "$(cat "$OUT/loadgen.json")" \
	--argjson profiled "$([ "$PROFILE" = 1 ] && echo true || echo false)" \
	--argjson loadgen_exit "$loadgen_exit" \
	--argjson rows_before "$rows_before" --argjson rows_after "$rows_after" \
	--argjson drain_sec "$drain_sec" --argjson collector_mem_mb "${collector_mem_mb:-0}" \
	--argjson rows_inserted "$(metric_delta logagg_storage_rows_inserted_total)" \
	--argjson rows_deduplicated "$(metric_delta logagg_storage_rows_deduplicated_total)" \
	--argjson write_retries "$(metric_delta logagg_storage_write_retries_total)" \
	--argjson writes "$write_n" --argjson write_sum "$write_sum" \
	--argjson batch_sum "$(metric_delta logagg_batch_size_sum)" \
	--argjson batch_n "$(metric_delta logagg_batch_size_count)" \
	'{
	  name: $name, started_at: $started, commit: $commit, profiled: $profiled,
	  loadgen_exit: $loadgen_exit,
	  hardware: $hardware, env: $env, loadgen: $loadgen,
	  end_to_end: {
	    rows_before: $rows_before, rows_after: $rows_after,
	    drain_sec: $drain_sec, collector_mem_mb_at_drain: $collector_mem_mb,
	    # Accepted records over client time plus the drain: the rate the database
	    # actually absorbed, which is the number that matters when the client is
	    # faster than the writer.
	    sustained_records_per_sec: ($loadgen.accepted / ($loadgen.took_sec + $drain_sec))
	  },
	  writer: {
	    rows_inserted: $rows_inserted, rows_deduplicated: $rows_deduplicated,
	    write_retries: $write_retries, writes: $writes,
	    mean_write_ms: (if $writes > 0 then $write_sum / $writes * 1000 else null end),
	    mean_batch_size: (if $batch_n > 0 then $batch_sum / $batch_n else null end)
	  }
	}' >"$OUT/run.json"

jq -r '"\(.name): \(.loadgen.records_per_sec | floor) rec/s at the client, " +
	"\(.end_to_end.sustained_records_per_sec | floor) rec/s sustained end to end, " +
	"drain \(.end_to_end.drain_sec)s, ack p99 \(.loadgen.ack_p99_ms)ms, " +
	"mean write \(.writer.mean_write_ms // 0 | . * 10 | round / 10)ms over \(.writer.writes) batches"' "$OUT/run.json"
