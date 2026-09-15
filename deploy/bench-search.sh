#!/usr/bin/env bash
# Phase 8 §2.4: substring search on the message column, three ways.
#
# Runs against whatever is in the dev database's logs table (a bench.sh run leaves
# ~3M rows there) and times the same searches with no index, with a pg_trgm GIN
# index, and with a tsvector GIN index. Every timing is the median of three
# EXPLAIN ANALYZE executions after one warm-up, so it measures the plan and not the
# page cache filling. Index build time and on-disk size are recorded too, because
# a search index is paid for on every insert and a benchmark that only shows the
# read side is half a benchmark.
#
# Two query shapes, because the DSL produces both:
#   count   -- `| count_over_time` style: touch every match
#   latest  -- the default `ORDER BY time DESC LIMIT 100`: stop at the first page
# Three needles, chosen from the loadgen vocabulary by Zipf rank so their
# selectivity is known: a word in nearly every row, one in some, one in few.
#
# The tsvector variant does not do the same thing as the other two. It matches
# whole words, not substrings, so `seq=12` or `dead` would find nothing. It is
# measured because §2.4 named it; the write-up says why that difference decides it.
set -euo pipefail

OUT=${OUT:-docs/benchmarks/runs/search}
LIMIT=${LIMIT:-100}
NEEDLES=${NEEDLES:-"request timeout corrupt"}
COMPOSE="docker compose -f deploy/docker-compose.yml"

psql_q() { $COMPOSE exec -T timescaledb psql -U logagg -d logagg -At -v ON_ERROR_STOP=1 -c "$1"; }

# exec_ms runs a query via EXPLAIN ANALYZE and prints its execution time in ms.
exec_ms() {
	psql_q "EXPLAIN (ANALYZE, FORMAT JSON) $1" | jq -r '.[0]["Execution Time"]'
}

# median_ms warms once, then reports the median of three runs, and the plan's
# top node so the write-up can say *how* it ran, not only how fast.
median_ms() {
	exec_ms "$1" >/dev/null
	local a b c
	a=$(exec_ms "$1"); b=$(exec_ms "$1"); c=$(exec_ms "$1")
	printf '%s\n%s\n%s\n' "$a" "$b" "$c" | sort -n | sed -n 2p
}

plan_node() {
	psql_q "EXPLAIN (FORMAT JSON) $1" | jq -r '
		def walk_scan: if .["Node Type"] | test("Scan") then .["Node Type"] + (if .["Index Name"] then " on " + .["Index Name"] else "" end)
			elif .Plans then (.Plans[] | walk_scan) else empty end;
		[.[0].Plan | walk_scan] | first // .[0].Plan["Node Type"]'
}

index_bytes() { psql_q "SELECT coalesce(hypertable_index_size('$1'), 0)"; }

# queries prints the two query shapes for a predicate.
count_sql()  { echo "SELECT count(*) FROM logs WHERE $1"; }
latest_sql() { echo "SELECT time, message FROM logs WHERE $1 ORDER BY time DESC LIMIT $LIMIT"; }

mkdir -p "$OUT"
results="$OUT/results.jsonl"
: >"$results"

rows=$(psql_q "SELECT count(*) FROM logs")
[ "$rows" -gt 0 ] || { echo "logs is empty; run a bench first" >&2; exit 1; }
echo "searching $rows rows; needles: $NEEDLES"

measure() {
	local variant=$1 predicate_tmpl=$2 index=$3 build_sec=$4
	local size=0
	[ -n "$index" ] && size=$(index_bytes "$index")
	for needle in $NEEDLES; do
		local pred=${predicate_tmpl//NEEDLE/$needle}
		local matches count_ms latest_ms count_plan latest_plan
		matches=$(psql_q "$(count_sql "$pred")")
		count_ms=$(median_ms "$(count_sql "$pred")")
		latest_ms=$(median_ms "$(latest_sql "$pred")")
		count_plan=$(plan_node "$(count_sql "$pred")")
		latest_plan=$(plan_node "$(latest_sql "$pred")")
		jq -cn --arg v "$variant" --arg n "$needle" --argjson rows "$rows" --argjson m "$matches" \
			--argjson c "$count_ms" --argjson l "$latest_ms" --arg cp "$count_plan" --arg lp "$latest_plan" \
			--argjson size "$size" --argjson build "$build_sec" \
			'{variant: $v, needle: $n, rows: $rows, matches: $m, selectivity: ($m / $rows),
			  count_ms: $c, count_plan: $cp, latest_ms: $l, latest_plan: $lp,
			  index_bytes: $size, index_build_sec: $build}' | tee -a "$results"
	done
}

timed() {
	local started ended
	started=$(perl -MTime::HiRes=time -e 'print time')
	psql_q "$1" >/dev/null
	ended=$(perl -MTime::HiRes=time -e 'print time')
	echo "$ended $started" | awk '{printf "%.1f", $1 - $2}'
}

echo; echo "--- ilike, no index"
psql_q "DROP INDEX IF EXISTS logs_message_trgm; DROP INDEX IF EXISTS logs_message_tsv" >/dev/null
measure ilike "message ILIKE '%NEEDLE%'" "" 0

echo; echo "--- ilike, pg_trgm GIN"
psql_q "CREATE EXTENSION IF NOT EXISTS pg_trgm" >/dev/null
build=$(timed "CREATE INDEX logs_message_trgm ON logs USING GIN (message gin_trgm_ops)")
echo "built in ${build}s"
measure trgm "message ILIKE '%NEEDLE%'" logs_message_trgm "$build"
psql_q "DROP INDEX logs_message_trgm" >/dev/null

echo; echo "--- tsvector GIN"
build=$(timed "CREATE INDEX logs_message_tsv ON logs USING GIN (to_tsvector('simple', message))")
echo "built in ${build}s"
measure tsvector "to_tsvector('simple', message) @@ plainto_tsquery('simple', 'NEEDLE')" logs_message_tsv "$build"
psql_q "DROP INDEX logs_message_tsv" >/dev/null

echo; echo "results: $results"
jq -r '[.variant, .needle, (.selectivity * 100 | round | tostring + "%"), (.count_ms | round), .count_plan, (.latest_ms | round), .latest_plan, (.index_bytes / 1048576 | round | tostring + " MB")] | @tsv' "$results" | column -t
