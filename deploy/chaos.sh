#!/usr/bin/env bash
# Phase 5 exit criteria, demonstrated on the real stack.
#
# Runs N collectors behind the proxy, ships numbered lines through the agent,
# kills two collectors outright while lines are in flight, and then asserts two
# things: every line the sidecar wrote is in the database with no gaps, and
# /v1/cluster on a survivor shows the ring rebalanced onto the survivors.
#
# Kill, not stop. A graceful shutdown withdraws readiness, leaves the gossip
# ring and drains its queue consumer; none of that happens here. What the test
# claims is that the delivery contract holds anyway: an acked record is in
# JetStream, a batch the dying collector had pulled but not acked is redelivered
# once ack_wait expires, and the agent resends anything it never got an ack for.
#
# Why the sidecar numbers its lines, why the stream's rows are cleared first,
# and why the writer is stopped before the assertion: see e2e-agent.sh, whose
# machinery this reuses.
set -euo pipefail

COMPOSE=${COMPOSE:-docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.scale.yml}
N=${N:-5}
KILL=${KILL:-2}
WRITE_INTERVAL=${WRITE_INTERVAL:-0.1}
ROTATE_EVERY=${ROTATE_EVERY:-50}
# Lines keep flowing this long after the kill before the writer is stopped.
RUN_AFTER_KILL=${RUN_AFTER_KILL:-30}
# A killed collector's unacked deliveries sit in JetStream until ack_wait
# expires, so recovery time is bounded by ack_wait and the drain has to outlast
# it. Both are shortened for this run; the collector refuses an ack_wait below
# the writer's worst case, which is why this is not shorter still.
export LOGAGG_QUEUE_ACK_WAIT=${LOGAGG_QUEUE_ACK_WAIT:-150s}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-240}
MAX_EXCESS=${MAX_EXCESS:-200}
TOKEN=${LOGAGG_HTTP_AUTH_TOKEN:-dev-token}

SERVICE_LABEL=app
HOST_LABEL=agent
ENV_LABEL=dev

say() { printf '\n=== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

psql_q() {
	$COMPOSE exec -T timescaledb psql -U logagg -d logagg -At -c "$1"
}

received_stats() {
	psql_q "
		SELECT coalesce(count(*), 0), coalesce(count(DISTINCT seq), 0),
		       coalesce(min(seq), 0), coalesce(max(seq), 0)
		FROM (
			SELECT ((regexp_match(l.message, 'seq=([0-9]+)'))[1])::bigint AS seq
			FROM logs l
			JOIN streams s USING (stream_id)
			WHERE s.service = '$SERVICE_LABEL'
			  AND s.host    = '$HOST_LABEL'
			  AND s.env     = '$ENV_LABEL'
		) t
		WHERE seq IS NOT NULL;" | tr '|' ' '
}

cluster_members() {
	curl -fsS -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/cluster |
		python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["members"]), sum(1 for m in d["members"] if m["ready"]))'
}

cleanup_note() {
	printf '\nStack left running. Inspect with:\n'
	printf '  curl -H "Authorization: Bearer %s" http://127.0.0.1:8080/v1/cluster\n' "$TOKEN"
	printf '  %s logs agent\n' "$COMPOSE"
	printf 'Tear down with: make dev-nuke\n'
}
trap cleanup_note EXIT

say "resetting this test's state (agent volumes and the app stream's rows)"
$COMPOSE rm -sf agent logwriter >/dev/null 2>&1 || true
for v in agent-state agent-log-data; do
	docker volume rm "logagg_${v}" >/dev/null 2>&1 || true
done

say "starting $N collectors behind the proxy (ack_wait=$LOGAGG_QUEUE_ACK_WAIT)"
$COMPOSE up -d --build --wait --wait-timeout 240 --scale "collector=$N" timescaledb nats collector proxy
psql_q "
	DELETE FROM logs
	WHERE stream_id IN (
		SELECT stream_id FROM streams
		WHERE service = '$SERVICE_LABEL' AND host = '$HOST_LABEL' AND env = '$ENV_LABEL'
	);" >/dev/null

say "waiting for all $N members to be ready in the ring"
deadline=$((SECONDS + 60))
while :; do
	read -r members ready <<<"$(cluster_members || echo 0 0)"
	[ "$ready" -eq "$N" ] && break
	[ $SECONDS -ge $deadline ] && fail "ring shows $ready/$members ready members, want $N"
	sleep 1
done
echo "ring: $members members, $ready ready"

# The agent reaches ingest through the proxy's service name, which is what a
# host-side client also does, and which is what keeps it connected when the
# collector it was talking to dies: the next connection lands elsewhere.
# --no-deps matters: without it compose reconciles the agent's dependencies
# to their configured scale of one and quietly removes four collectors.
say "starting the agent, then the sidecar (${WRITE_INTERVAL}s/line)"
LOGAGG_AGENT_INGEST_ADDR=proxy:9095 $COMPOSE up -d --build --no-deps --wait --wait-timeout 240 agent
WRITE_INTERVAL=$WRITE_INTERVAL ROTATE_EVERY=$ROTATE_EVERY \
	$COMPOSE up -d --no-deps --wait --wait-timeout 240 logwriter

say "waiting for the first records to land"
deadline=$((SECONDS + 90))
while :; do
	read -r rows _ _ _ <<<"$(received_stats)"
	[ "${rows:-0}" -gt 100 ] && break
	[ $SECONDS -ge $deadline ] && fail "no records arrived within 90s"
	sleep 2
done
echo "$rows rows in; lines are flowing"

# Pick victims from the running replicas. The proxy is never a victim: it is
# dev plumbing, not part of the claim.
victims=$(docker ps --filter label=com.docker.compose.project=logagg --filter label=com.docker.compose.service=collector --format '{{.Names}}' | sort | head -n "$KILL")
[ "$(echo "$victims" | wc -l | tr -d ' ')" -eq "$KILL" ] || fail "found $(echo "$victims" | wc -l | tr -d ' ') running collectors to kill, want $KILL of $N"
say "killing $KILL collectors mid-flight: $(echo "$victims" | tr '\n' ' ')"
# The stack's restart policy would bring a killed container straight back,
# which is not the scenario: these two stay dead for the rest of the run.
# shellcheck disable=SC2086
docker update --restart=no $victims >/dev/null
# shellcheck disable=SC2086
docker kill $victims >/dev/null
read -r rows_at_kill _ _ _ <<<"$(received_stats)"
echo "$rows_at_kill rows at the moment of the kill"

say "running ${RUN_AFTER_KILL}s more on the survivors"
sleep "$RUN_AFTER_KILL"

say "stopping the sidecar so the target stops moving"
$COMPOSE stop logwriter >/dev/null
written=$($COMPOSE run --rm --no-deps -T --entrypoint cat logwriter /var/log/app/.seq | tr -d '[:space:]')
[ -n "$written" ] && [ "$written" -gt 0 ] || fail "could not read the sidecar's line counter"
echo "sidecar wrote $written lines"

say "waiting up to ${DRAIN_TIMEOUT}s for every line to land (redelivery after a kill waits for ack_wait)"
deadline=$((SECONDS + DRAIN_TIMEOUT))
while :; do
	read -r rows distinct min max <<<"$(received_stats)"
	if [ "$distinct" -ge "$written" ]; then break; fi
	[ $SECONDS -ge $deadline ] && break
	sleep 5
done
echo "rows=$rows distinct=$distinct min=$min max=$max written=$written"

say "asserting"
[ "$min" -eq 1 ] || fail "first line received is seq=$min, want 1"
[ "$max" -eq "$written" ] || fail "last line received is seq=$max, sidecar wrote $written"
[ "$distinct" -eq "$written" ] || fail "$((written - distinct)) lines missing: gaps in the sequence"
excess=$((rows - distinct))
[ "$excess" -le "$MAX_EXCESS" ] || fail "$excess duplicate rows, ceiling is $MAX_EXCESS"
echo "no gaps: $distinct/$written lines present, $excess duplicates (ceiling $MAX_EXCESS)"

read -r members ready <<<"$(cluster_members)"
expected=$((N - KILL))
[ "$members" -eq "$expected" ] || fail "/v1/cluster shows $members members after the kill, want $expected"
[ "$ready" -eq "$expected" ] || fail "/v1/cluster shows $ready ready members, want $expected"
echo "ring rebalanced: $members members, all ready, after killing $KILL of $N"

# The survivors answer the whole stream's count through the proxy, fanned out
# across whoever owns the streams now, with no shard warnings. The line filter
# is there to force the raw table: a bare count would be served from the hourly
# continuous aggregate, whose materialized buckets still hold the rows this run
# deleted at the start (a delete older than the refresh window is never
# re-materialized), and the point here is the fan-out, not the aggregate.
count=$(curl -fsS -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
	-d "{\"query\":\"{service=\\\"$SERVICE_LABEL\\\", host=\\\"$HOST_LABEL\\\"} |= \\\"seq=\\\" | count_over_time(1d)\",\"start\":\"2000-01-01T00:00:00Z\",\"end\":\"2100-01-01T00:00:00Z\"}" \
	http://127.0.0.1:8080/v1/query |
	python3 -c 'import json,sys; d=json.load(sys.stdin); assert not d.get("warnings"), d["warnings"]; print(int(sum(p["value"] for p in d["points"])))')
[ "$count" -eq "$rows" ] || fail "fan-out counted $count rows, database has $rows"
echo "fan-out query agrees with the database: $count rows"

say "PASS: zero acked records lost across $KILL kills; ring rebalanced to $expected members"
