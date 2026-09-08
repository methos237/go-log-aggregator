#!/usr/bin/env bash
# Phase 3 exit criteria, demonstrated on the real stack.
#
# Brings up the agent alongside a sidecar that writes and rotates a log file,
# subjects it to both events the phase must survive -- a collector restart and a
# log rotation -- and then asserts that every line the sidecar wrote arrived, with
# no gaps and with duplicates inside a bound.
#
# Why the sidecar numbers its lines: a gap and a duplicate both move a plain row
# count, and only a gap skips a number. Asserting on the sequence is the only way
# to tell the two apart, and telling them apart is exactly what the exit criteria
# ask for -- the design permits bounded duplicates and forbids gaps outright.
#
# The sidecar is stopped before the final assertion so its counter stops moving.
# Otherwise "has the agent caught up?" has no fixed target and the check would
# race the writer forever.
set -euo pipefail

COMPOSE=${COMPOSE:-docker compose -f deploy/docker-compose.yml}
# Faster than the sidecar's own defaults: this needs several rotations inside a
# short run, not a realistic write rate.
WRITE_INTERVAL=${WRITE_INTERVAL:-0.2}
ROTATE_EVERY=${ROTATE_EVERY:-25}
# How long to let the stack run before the assertion phase. At the settings above
# this is ~5 lines/second and a rotation every ~5 seconds.
RUN_SECONDS=${RUN_SECONDS:-60}
# How long to wait for everything to land after the writer stops.
#
# This must exceed the collector's JetStream consumer ack_wait, which defaults to
# five minutes, and the reason is worth knowing because it looks exactly like data
# loss when you get it wrong. Restarting the collector leaves whatever its consumer
# had delivered-but-unacked pending in JetStream, and those messages are not
# redelivered until ack_wait expires. So the observable recovery time after a
# collector restart is bounded by ack_wait, not by anything the agent does: with a
# shorter window than this the run reports a gap that fills in by itself minutes
# later. The agent's own metrics are the tell -- zero drops with rows still
# missing means "not yet redelivered", not "lost".
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-400}
# Duplicate ceiling. Duplicates here come from two bounded sources: batches
# in flight when the collector restarts (resent from the spool, since the
# checkpoint only advances on an ack) and the spool's cursor-persist window. This
# ceiling is deliberately generous -- the load-bearing assertion is zero gaps --
# but it is low enough to catch a systemic "re-ship everything on reconnect" bug,
# which is the failure mode worth guarding against.
MAX_EXCESS=${MAX_EXCESS:-100}

SERVICE_LABEL=app
HOST_LABEL=agent
ENV_LABEL=dev

say() { printf '\n=== %s\n' "$*"; }

psql_q() {
	$COMPOSE exec -T timescaledb psql -U logagg -d logagg -At -c "$1"
}

# received_stats prints "rows distinct min max" for the agent's stream.
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

cleanup_note() {
	printf '\nStack left running. Inspect with:\n'
	printf '  %s logs agent\n' "$COMPOSE"
	printf '  %s exec timescaledb psql -U logagg -d logagg\n' "$COMPOSE"
	printf 'Tear down with: make dev-nuke\n'
}
trap cleanup_note EXIT

# Reset only this test's own state, never the shared database volume. Two things
# have to go together or a rerun silently lies: the sidecar's counter lives on the
# log volume, so dropping that volume restarts numbering at 1, and rows from a
# previous run carrying those same numbers would then read as duplicates -- or
# worse, fill a gap the current run actually has. So the volumes and the stream's
# rows are cleared as a pair.
say "resetting this test's state (agent volumes and the app stream's rows)"
$COMPOSE rm -sf agent logwriter >/dev/null 2>&1 || true
for v in agent-state agent-log-data; do
	docker volume rm "logagg_${v}" >/dev/null 2>&1 || true
done

$COMPOSE up -d --wait --wait-timeout 240 timescaledb nats collector
psql_q "
	DELETE FROM logs
	WHERE stream_id IN (
		SELECT stream_id FROM streams
		WHERE service = '$SERVICE_LABEL' AND host = '$HOST_LABEL' AND env = '$ENV_LABEL'
	);" >/dev/null
echo "prior rows for this stream cleared"

# The agent starts BEFORE the writer, and the ordering is not cosmetic. The agent
# waits for a missing file to appear, but it never chases a file that was rotated
# away before it ever opened one -- by design, since it only opens its configured
# path. If the writer went first it could write and rotate away an entire
# generation during the agent's container startup, and those lines would be
# legitimately unreachable: with no checkpoint yet, the agent cannot know data
# preceded it. That is correct behavior and it would look like a gap here.
say "starting the agent first, so no generation is written before it is watching"
$COMPOSE up -d --build --wait --wait-timeout 240 agent

say "starting the sidecar (${WRITE_INTERVAL}s/line, rotate every ${ROTATE_EVERY} lines)"
WRITE_INTERVAL=$WRITE_INTERVAL ROTATE_EVERY=$ROTATE_EVERY \
	$COMPOSE up -d --wait --wait-timeout 240 logwriter

say "waiting for the first records to land"
deadline=$((SECONDS + 90))
while :; do
	read -r rows _ _ _ <<<"$(received_stats)"
	[ "${rows:-0}" -gt 0 ] && break
	if [ "$SECONDS" -ge "$deadline" ]; then
		echo "FAIL: no records arrived within 90s; the agent never shipped anything" >&2
		$COMPOSE logs --tail 40 agent >&2
		exit 1
	fi
	sleep 2
done
echo "first records present"

# Exit-criterion event 1: the collector goes away mid-flight. Batches already
# sent but unacked must be spooled and replayed, not lost, and the agent must not
# exit.
say "letting it run, then restarting the collector"
sleep $((RUN_SECONDS / 3))
before_restart=$(received_stats)
$COMPOSE restart collector
echo "collector restarted (stats before: $before_restart)"

# Exit-criterion event 2: a rotation forced at a moment of our choosing, on top of
# the ones the sidecar performs on its own schedule.
#
# It is deliberately timed to land mid-cycle rather than whenever the clock says.
# The agent detects rotation by polling, so it survives rotations spaced well
# apart and cannot survive two inside one poll interval -- the generation between
# them is skipped, and by design it is not even countable mid-run, because from
# the source's point of view two rotations and one look identical. Forcing a
# rotation blindly can land microseconds from a scheduled one and produce exactly
# that, which is a real property of the design rather than a bug, but not the
# thing this test is trying to measure. So wait for the writer to be about halfway
# to its next scheduled rotation first.
say "forcing an out-of-band rotation, timed away from the sidecar's own schedule"
sleep $((RUN_SECONDS / 3))
deadline=$((SECONDS + 60))
while :; do
	seq_now=$($COMPOSE exec -T logwriter sh -c 'cat "$LOG_DIR/.seq"' | tr -d '\r[:space:]')
	phase=$((seq_now % ROTATE_EVERY))
	# The middle third of a cycle: far enough from the rotation behind and the one
	# ahead that a poll interval cannot span both.
	if [ "$phase" -gt $((ROTATE_EVERY / 3)) ] && [ "$phase" -lt $((2 * ROTATE_EVERY / 3)) ]; then
		break
	fi
	if [ "$SECONDS" -ge "$deadline" ]; then
		echo "warning: never hit a safe window; forcing anyway" >&2
		break
	fi
	sleep 0.2
done
$COMPOSE exec -T logwriter sh -c 'mv "$LOG_DIR/app.log" "$LOG_DIR/app.log.1" && : > "$LOG_DIR/app.log"'
echo "rotation forced at seq=$seq_now (phase $phase of $ROTATE_EVERY)"

sleep $((RUN_SECONDS / 3))

# Stop the writer so its counter stops moving and the drain has a fixed target.
say "stopping the sidecar and reading its final counter"
$COMPOSE stop logwriter >/dev/null
written=$($COMPOSE run --rm --no-deps --entrypoint sh logwriter -c 'cat /var/log/app/.seq' | tr -d '\r[:space:]')
if ! [ "$written" -gt 0 ] 2>/dev/null; then
	echo "FAIL: could not read the sidecar's counter (got '$written')" >&2
	exit 1
fi
echo "sidecar wrote $written lines"

say "waiting for the agent to drain (timeout ${DRAIN_TIMEOUT}s)"
deadline=$((SECONDS + DRAIN_TIMEOUT))
while :; do
	read -r rows distinct min_seq max_seq <<<"$(received_stats)"
	printf '  rows=%s distinct=%s max=%s/%s\n' "$rows" "$distinct" "$max_seq" "$written"
	[ "${distinct:-0}" -ge "$written" ] && break
	if [ "$SECONDS" -ge "$deadline" ]; then
		echo "drain timed out; asserting on what arrived" >&2
		break
	fi
	sleep 5
done

say "results"
read -r rows distinct min_seq max_seq <<<"$(received_stats)"
excess=$((rows - distinct))
printf 'sidecar wrote     : %s\n' "$written"
printf 'rows received     : %s\n' "$rows"
printf 'distinct seq      : %s\n' "$distinct"
printf 'seq range         : %s..%s\n' "$min_seq" "$max_seq"
printf 'duplicate excess  : %s (ceiling %s)\n' "$excess" "$MAX_EXCESS"

# Report the actual missing numbers, not just a count: "3 missing" is not
# actionable, while a run of consecutive missing numbers points straight at a
# rotation boundary.
missing=$(psql_q "
	SELECT string_agg(n::text, ',')
	FROM generate_series(1, $written) AS n
	WHERE NOT EXISTS (
		SELECT 1
		FROM logs l JOIN streams s USING (stream_id)
		WHERE s.service = '$SERVICE_LABEL' AND s.host = '$HOST_LABEL' AND s.env = '$ENV_LABEL'
		  AND l.message LIKE '%seq=' || n || ' %'
	);")

status=0
if [ -n "$missing" ]; then
	printf 'GAPS              : %s\n' "$missing"
	echo "FAIL: gaps are forbidden by the exit criteria" >&2
	status=1
else
	echo 'gaps              : none'
fi

if [ "$excess" -gt "$MAX_EXCESS" ]; then
	echo "FAIL: duplicate excess $excess exceeds the $MAX_EXCESS ceiling" >&2
	status=1
fi

if [ "$status" -eq 0 ]; then
	say "PASS: no gaps, duplicates within bound, across a collector restart and log rotations"
fi
exit "$status"
