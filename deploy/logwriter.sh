#!/bin/sh
# Sidecar for the agent demo stack (see docker-compose.yml, service "logwriter").
#
# Continuously appends a timestamped, numbered line to $LOG_DIR/app.log, then
# rotates by rename-and-recreate every ROTATE_EVERY lines: the current file
# becomes app.log.1 (overwriting whatever was there) and a fresh app.log is
# started. That is the exact scheme the agent's rotation detection is built
# around, so this is what exercises it without anyone triggering it by hand.
#
# The line counter lives in a side file (COUNTER_FILE), not in the log file
# itself, so it keeps incrementing straight through a rotation -- and even
# across a restart of this container, since COUNTER_FILE sits on the same
# shared volume as the log. That is what lets a test assert "no gaps" by
# checking the sequence numbers the agent actually received: a gap and a
# duplicate both move a plain row count, but only a gap skips a number.
set -eu

LOG_DIR="${LOG_DIR:-/var/log/app}"
LOG_FILE="${LOG_DIR}/app.log"
COUNTER_FILE="${LOG_DIR}/.seq"
WRITE_INTERVAL="${WRITE_INTERVAL:-1}"   # seconds between lines; modest by design
ROTATE_EVERY="${ROTATE_EVERY:-20}"      # lines written between rotations

mkdir -p "$LOG_DIR"
[ -f "$LOG_FILE" ] || : > "$LOG_FILE"
[ -f "$COUNTER_FILE" ] || echo 0 > "$COUNTER_FILE"

while true; do
	seq=$(cat "$COUNTER_FILE")
	seq=$((seq + 1))

	printf '%s seq=%d host=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$seq" "$(hostname)" >> "$LOG_FILE"
	echo "$seq" > "$COUNTER_FILE"

	if [ "$((seq % ROTATE_EVERY))" -eq 0 ]; then
		mv "$LOG_FILE" "${LOG_FILE}.1"
		: > "$LOG_FILE"
	fi

	sleep "$WRITE_INTERVAL"
done
