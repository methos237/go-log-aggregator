# ADR-0009: The agent

- **Status:** Accepted
- **Date:** 2026-09-16 (backfilled; the decisions date from phase 3, merged 2026-09-08)
- **Supersedes:** none

## Context

The agent is the process that runs next to the programs producing logs, reads their
output, and ships it to a collector over the ingest stream (ADR-0003). It is the one
component that has to survive things it does not control: files rotated under it,
collectors that restart, and its own restarts. Phase 3 shipped it with a file tail
source, a stdin source, a Docker log source, multiline joining, field extraction, a
bounded on-disk spool and a batching shipper. The decisions were recorded in code
comments at the time; this ADR collects them.

The exit criterion that shaped every choice: a file written by a sidecar and rotated
repeatedly, across a collector restart, arrives with no gaps and bounded duplicates.

## Decisions

### 1. Progress is durable only once the collector has acknowledged it

A source knows nothing about acknowledgement. It reads lines and pushes them into a
bounded channel; the checkpoint store advances a source's cursor only when the
shipper receives `ACK_CODE_ACCEPTED` for the batch that carried it. A crash between
read and ack replays the lines, and the dedup index (ADR-0002 §2) absorbs the
duplicates.

Rejected:

- **Checkpoint on read.** Simpler, and wrong: a crash between read and ack loses
  those lines forever, silently, converting the at-least-once promise into
  at-most-once for exactly the window that matters.
- **Sources that track their own durability.** A second checkpoint store that can
  disagree with the first. One owner of "how far are we", driven by acks.

### 2. `Seq` is the line's byte offset, carried explicitly

A file line's `Cursor.Start` is the byte offset of its first byte and becomes the
record's `Seq`. It is stored, not derived: `Offset` minus line length is wrong for
CRLF input and for every truncated line, and a `Seq` that does not reproduce exactly
on replay turns deduplication into duplicate rows. For the Docker source, `Start`
is the line's timestamp in Unix nanoseconds, which the daemon assigns once.

Consequence: `Seq` restarts near zero when a file rotates. Nothing downstream may
assume a contiguous `Seq` per stream; the end-to-end tests assert on numbered
payloads instead.

Rejected:

- **An in-process generation counter in the cursor.** Convenient for noticing
  rotation while running, and worthless in a checkpoint file, because it resets to
  zero on the restart the checkpoint exists to survive. File identity plus offset is
  the pair that still means something after a crash.

### 3. Rotation and truncation are detected by identity and content, not by name

The tail source watches one path. Rename-and-recreate rotation is an inode change
under an already-open descriptor; the old descriptor is drained to true EOF before
the switch, because a renamed file keeps accepting the writer's last flush. In-place
truncation is a size shrink, or, when the rewrite lands past the old offset so the
size never appears to shrink, a fingerprint over the file's first 256 bytes that no
longer matches. The fingerprint is persisted in the checkpoint so that a restart can
tell "same file, resume" from "same inode, replaced contents", which inode and size
together cannot.

Any monotonicity check on `Offset` is scoped to a matching file identity. Without
that scope the first rotation freezes the checkpoint on the old generation forever
and every restart re-reads the whole current file. This bug shipped once and was
caught by a test; the rule is now a comment at the check.

Rejected:

- **Filename patterns.** Following `.1` or `.gz` siblings means guessing every
  rotation tool's naming scheme, and guessing wrong reads a file twice or never.
- **A file-watching library (`fsnotify`).** Notification semantics differ per
  platform in exactly the rename and truncate cases that matter here. A 250 ms poll
  once caught up to EOF is portable, cheap, and does not compete with the writer
  for disk.

Accepted limitation: the agent only ever opens its configured path. A rotation
missed while the agent was down, or two rotations inside one poll interval, skips a
generation. The first case is detected on restart and counted; the second is
indistinguishable from one rotation and is not. Keep the poll interval well under
the rotation interval.

### 4. Backpressure blocks; only hard bounds drop

The collector sheds load (ADR-0003 §3) because it has a client to say "slow down"
to. The agent's only unbounded resource is the file it is reading, which is already
durable on disk, so a full channel simply stops the reader and the file holds the
data. Records are dropped in two places only, each with a counter: an over-long line,
and a full spool.

Rejected:

- **Shedding in the agent.** Dropping a line that is safely on disk to protect a
  queue in memory is backwards.

### 5. The spool is a bounded, replayed buffer for send failures, not a write-ahead log

`Spool.Append` runs only after a send has failed, never before, and never fsyncs.
Entries are length-prefixed with a CRC32 and an explicit byte order, so a segment
written on one architecture reads back on another and a torn tail is detected rather
than parsed. Entries are released only on ack. When the spool exceeds its byte bound,
whole oldest segments are deleted and counted as drops.

Rejected:

- **Write every record to disk before sending.** A WAL doubles the write load of
  every agent to protect against a case the source file already protects against;
  everything in the spool is unacknowledged, so losing the spool on a crash loses
  nothing that the checkpoint would not replay.
- **Dropping newest on a full spool.** Keeps the oldest data, which is the data an
  operator investigating the outage wants least, and it makes the agent's behaviour
  under a long outage depend on when the outage started.

### 6. One source per Docker container stream

A Docker source names one container and one of `stdout` or `stderr`, never both.
The daemon multiplexes both over one connection, and demultiplexing them into one
source is the tempting design, but a single source has a single cursor, and two
interleaved streams with independent progress cannot share one. The Docker client
interface is the exact method signature of `moby/moby/client`, already an indirect
dependency, so tests substitute a fake and production uses the real client without a
wrapper.

### 7. One wire client, shared

The agent does not speak gRPC. `internal/ingest.Client` is the single client, shared
with `logctl` and `loadgen`, so there is one place the stream protocol, the ack
window and TLS live. The agent gained `DialLazy`, which returns before the
connection is ready and lets gRPC reconnect, because an agent legitimately starts
before its collector; `Dial` still waits, which is right for a CLI.

## Consequences

- Duplicates are bounded by the ack window: a crash replays at most the batches in
  flight. The compose end-to-end test measured 911 lines with zero gaps and zero
  duplicates across a collector restart and 22 rotations.
- Recovery after a collector restart is bounded by the JetStream consumer's
  `ack_wait`, not by the agent. Delivered-but-unacknowledged messages are redelivered
  only after it expires, which looks like data loss for up to five minutes while the
  agent's own drop counters read zero. Wait past `ack_wait` before concluding
  anything.
- A joined multiline record takes the first line's `Start` and time and the last
  line's `Offset`, file identity and fingerprint, so acknowledging it advances the
  checkpoint past every line it contains.
- The agent exposes its own metrics on an admin port and, in the dev stack, tails the
  collector's container so the system ingests its own logs (ADR-0007).
