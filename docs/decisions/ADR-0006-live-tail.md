# ADR-0006: Live tail

- **Status:** Accepted
- **Date:** 2026-09-13
- **Supersedes:** none
- **Amends:** ADR-0001 (decision 2: tail subscriptions are not routed by ring
  ownership)
- **Refines:** ADR-0003 (the ingest handler's ack ordering)

## Context

Phase 6 adds `GET /v1/tail`: a WebSocket that streams each record a query
matches as it arrives, and `logctl tail` on top of it. Two things had to hold.
A tail reader must never slow ingest, not by a stalled browser tab and not by
a thousand of them. And a record the tail shows must be one the query would
have found, with the same selector semantics, or the two surfaces disagree
about what the data is.

The project plan (not committed) sketched a JetStream fan-out subject and
ring-routed subscriptions. Both were reconsidered once the pieces were on the
table.

## Decisions

### 1. The fan-out is a core NATS publish, not a second stream

After the ingest handler's JetStream publish is acknowledged, and before it
acknowledges the agent, it copies the same bytes to `tail.<env>.<service>`
with a plain `nc.Publish`. No stream captures that subject; configuration
refuses a tail prefix under the durable stream's filter, since the stream
would otherwise store every record twice.

A JetStream publish waits for the broker to write the message and answer.
That is the price the durable path pays for the ack it hands the agent, and
the tail path needs neither half of it: a record nobody is tailing is not
worth storing, and one that arrives late is not worth waiting for. A core
publish appends to the connection's outbound buffer and returns. The durable
publishes share that buffer, so the copy adds no way for ingest to stall that
did not already exist. A failed copy is counted in
`logagg_queue_fanout_total{outcome="failure"}` and logged at debug; the agent
is never told, because from its side nothing went wrong.

The copy is made by the collector that received the batch. It is durable by
then, so a tail never shows a record the store will not have; it may show one
the writer has not landed yet, which is what "live" means.

Rejected:

- A JetStream fan-out subject with a consumer per tail, the plan's shape. Every
  record would be stored twice, each copy would wait for the broker's ack, and
  a tail client would hold a consumer the broker has to track; the tail path
  needs none of that.

### 2. Subscriptions are evaluated where the client is

A client's subscription lives on the node it connected to. That node
subscribes to the narrowest subject the selector allows — `tail.prod.api` when
env and service are pinned with `=`, wildcards otherwise — decodes each batch,
and runs the query evaluator over it. The hash ring is not consulted.

ADR-0001 expected ownership to route tail subscriptions as it routes query
fan-out. For queries the ring divides a scan that would otherwise be
duplicated. For tail the fan-out already reaches every node, so there is no
duplicated work to divide; owner-side evaluation would need a second hop to
bring matches back to the client's node and a cluster-wide mirror of every
subscription, and would buy only a spread of matching CPU across nodes. Tail
clients are people, so they are few, and per-node evaluation is proportional
to them. The upgrade path is recorded in the package comment of
`internal/tail` for the day tail CPU shows up in a profile.

Rejected:

- Routing subscriptions by ring ownership, as ADR-0001 and the plan had it, for
  the reasons above: a second hop and a subscription mirror, to divide work
  that is not duplicated.

### 3. One evaluator, mirroring the planner

`query.Evaluator` applies the same parsed query to a `LabelSet` and a
`LogRecord` that the planner compiles to SQL, predicate for predicate,
including the corners: an extra label with `=` follows the jsonb containment
the planner uses for the GIN index, so the key must exist; every other
operator treats a missing label as `""`, as `coalesce` does; selector regexes
are anchored, line regexes are not; `|=` is a case-insensitive substring
search like `ILIKE`. Parser stages, label filters and aggregations need the
executor and are refused with a positioned error before the upgrade, so the
client reads a 400 rather than decoding a close frame.

The guarantee is a test, not a comment: `TestEvaluatorAgreesWithSQL` runs the
evaluator and `executor.Run` over the same seeded data for 32 queries and
requires identical answers. A change to either that the other does not follow
fails there.

Rejected:

- A second matcher written for tail. Re-deriving the semantics would let "what
  a query matches" and "what a tail matches" drift apart, and that divergence
  would be a real bug, not a cosmetic one.

### 4. A slow client loses records, counted, and never blocks delivery

Each subscription owns a bounded channel sized by `LOGAGG_HTTP_TAIL_BUFFER`.
The goroutine delivering a fan-out offers each match with a non-blocking send;
a full buffer drops the record and increments the client's counter and
`logagg_tail_records_dropped_total`. The next record the client does receive
carries how many it missed, so `logctl tail` can say so on stderr without a
second message type.

Each WebSocket write is bounded by the HTTP write timeout, and the library
closes the connection when that expires. A client that has stopped reading
therefore fills the kernel's buffers, stalls one write, and is cut, while its
registry buffer was already dropping. Writes and pings also end when the
subscription does, so a client stalled mid-write cannot hold shutdown for the
whole timeout; the read side is left alone so a healthy client still gets its
close frame.

The drop counter is deliberately not a reason under the shared
`records_dropped_total` family. That family answers "is this node losing
records", and a tail drop loses nothing: the record is durable and a query
finds it.

Rejected:

- A blocking send or an unbounded buffer per client. One stalled browser tab
  would back up the NATS subscription and, past the client library's pending
  limit, cost every tail on this node.
- Folding the drop counter into `records_dropped_total`. The loss panel would
  lie every time a browser tab stalled.

### 5. Heartbeats both ways

The server never expects a data message, so its read side goes to the
library's `CloseRead`, which answers the client's pings and close frames and
cancels the connection when the peer leaves. The server pings every
`LOGAGG_HTTP_TAIL_PING_INTERVAL` and treats a pong later than one interval as a
dead client. Thirty seconds keeps an idle tail alive through nginx's 300s
`proxy_read_timeout`, and notices a vanished laptop without waiting for TCP.
The library answers pings only while a read is in progress, so a client that
stops reading is disconnected by the heartbeat as well — which is correct,
and which is why the tests run a background reader the way a real client
does.

The library is `coder/websocket`; the plan left the choice between it and
`gorilla/websocket`. `CloseRead`, context-bounded writes and the per-write
deadline the paragraphs above rely on are what it provides directly, so the
heartbeat needs no pong handler or read-deadline plumbing of its own.

Authentication is the same bearer token as the query API, checked on the
handshake by the same middleware. Origin is left at the library's default,
same-origin only: the CLI sends none, and a page served from elsewhere has no
business holding this token.

Rejected:

- `gorilla/websocket`, the other candidate in the plan. The close and ping
  semantics this section depends on would have been hand-written on top of it
  rather than taken from the library.

## Consequences

- `internal/tail` is the only new package. `queue` grows `Fanout`,
  `Subscribe` and `Filter`; `httpapi` grows one route; `logctl` one command.
- A batch an agent resends after a lost ack is fanned out twice. Storage
  deduplicates it; tail does not. Live output may repeat a line the store
  holds once, which is accepted for a best-effort surface.
- `GET /v1/tail` is the first endpoint that hijacks the connection, so
  net/http's request timeouts no longer apply once upgraded. The per-write
  deadline and the heartbeat are what bound it instead, and the access-log
  wrapper exposes `Unwrap` so the upgrade can reach the `Hijacker`.
- Measured through a real broker on a laptop: about 2ms from gRPC send to
  tail delivery, against a one-second criterion; 200 batches of fifty 4KB
  records acked in about a second with a stalled client attached, slowest ack
  47ms, and the stalled client dropped with its misses counted.
