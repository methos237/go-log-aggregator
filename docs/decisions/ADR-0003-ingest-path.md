# ADR-0003: Ingest path

- **Status:** Accepted
- **Date:** 2026-09-08
- **Supersedes:** none
- **Refines:** ADR-0001 (decision 1: ingest and delivery semantics), ADR-0002 (the
  write path this feeds)

## Context

Phase 2 adds the half of the system that faces the outside world: a bidirectional gRPC
service agents stream batches into, a NATS JetStream stream those batches land in, and
a consumer that hands them to the writer pool built in phase 2's predecessor.

Three constraints shape every decision below.

- **Delivery is at-least-once, and the ack is the contract.** An acknowledgement tells
  an agent "this batch is durable, you may drop it from your spool". Anything that
  makes an ack precede durability turns a crash into silent loss, and nothing
  downstream can detect it.
- **This is the first untrusted boundary.** Batch size, label values, record contents
  and connection count are all controlled by whoever can reach the port. Every limit
  has to be explicit and every label value has to be treated as hostile.
- **No queue may be unbounded.** ADR-0001 makes JetStream the buffer, not process
  memory. A slow database must grow disk on the broker, never the heap here.

## Decision

### 1. The ack is sent after the JetStream publish is acknowledged, never on receipt

The handler receives a batch, validates its labels, fingerprints them, filters invalid
records, publishes, waits for the broker's acknowledgement, and only then sends the
`Ack`. Everything that can fail sits in front of the ack.

The rejected alternative is acking on receipt and publishing asynchronously, which
roughly doubles achievable throughput because the agent's window never waits on the
broker. It also means a collector that dies with batches in memory loses records that
every agent has already forgotten. That is the exact failure this project exists to
demonstrate an answer to, so the throughput is not available for trade.

A consequence worth stating: the ack's latency is the publish's latency, so the
JetStream publish timeout is also the ceiling on how long a batch can hold a gRPC
handler.

### 2. Batches on one stream are handled strictly in order

One goroutine per stream, no per-batch fan-out. Agents assign monotonic per-stream
sequence numbers and treat an ack as permission to forget everything up to it, so
handling batch N+1 before batch N would let a crash lose N while N+1 was already
acknowledged. Concurrency comes from many streams and from the publisher pool, not
from reordering within a stream.

### 3. A bounded intake channel, and a full one sheds rather than waits

Between the handlers and the queue sits one bounded channel drained by a pool of
publishers. Handlers hand over a job and wait on a per-job reply channel, so the buffer
decouples *who publishes* from *who received* without decoupling the ack from
durability. A fire-and-forget queue here would be simpler and would quietly break
decision 1.

Enqueueing uses `select` with `default`: no timeout, no wait. An agent told to slow down
is better off than one blocked on an ack that may never come, and this is the load
shedding point in ADR-0001's backpressure chain — the last place in the system where
there is still an unacknowledged client to say "slow down" to. The writer, by contrast,
blocks instead of shedding, because by then the records are already durable and the
queue depth is the signal an operator alerts on.

A shed batch answers `ACK_CODE_OVERLOADED` **in band** rather than failing the RPC with
`RESOURCE_EXHAUSTED`. Both mean "retry later", but a status code ends the whole stream
and penalises every other batch on it for one busy moment. gRPC still returns
`RESOURCE_EXHAUSTED` itself for the transport-level case, an agent exceeding
`MaxRecvMsgSize`.

### 4. Invalid records are filtered at ingest, and the surviving records are re-encoded

A record that fails validation is dropped before publishing, and the batch is
re-marshaled with only the survivors. Forwarding the received bytes unchanged would be
cheaper, and would let one oversized message fail the writer's `COPY`, fail every retry
of it, and be redelivered forever — the classic poison message, blocking a stream
indefinitely.

Records are converted to the model purely to validate them here, and converted again by
the consumer after decoding. That duplicated conversion is accepted deliberately:
validation lives in exactly one place, so the agent, the load generator and this handler
cannot disagree about what a valid record is.

### 5. The stream is work-queue retention with DiscardNew

`WorkQueuePolicy`: a record leaves the stream once the writer acks it. The queue is a
buffer and TimescaleDB is the archive, so keeping acked records would spend disk holding
data that is already durable elsewhere. Replicas still scale out by binding to one
shared durable consumer, which work-queue retention permits.

`DiscardNew`: a full stream rejects publishes and keeps what is queued. `DiscardOld`
would bound disk by discarding batches agents already consider delivered — data loss
dressed as a limit. Rejecting propagates pressure up the chain instead: ingest answers
OVERLOADED, the agent backs off and spools.

Classifying a refused publish takes care: `jetstream.ErrMaxBytesExceeded` carries no
`APIError`, so `errors.Is` against it never matches what a rejected publish actually
returns, and the classification degrades silently to "unavailable" — reporting a
collector fault for the one condition this design exists to signal. The code matches the
API error code instead (`err_code=10077`), verified against a real broker by an
integration test that fails against the sentinel version.

The ingest message ceiling is checked against the broker's advertised `max_payload` at
startup, and defaults to the broker's own default rather than gRPC's, because a ceiling
above it means a valid batch is accepted, validated, re-marshaled and then permanently
dropped as unsendable — after the agent has been told nothing.

No deduplication window on the stream. Publish-side dedup cannot cover a redelivery to
the writer, so deduplication stays where it can be complete: the `logs_dedup` index
(ADR-0002, decision 2).

The client-side reconnect buffer is disabled. nats.go by default buffers publishes made
while disconnected and flushes them on reconnect, which is an unbounded in-memory queue
in the one place this design refuses to have one. With it off, a publish during an
outage fails immediately, ingest refuses the batch, and the agent's disk spool becomes
the buffer — the only component that can afford to lose nothing.

### 6. Every delivery ends in exactly one of ack, nak or term

- **Ack** is deferred to the writer's callback, firing once rows are in the hypertable.
  This is what makes a crash between write and ack a redelivery rather than a loss.
- **Nak** covers a refusal about this moment rather than about the batch: a full writer
  queue, a shutdown, a failed write. The batch returns to the stream.
- **Term** covers a batch that will fail identically forever: a payload that does not
  decode, or labels that do not validate. Naking those would spend the consumer's ack
  budget on messages that can never leave the stream, so one corrupt message would
  stall every healthy batch behind it. Each Term is counted, because it is data leaving
  the system.

One case is easy to miss. `Writer.Submit` only invokes the ack callback when it accepted
at least one record, so a batch it accepted nothing from would sit unacked until
`AckWait` expired and then be redelivered forever. That case is acked: nothing is
durable, but nothing ever will be.

`MaxDeliver` is unlimited. A batch reaching the consumer was already validated at
ingest, so repeated failure means the database is unreachable or the schema is wrong —
conditions that get fixed, after which the backlog should still exist. A delivery
ceiling would discard records during exactly the outage the queue exists to survive.

`AckWait` must exceed the writer's worst case (`WriteTimeout` × `MaxAttempts` plus
backoff), or a batch that is merely slow gets redelivered while it is still being
written. Configuration validation enforces that relationship rather than leaving it to
be discovered as unexplained duplicate rows.

### 7. mTLS, or plaintext, but never one-way TLS

Ingest speaks plaintext when no TLS files are configured, because `make dev` has to work
with no setup and an empty configuration says exactly that. Configuring a server
certificate and key *without* a client CA is refused at startup, in both configuration
validation and the server itself: one-way TLS would encrypt the connection while letting
anyone who can reach the port write logs into the cluster, which is worse than plaintext
because it looks secure.

Client certificates are verified against a dedicated pool, not the system roots — with
the system pool, anything signed by any public CA would authenticate as an agent.
Verification is `RequireAndVerifyClientCert`, not `VerifyIfGiven`, since the weaker mode
accepts a peer presenting nothing at all. TLS 1.3 only: every client is a Go binary from
this repository, so there is no legacy peer to accommodate.

`make certs` generates development material into a gitignored directory. It is
documented as dev-only in the README: self-signed by a CA whose key sits in the working
tree, one year, no revocation.

Plaintext is still the default, so the default listen address is loopback —
deliberately unlike the HTTP and admin listeners, which serve reads. Serving ingest
without TLS on a routable address requires `LOGAGG_INGEST_ALLOW_PLAINTEXT=true`, which
a container legitimately needs (inside one, the listener must bind every interface for
the runtime to forward to it) and which nothing else should set casually. That
combination also logs at WARN rather than INFO, because it means anything able to reach
the port can write records.

**Known limitation: ingest is a single trust domain.** A verified client certificate
authorises writing *anything*, not writing as a particular service. The handler takes
`service`, `host` and `env` from the batch and never consults the peer certificate, so
any holder of a certificate signed by the configured CA can attribute records to any
service — which matters for a store whose contents are used as evidence, and is made
starker by `make certs` minting one shared agent certificate for a whole fleet. Binding
the authenticated identity to the claimed labels (rejecting a mismatched `host`, or
recording the verified identity in a column agents cannot set) needs per-agent
certificates and a rotation story, so it is deferred rather than half-done. Stated here
because an undocumented gap is worse than a documented one.

### 8. Subject scheme is `prefix.env.service`, with label values sanitized per byte

Those two labels are what every query filters on, so a consumer can subscribe to one
environment or one noisy service without filtering client-side. Host is deliberately not
a token: it would multiply subject cardinality by the fleet size for no query benefit.

Sanitizing is required, not defensive. Label values are attacker-controlled and only
length-validated, while a NATS subject token may not contain a dot, a space or either
wildcard. A service named `>` would otherwise publish to a wildcard subject, and one
named `a.b` would add a token and land outside the stream's filter. The mapping is lossy
on purpose — subjects are routing only, and stream identity is the fingerprint in the
payload.

### 9. Shutdown order is fixed, and the queue connection closes last

Deregister readiness checks, stop the ingest listener, stop the consumer (waiting for
handlers mid-Submit), stop the public API, drain the writer, tear down admin. Unready
then drain produces no client-visible errors; drain then unready produces a burst of
them. The queue connection closes after all of it, because draining the writer is what
fires the deferred acks and they travel on that connection.

Every step shares one deadline, and exceeding it is tolerable for exactly one reason:
whatever did not finish was never acknowledged, so the queue redelivers it. The shutdown
is allowed to be imperfect because the delivery contract is not.

## Consequences

- Ingest throughput is bounded by the JetStream publish round trip, by construction.
  Measured on a laptop against the dev stack: ~200k records/sec accepted with four
  senders and 500-record batches. Phase 8 profiles this properly.
- A collector killed mid-stream loses no acknowledged record, at the cost of duplicate
  rows that the dedup index absorbs. The integration suite asserts both.
- Records are converted between wire and model form twice per batch. Phase 8 may revisit
  this; correctness of a single validation path came first.
- Overload is visible as `ACK_CODE_OVERLOADED` acks and
  `logagg_records_dropped_total{component="ingest",reason="ingest_buffer_full"}` rather
  than as growing memory. An operator watching the wrong one of those sees nothing.
- The no-unbuffered-data-channel rule is enforced by `make lint-arch` (ast-grep), since
  the bounded-queue property is an invariant of this design rather than something a Go
  linter knows about.
