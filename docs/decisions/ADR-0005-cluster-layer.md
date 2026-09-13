# ADR-0005: Cluster layer

**Status:** accepted (phase 5)

## Context

Several collectors run against one TimescaleDB and one NATS JetStream. Any of
them can accept writes, because a write is a publish to JetStream and the
writer pool drains it; the queue is what makes a killed collector lose nothing
that was acknowledged. What the collectors did not have was any knowledge of
each other: a query ran wherever it landed, and there was no way to divide
work or to show an operator who is in the cluster.

## Decisions

### Membership is gossip; the ring is derived

`hashicorp/memberlist` provides membership and failure detection. Each node
gossips a small JSON metadata record: its peer and HTTP ports, build version,
configured virtual-node count and readiness. Ring tokens are not gossiped.
They derive from the node name and the agreed virtual-node count, so every
member computes the same ring from names alone and stays within memberlist's
512-byte metadata limit; a member that disagrees on the count is logged as an
error, since two rings would disagree on owners.

Membership is maintained only from memberlist's event callbacks. memberlist
holds its node lock while it notifies, so asking it for its member list inside
a callback would deadlock; the event carries everything needed. Every join,
leave or update rebuilds the ring as a new immutable value, logs each
ownership transfer with its before and after range, and updates
`logagg_cluster_members` and `logagg_ring_ownership_changes_total`.

Startup joins the seed peers with backoff for a bounded time and then carries
on alone. A partition at startup is not a reason to refuse to serve, and the
compose service name resolves to every replica, so the node is found as soon
as any peer joins through it. Shutdown withdraws readiness and leaves before
draining, so peers stop routing to a node before it stops answering.

### The ring divides scans, not data

Every collector writes to the same database, so stream ownership is not
about where data lives. It divides the read: a coordinator resolves the
stream set once, gives each owner its share of the ids, and each owner scans
only those. The ring routes query fan-out and, in phase 6, tail
subscriptions. Writes never consult it. Saying this plainly is better than
implying ownership does more than it does.

### Ship the query, not the SQL

The roadmap said to plan once and ship the plan. The first version did: the
coordinator sent each peer its planned logs statement with the ids bound, and
the peer checked the text against `query.CheckSQL`, the planner's identifier
allow-list, before running it. Review showed why that is the wrong depth. An
allow-list bounds vocabulary, not meaning: `SELECT ... FROM logs` with no
`WHERE` and no `LIMIT`, or a self cross join, is made entirely of words the
planner writes. A peer port would have been an execute-SQL endpoint bounded
by a word list, and in the compose stack that port is plaintext on the whole
compose network.

So the coordinator ships the query text and the request half of the plan
(range, over-fetched limit, direction) with the peer's share of the ids, and
the peer runs the same compiler over the same inputs. The only statements a
peer executes are ones its own planner wrote, with the time bound and row cap
the planner always injects; the network cannot make it run anything the
language cannot mean. The parse-once benefit is kept in the sense that
matters: the coordinator already rejected anything malformed, so a peer parse
error can only come from a version skew and surfaces as a shard warning.
`query.CheckSQL` stays exported for the unit tests and the fuzzer.

The listener is mTLS or loopback unless the operator sets
`LOGAGG_CLUSTER_PEER_ALLOW_PLAINTEXT`, the same rule ingest applies; even
then it is a query endpoint, not a SQL one. One key pair serves both
directions, so one CA signs every collector. Configuration refuses a loopback
peer port behind a routable gossip address, since peers dial the gossip host
with the peer port and would find nothing there.

### Merge and degrade

Records from the shards are k-way merged with a heap in the request's
direction and cut at the limit. Points are summed per bucket and group, since
every aggregation is a count or a sum, then ordered as a single node would
order them: the planner sorts text group columns in the "C" collation and the
merge compares bytewise, and both order level by severity, so the order does
not depend on the database's default collation. A peer that has just died is
still ready in gossip for a few seconds; the client gives up on a connection
in three, well inside the query budget, so the shard degrades to a warning
instead of timing out the query. A shard whose owner is unreachable becomes a warning naming the
member and the number of streams not searched, and the rest of the result
stands. Streams whose owner is not ready, or not in the ring, are scanned
locally without a warning; the database is shared, so the coordinator can
answer for a member that is starting or stopping.

Each shard applies the row cap itself, so a shard at the cap marks the merged
result truncated even before the final cut. For a series this can undercount
a bucket that one shard cut and another kept. It is the known ceiling of
applying the limit per shard, and the truncated flag is the signal that it
may have happened.

### A proxy, not port ranges

Compose cannot publish one host port for several replicas. The scaled stack
puts nginx on the single collector's 8080 and 9095: the API proxied per
request so coordination spreads across collectors, ingest as a layer-4 stream
so gRPC frames pass untouched. nginx resolves the service name through
Compose's DNS on every connection, so a killed replica leaves rotation within
seconds. The proxy is development plumbing; a real deployment terminates TLS
at the collectors.

## Consequences

- Peers must run the same compiler version as the coordinator, or a query
  the coordinator accepts may be a shard warning on a peer. Rolling upgrades
  degrade rather than fail, and the warning names the member.
- Shard replies are capped at the row cap times the largest record; a reply
  past the cap is a warning, not silent truncation.
- The chaos test (`make chaos`) is the phase's claim made executable: five
  collectors, two killed mid-ingest, zero gaps in the delivered sequence and a
  ring rebalanced onto the survivors. It waits out `ack_wait`, because a
  killed collector's unacked deliveries sit in JetStream until then; that
  wait is the observable recovery time after a kill and is bounded by
  configuration, not by anything the agent does.
- Per-shard limits and the shared database mean fan-out is a parallelism and
  resilience demonstration more than a necessity today. Phase 8 measures
  whether dividing the scan pays for the extra round trip.
