// Package agent is the log shipper: it reads lines from local sources, turns
// them into records, and hands them to the collector over the ingest stream.
//
// The package is organized around one interface, Source, and one pipeline. A
// Source owns a goroutine and pushes Lines into a bounded channel; downstream
// stages (multiline joining, field extraction, batching, the spool) consume from
// that channel and eventually call internal/ingest.Client. The agent does not
// speak gRPC itself — the wire protocol lives in internal/ingest and is shared
// with logctl and loadgen so there is exactly one client.
//
// Two properties shape everything here.
//
// The first is that a source's progress is only durable once the collector has
// acknowledged it. A line that has been read but not acked is not safe to forget,
// which is why Source deliberately knows nothing about acknowledgement: offsets
// are advanced by the checkpoint store, driven by acks, not by reads. A source
// that tracked its own durability would be a second, disagreeing checkpoint
// store.
//
// The second is that the agent's only unbounded resource is the file it is
// reading, and that resource is already durable on disk. So backpressure here
// runs the opposite way to the collector's: where internal/ingest sheds load
// because there is a waiting client to say "slow down" to, the agent blocks —
// a full channel simply stops the reader, and the file keeps the data until the
// reader comes back. Records are dropped only where the bound is genuinely
// hard: an over-long line, or a full on-disk spool.
package agent
