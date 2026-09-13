// Package queue is the boundary between the collector and NATS JetStream.
//
// The queue exists to decouple ingest from storage. An agent's batch is durable
// once JetStream has acknowledged it, which is what lets the ingest handler ack
// the agent before the record has reached TimescaleDB, and what lets a database
// outage grow a disk-backed backlog instead of process memory.
//
// Everything in this package is expressed in terms of the Publisher and Consumer
// interfaces. The concrete NATS implementation is the only thing in the tree that
// imports nats.go, so the ingest path can be exercised without a broker: a test
// substitutes an in-memory Publisher (see internal/queue/queuetest) and still
// covers the ack-after-publish ordering that is the whole correctness argument.
package queue

import (
	"context"
	"strings"
)

// Publisher writes records into the queue.
//
// Publish must not return nil until the message is durable: the caller's next
// action is to acknowledge an agent, and an ack for a message that is still in
// flight is exactly the lie this design exists to avoid.
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	// Subject renders the subject a label set publishes to. On the Publisher
	// because the prefix is connection configuration, not caller knowledge.
	Subject(env, service string) string
	// Fanout copies an accepted batch to live-tail subscribers. Fire-and-forget
	// on purpose: no JetStream ack, no disk, so a tail reader can never slow
	// the durable path down. An error means the copy was lost, not the batch,
	// which is already durable by the time this is called.
	Fanout(subject string, payload []byte) error
	// TailSubject renders the fan-out subject for a label set.
	TailSubject(env, service string) string
}

// Subject-token limits.
const (
	// MaxSubjectTokenLen truncates one token of a subject. Label values are bounded
	// at 1KB, and a subject built from two of them would otherwise be a 2KB routing
	// key repeated in every message header.
	MaxSubjectTokenLen = 64
	// emptyToken stands in for a label that sanitizes away to nothing. A subject
	// with an empty token is not addressable, so it has to be something.
	emptyToken = "_"
)

// Subject builds prefix.env.service.
//
// Records are partitioned by env and service because those are the two labels
// every query filters on, so a future consumer can subscribe to a slice of
// traffic — one environment, one noisy service — without filtering client-side.
// Host is deliberately not a token: it would multiply subject cardinality by the
// fleet size for no query benefit.
//
// Exported as a function as well as a method so a test double renders subjects
// through exactly this code rather than reimplementing the scheme.
func Subject(prefix, env, service string) string {
	var b strings.Builder
	b.Grow(len(prefix) + 2*MaxSubjectTokenLen + 2)
	b.WriteString(prefix)
	b.WriteByte('.')
	b.WriteString(sanitizeToken(env))
	b.WriteByte('.')
	b.WriteString(sanitizeToken(service))
	return b.String()
}

// sanitizeToken makes an arbitrary label value safe to use as one subject token.
//
// This is required, not defensive: label values are attacker-controlled and only
// length-validated, while a NATS subject token may not contain a dot, a space or
// either wildcard. A service literally named ">" would otherwise publish to a
// wildcard subject, and one named "a.b" would silently add a token and land
// outside the stream's configured subject filter.
//
// The mapping is lossy on purpose — "a.b" and "a_b" share a subject — because the
// subject is routing only. Stream identity is the fingerprint in the payload, so
// two services sharing a subject stay two streams in storage.
func sanitizeToken(s string) string {
	if len(s) > MaxSubjectTokenLen {
		s = s[:MaxSubjectTokenLen]
	}

	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_':
			b.WriteByte(c)
		default:
			// Anything else — dots, spaces, wildcards, multi-byte UTF-8, control
			// bytes — collapses to an underscore. Replacing per byte rather than per
			// rune keeps a truncated multi-byte sequence from producing invalid UTF-8.
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return emptyToken
	}
	return b.String()
}
