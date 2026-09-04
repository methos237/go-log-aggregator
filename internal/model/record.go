// Package model holds the canonical in-process representation of a log record
// and its labels.
//
// This package is the boundary between the wire format (protobuf, see
// api/proto/logagg/v1) and storage (see internal/storage). It deliberately
// depends on neither pgx nor gRPC, only on the generated protobuf types, so that
// the shape of a record is defined in exactly one place and both edges convert
// to it rather than to each other.
//
// Everything here is validated before it reaches storage. Validation lives with
// the model rather than in the ingest handler so that the writer, the loadgen and
// any future receiver all enforce the same limits.
package model

import (
	"errors"
	"fmt"
	"time"
)

// Validation failures. All of these mean "the producer sent something wrong",
// which the ingest path reports as a non-retryable rejection: resending the same
// bytes would fail identically.
var (
	ErrInvalidLevel     = errors.New("unknown log level")
	ErrMissingLabel     = errors.New("label must not be empty")
	ErrReservedLabel    = errors.New("label name is reserved")
	ErrInvalidLabelName = errors.New("label name must match [a-zA-Z_][a-zA-Z0-9_]*")
	ErrLabelTooLong     = errors.New("label exceeds maximum length")
	ErrTooManyLabels    = errors.New("too many labels")
	ErrMissingTime      = errors.New("record timestamp must be set")
	ErrTimeOutOfRange   = errors.New("record timestamp is outside the accepted window")
	ErrMessageTooLong   = errors.New("message exceeds maximum length")
	ErrTooManyFields    = errors.New("too many structured fields")
	ErrFieldTooLong     = errors.New("structured field exceeds maximum length")
	ErrBadTraceID       = errors.New("trace ID must be 16 bytes")
	ErrBadSpanID        = errors.New("span ID must be 8 bytes")
	ErrNegativeSeq      = errors.New("sequence number must not be negative")
)

// Record-shaped limits, per §8 of the roadmap.
const (
	// MaxMessageLen bounds a single log line. Larger lines are almost always a
	// binary blob or a runaway stack trace, and TOAST-ing multi-megabyte values
	// into the hot table destroys scan performance for every other query.
	MaxMessageLen = 64 << 10
	// MaxFields caps extracted structured fields per record. Unlike labels these
	// do not multiply stream cardinality, so the limit is looser.
	MaxFields = 64
	// MaxFieldNameLen bounds a field name.
	MaxFieldNameLen = 128
	// MaxFieldValueLen bounds a field value.
	MaxFieldValueLen = 8 << 10
	// TraceIDLen and SpanIDLen are fixed by the W3C trace context spec.
	TraceIDLen = 16
	SpanIDLen  = 8
)

// Timestamp acceptance window. Records outside it are rejected rather than
// clamped: silently rewriting a timestamp makes the data lie, and a clock that is
// years off is a configuration bug the operator needs to see. The window also
// bounds how many hypertable chunks one bad agent can create.
const (
	// MaxClockSkewFuture allows for modest clock skew between agent and collector.
	MaxClockSkewFuture = 15 * time.Minute
	// MaxBackfill bounds how old a record may be. Wider than any realistic spool
	// replay, narrower than "any timestamp at all".
	MaxBackfill = 7 * 24 * time.Hour
)

// LogRecord is one log line, resolved against its stream.
//
// StreamID is denormalized onto the record rather than carried alongside it
// because the writer batches records from many streams into a single CopyFrom and
// needs the ID per row.
type LogRecord struct {
	StreamID StreamID
	// Time is when the record was produced, as observed by the agent. It is the
	// hypertable partitioning column, so it is never nil and never zero.
	Time time.Time
	// Seq is a monotonic per-stream counter from the agent. (StreamID, Seq, Time)
	// is the storage dedup key: it is what makes replaying an unacknowledged batch
	// safe instead of duplicating rows.
	Seq     int64
	Level   Level
	Message string
	// TraceID is 16 bytes or nil; SpanID is 8 bytes or nil.
	TraceID []byte
	SpanID  []byte
	// Fields are extracted structured values, nil when the record had none.
	Fields map[string]string
}

// Validate reports the first problem with the record, judged against now.
//
// now is a parameter rather than a call to time.Now so the timestamp window is
// testable without freezing the clock globally.
func (r *LogRecord) Validate(now time.Time) error {
	if r.Time.IsZero() {
		return ErrMissingTime
	}
	if r.Time.After(now.Add(MaxClockSkewFuture)) {
		return fmt.Errorf("%w: %s is more than %s in the future", ErrTimeOutOfRange, r.Time.UTC(), MaxClockSkewFuture)
	}
	if r.Time.Before(now.Add(-MaxBackfill)) {
		return fmt.Errorf("%w: %s is more than %s old", ErrTimeOutOfRange, r.Time.UTC(), MaxBackfill)
	}
	if r.Seq < 0 {
		return fmt.Errorf("%w: %d", ErrNegativeSeq, r.Seq)
	}
	if !r.Level.Valid() {
		return fmt.Errorf("%w: %d", ErrInvalidLevel, int16(r.Level))
	}
	if len(r.Message) > MaxMessageLen {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrMessageTooLong, len(r.Message), MaxMessageLen)
	}
	if n := len(r.TraceID); n != 0 && n != TraceIDLen {
		return fmt.Errorf("%w: got %d", ErrBadTraceID, n)
	}
	if n := len(r.SpanID); n != 0 && n != SpanIDLen {
		return fmt.Errorf("%w: got %d", ErrBadSpanID, n)
	}
	if len(r.Fields) > MaxFields {
		return fmt.Errorf("%w: %d fields exceeds %d", ErrTooManyFields, len(r.Fields), MaxFields)
	}
	for name, value := range r.Fields {
		if err := validateFieldName(name); err != nil {
			return err
		}
		if len(value) > MaxFieldValueLen {
			return fmt.Errorf("field %q: %w: %d bytes exceeds %d", name, ErrFieldTooLong, len(value), MaxFieldValueLen)
		}
	}
	return nil
}

func validateFieldName(name string) error {
	if name == "" {
		return fmt.Errorf("field name: %w", ErrMissingLabel)
	}
	if len(name) > MaxFieldNameLen {
		return fmt.Errorf("field name %q: %w: %d bytes exceeds %d", name, ErrFieldTooLong, len(name), MaxFieldNameLen)
	}
	return nil
}

// Stream is a label set as it is stored: the dimension table row that every
// record's stream_id points at.
type Stream struct {
	ID     StreamID
	Labels LabelSet
	// FirstSeen and LastSeen are maintained by the writer's upsert. LastSeen is
	// what makes /v1/labels able to hide streams that stopped reporting.
	FirstSeen time.Time
	LastSeen  time.Time
}

// NewStream derives a stream from its labels, stamping both timestamps with seen.
func NewStream(labels LabelSet, seen time.Time) Stream {
	return Stream{
		ID:        labels.ID(),
		Labels:    labels,
		FirstSeen: seen,
		LastSeen:  seen,
	}
}
