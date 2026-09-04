package model

import (
	"bytes"
	"maps"
	"testing"
	"time"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
)

func TestLabelSetProtoRoundTrip(t *testing.T) {
	t.Parallel()

	original := LabelSet{
		Service: "api",
		Host:    "node-1",
		Env:     "prod",
		Extra:   map[string]string{"region": "eu", "tier": "edge"},
	}

	back := LabelSetFromProto(original.Proto())
	if back.Service != original.Service || back.Host != original.Host || back.Env != original.Env {
		t.Fatalf("promoted labels changed: %s", back)
	}
	if !maps.Equal(back.Extra, original.Extra) {
		t.Fatalf("extra labels changed: %v", back.Extra)
	}
	// The ID surviving the round trip is the property that actually matters: it is
	// what ties a stream on the wire to a stream row in the database.
	if back.ID() != original.ID() {
		t.Fatalf("ID changed across the wire: %d != %d", back.ID(), original.ID())
	}
}

func TestLabelSetFromProtoDoesNotAliasTheMessage(t *testing.T) {
	t.Parallel()

	pb := &logaggv1.LabelSet{Service: "s", Host: "h", Env: "e", Extra: map[string]string{"k": "v"}}
	ls := LabelSetFromProto(pb)

	// gRPC may reuse or pool the message once the handler returns; the stream ID is
	// derived from this map, so aliasing it would let a later request change the ID
	// of an already-queued batch.
	pb.Extra["k"] = "mutated"
	if ls.Extra["k"] != "v" {
		t.Fatalf("model aliases the protobuf map: got %q", ls.Extra["k"])
	}
}

func TestLabelSetFromProtoHandlesNil(t *testing.T) {
	t.Parallel()

	ls := LabelSetFromProto(nil)
	if ls.Service != "" || ls.Extra != nil {
		t.Fatalf("expected the zero LabelSet, got %s", ls)
	}
	// The zero value must be rejected rather than silently written as a stream with
	// empty labels.
	if err := ls.Validate(); err == nil {
		t.Fatal("zero LabelSet should not validate")
	}
}

func TestLogRecordProtoRoundTrip(t *testing.T) {
	t.Parallel()

	original := LogRecord{
		StreamID: 99,
		Time:     time.Date(2026, 3, 14, 15, 9, 26, 535897932, time.UTC),
		Seq:      12345,
		Level:    LevelWarn,
		Message:  "disk almost full",
		TraceID:  bytes.Repeat([]byte{0xab}, TraceIDLen),
		SpanID:   bytes.Repeat([]byte{0xcd}, SpanIDLen),
		Fields:   map[string]string{"device": "/dev/sda1", "pct": "94"},
	}

	back := LogRecordFromProto(original.StreamID, original.Proto())

	if !back.Time.Equal(original.Time) {
		t.Fatalf("time = %s, want %s", back.Time, original.Time)
	}
	// Nanosecond precision must survive: seq alone does not order records within a
	// millisecond, and the dedup key includes the timestamp.
	if back.Time.Nanosecond() != original.Time.Nanosecond() {
		t.Fatalf("nanoseconds lost: %d != %d", back.Time.Nanosecond(), original.Time.Nanosecond())
	}
	if back.Seq != original.Seq || back.Level != original.Level || back.Message != original.Message {
		t.Fatalf("scalar fields changed: %+v", back)
	}
	if !bytes.Equal(back.TraceID, original.TraceID) || !bytes.Equal(back.SpanID, original.SpanID) {
		t.Fatalf("trace identifiers changed: %x / %x", back.TraceID, back.SpanID)
	}
	if !maps.Equal(back.Fields, original.Fields) {
		t.Fatalf("fields changed: %v", back.Fields)
	}
	if back.StreamID != original.StreamID {
		t.Fatalf("stream ID = %d, want %d", back.StreamID, original.StreamID)
	}
}

// TestLogRecordProtoUnsetTimeStaysZero pins the distinction between "the agent set
// no timestamp" and "the agent claims the Unix epoch". Conflating them would turn a
// missing timestamp into a 1970 row that Validate happily rejects for being outside
// the backfill window, with a misleading message.
func TestLogRecordProtoUnsetTimeStaysZero(t *testing.T) {
	t.Parallel()

	back := LogRecordFromProto(1, &logaggv1.LogRecord{Message: "no timestamp"})
	if !back.Time.IsZero() {
		t.Fatalf("expected the zero time, got %s", back.Time)
	}
	if err := back.Validate(fixedNow); err == nil {
		t.Fatal("a record with no timestamp should not validate")
	}
}

func TestLogRecordProtoOmitsEmptyOptionals(t *testing.T) {
	t.Parallel()

	rec := LogRecord{Time: fixedNow, Message: "plain"}
	pb := rec.Proto()

	if pb.GetTraceId() != nil || pb.GetSpanId() != nil || pb.GetFields() != nil {
		t.Fatalf("empty optionals should stay unset on the wire: %+v", pb)
	}
}

func TestLogRecordFromProtoHandlesNil(t *testing.T) {
	t.Parallel()

	rec := LogRecordFromProto(7, nil)
	if rec.StreamID != 7 {
		t.Fatalf("stream ID = %d, want 7", rec.StreamID)
	}
	if !rec.Time.IsZero() {
		t.Fatalf("expected the zero time, got %s", rec.Time)
	}
}

// TestProtoLevelEnumMatchesModel keeps the two enumerations in lockstep. They are
// declared in different files and the conversion is a plain numeric cast, so a value
// added to only one of them would silently mistranslate.
func TestProtoLevelEnumMatchesModel(t *testing.T) {
	t.Parallel()

	pairs := map[Level]logaggv1.Level{
		LevelUnspecified: logaggv1.Level_LEVEL_UNSPECIFIED,
		LevelTrace:       logaggv1.Level_LEVEL_TRACE,
		LevelDebug:       logaggv1.Level_LEVEL_DEBUG,
		LevelInfo:        logaggv1.Level_LEVEL_INFO,
		LevelWarn:        logaggv1.Level_LEVEL_WARN,
		LevelError:       logaggv1.Level_LEVEL_ERROR,
		LevelFatal:       logaggv1.Level_LEVEL_FATAL,
	}
	for modelLevel, wireLevel := range pairs {
		if int32(modelLevel) != int32(wireLevel) {
			t.Errorf("%s: model %d != wire %d", modelLevel, modelLevel, wireLevel)
		}
	}
	// And no value exists on the wire that the model cannot name.
	if len(pairs) != len(logaggv1.Level_name) {
		t.Errorf("wire enum has %d values, model covers %d", len(logaggv1.Level_name), len(pairs))
	}
}
