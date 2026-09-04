package model

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// fixedNow is an arbitrary fixed instant. Validation compares against a caller-
// supplied "now", so no test here depends on the wall clock.
var fixedNow = time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)

func validRecord() LogRecord {
	return LogRecord{
		StreamID: 42,
		Time:     fixedNow,
		Seq:      7,
		Level:    LevelInfo,
		Message:  "request completed",
		TraceID:  make([]byte, TraceIDLen),
		SpanID:   make([]byte, SpanIDLen),
		Fields:   map[string]string{"status": "200"},
	}
}

func TestLogRecordValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*LogRecord)
		wantErr error
	}{
		{name: "valid", mutate: func(*LogRecord) {}},
		{
			name:   "no optional fields",
			mutate: func(r *LogRecord) { r.TraceID, r.SpanID, r.Fields = nil, nil, nil },
		},
		{
			name:   "unspecified level is accepted",
			mutate: func(r *LogRecord) { r.Level = LevelUnspecified },
		},
		{
			name:   "empty message is accepted",
			mutate: func(r *LogRecord) { r.Message = "" },
		},
		{name: "zero time", mutate: func(r *LogRecord) { r.Time = time.Time{} }, wantErr: ErrMissingTime},
		{
			name:    "too far in the future",
			mutate:  func(r *LogRecord) { r.Time = fixedNow.Add(MaxClockSkewFuture + time.Minute) },
			wantErr: ErrTimeOutOfRange,
		},
		{
			name:   "inside the clock skew window",
			mutate: func(r *LogRecord) { r.Time = fixedNow.Add(MaxClockSkewFuture - time.Minute) },
		},
		{
			name:    "older than the backfill window",
			mutate:  func(r *LogRecord) { r.Time = fixedNow.Add(-MaxBackfill - time.Hour) },
			wantErr: ErrTimeOutOfRange,
		},
		{
			name:   "inside the backfill window",
			mutate: func(r *LogRecord) { r.Time = fixedNow.Add(-MaxBackfill + time.Hour) },
		},
		{name: "negative seq", mutate: func(r *LogRecord) { r.Seq = -1 }, wantErr: ErrNegativeSeq},
		{name: "unknown level", mutate: func(r *LogRecord) { r.Level = 99 }, wantErr: ErrInvalidLevel},
		{
			name:    "oversized message",
			mutate:  func(r *LogRecord) { r.Message = strings.Repeat("x", MaxMessageLen+1) },
			wantErr: ErrMessageTooLong,
		},
		{
			name:   "message exactly at the limit",
			mutate: func(r *LogRecord) { r.Message = strings.Repeat("x", MaxMessageLen) },
		},
		{
			name:    "short trace ID",
			mutate:  func(r *LogRecord) { r.TraceID = make([]byte, TraceIDLen-1) },
			wantErr: ErrBadTraceID,
		},
		{
			name:    "long span ID",
			mutate:  func(r *LogRecord) { r.SpanID = make([]byte, SpanIDLen+1) },
			wantErr: ErrBadSpanID,
		},
		{
			name: "too many fields",
			mutate: func(r *LogRecord) {
				r.Fields = make(map[string]string, MaxFields+1)
				for i := 0; i <= MaxFields; i++ {
					r.Fields[strings.Repeat("k", i+1)] = "v"
				}
			},
			wantErr: ErrTooManyFields,
		},
		{
			name:    "oversized field value",
			mutate:  func(r *LogRecord) { r.Fields["status"] = strings.Repeat("v", MaxFieldValueLen+1) },
			wantErr: ErrFieldTooLong,
		},
		{
			name:    "oversized field name",
			mutate:  func(r *LogRecord) { r.Fields[strings.Repeat("n", MaxFieldNameLen+1)] = "v" },
			wantErr: ErrFieldTooLong,
		},
		{
			name:    "empty field name",
			mutate:  func(r *LogRecord) { r.Fields[""] = "v" },
			wantErr: ErrMissingLabel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := validRecord()
			tc.mutate(&rec)

			err := rec.Validate(fixedNow)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != nil && err == nil:
				t.Fatalf("expected error %v, got nil", tc.wantErr)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

// TestLogRecordValidateAllowsUnusualButLegalFieldNames documents the deliberate
// asymmetry with labels: field names are free-form because they come from parsing
// arbitrary log lines, while label names must be DSL identifiers because a label
// that cannot be selected is a label that cannot be used.
func TestLogRecordValidateAllowsUnusualButLegalFieldNames(t *testing.T) {
	t.Parallel()

	rec := validRecord()
	rec.Fields = map[string]string{"http.status_code": "200", "user-agent": "curl/8", "1st": "x"}
	if err := rec.Validate(fixedNow); err != nil {
		t.Fatalf("free-form field names should be accepted: %v", err)
	}
}

func TestNewStreamDerivesID(t *testing.T) {
	t.Parallel()

	labels := LabelSet{Service: "api", Host: "h", Env: "prod"}
	stream := NewStream(labels, fixedNow)

	if stream.ID != labels.ID() {
		t.Fatalf("ID = %d, want %d", stream.ID, labels.ID())
	}
	if !stream.FirstSeen.Equal(fixedNow) || !stream.LastSeen.Equal(fixedNow) {
		t.Fatalf("timestamps = %s / %s, want both %s", stream.FirstSeen, stream.LastSeen, fixedNow)
	}
}

func BenchmarkLogRecordValidate(b *testing.B) {
	rec := validRecord()
	for b.Loop() {
		if err := rec.Validate(fixedNow); err != nil {
			b.Fatal(err)
		}
	}
}
