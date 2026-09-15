package storage

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// stubPool satisfies NewWriter's nil check for tests that never reach the database.
// Any call on it would panic, which is the point: nothing in this file may touch
// Postgres.
var stubPool pgxpool.Pool

// These tests cover the writer's decision logic without a database. The parts that
// need real Postgres -- COPY, the staging insert, dedup on replay -- live in
// test/integration, because faking them would only test the fake.

func testWriterConfig() config.Writer {
	return config.Writer{
		Workers:               2,
		BatchSize:             4,
		FlushInterval:         50 * time.Millisecond,
		QueueDepth:            8,
		WriteTimeout:          time.Second,
		MaxAttempts:           3,
		RetryBaseDelay:        time.Millisecond,
		RetryMaxDelay:         10 * time.Millisecond,
		StreamCacheSize:       16,
		StreamRefreshInterval: time.Minute,
	}
}

func TestNewWriterRejectsBadInput(t *testing.T) {
	t.Parallel()

	if _, err := NewWriter(nil, testWriterConfig(), nil, nil); err == nil {
		t.Error("expected an error for a nil pool")
	}

	// A zero config would otherwise produce a writer with no workers, whose Submit
	// blocks forever -- the worst possible failure mode for a write path.
	if _, err := NewWriter(&stubPool, config.Writer{}, nil, nil); err == nil {
		t.Error("expected an error for an invalid config")
	}
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "connection failure", err: &pgconn.PgError{Code: "08006"}, want: true},
		{name: "deadlock", err: &pgconn.PgError{Code: "40P01"}, want: true},
		{name: "serialization failure", err: &pgconn.PgError{Code: "40001"}, want: true},
		{name: "too many connections", err: &pgconn.PgError{Code: "53300"}, want: true},
		{name: "admin shutdown", err: &pgconn.PgError{Code: "57P01"}, want: true},
		{name: "io error", err: &pgconn.PgError{Code: "58030"}, want: true},

		// Bad data or bad SQL: retrying burns attempts and never succeeds.
		{name: "unique violation", err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "foreign key violation", err: &pgconn.PgError{Code: "23503"}, want: false},
		{name: "not null violation", err: &pgconn.PgError{Code: "23502"}, want: false},
		{name: "string too long", err: &pgconn.PgError{Code: "22001"}, want: false},
		{name: "undefined table", err: &pgconn.PgError{Code: "42P01"}, want: false},
		{name: "malformed code", err: &pgconn.PgError{Code: "X"}, want: false},

		// Shutdown or a blown deadline: the caller has already stopped waiting.
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: false},

		// Anything unclassified defaults to retrying, because the unclassifiable
		// errors on this path are network and pool errors.
		{name: "network error", err: &net.OpError{Op: "dial", Err: errors.New("refused")}, want: true},
		{name: "unknown error", err: errors.New("something odd"), want: true},

		// Wrapping must not change the verdict: every error from writeOnce is wrapped
		// with context before it reaches the retry decision.
		{
			name: "wrapped connection failure",
			err:  errors.Join(errors.New("copy into staging"), &pgconn.PgError{Code: "08003"}),
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetryable(tc.err); got != tc.want {
				t.Fatalf("isRetryable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestCopyRowMatchesColumnOrder(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)
	rec := model.LogRecord{
		StreamID: -42, // negative is legal: stream IDs are reinterpreted hash bits
		Time:     at,
		Seq:      9,
		Level:    model.LevelError,
		Message:  "boom",
		TraceID:  []byte("0123456789abcdef"),
		SpanID:   []byte("01234567"),
		Fields:   map[string]string{"b": "2", "a": "1"},
	}

	var cr copyRow
	row := cr.fill(&rec)
	if len(row) != len(logColumns) {
		t.Fatalf("copyRow returned %d values for %d columns", len(row), len(logColumns))
	}

	// Values travel as pointers so nothing is boxed; deref to compare.
	want := []any{at, int64(-42), int64(9), int16(model.LevelError), "boom"}
	for i, w := range want {
		if got := reflect.ValueOf(row[i]).Elem().Interface(); got != w {
			t.Errorf("column %s = %#v, want %#v", logColumns[i], got, w)
		}
	}
	if got := string(*row[5].(*[]byte)); got != "0123456789abcdef" {
		t.Errorf("trace_id = %q", got)
	}
	if got := string(*row[6].(*[]byte)); got != "01234567" {
		t.Errorf("span_id = %q", got)
	}
	// encoding/json sorts map keys, so the JSONB bytes are stable across runs.
	if got := string(*row[7].(*[]byte)); got != `{"a":"1","b":"2"}` {
		t.Errorf("fields = %s", got)
	}

	// The buffer is reused across rows, so a second record must not see the first
	// one's optional columns.
	plain := model.LogRecord{Time: at, Message: "plain"}
	row = cr.fill(&plain)
	for _, i := range []int{5, 6, 7} {
		if row[i] != nil {
			t.Errorf("column %s = %#v after a bare record, want nil", logColumns[i], row[i])
		}
	}
}

func TestCopyRowUsesNullForAbsentOptionals(t *testing.T) {
	t.Parallel()

	rec := model.LogRecord{Time: time.Now(), Message: "plain"}
	var cr copyRow
	row := cr.fill(&rec)

	// nil rather than an empty slice or "{}": NULL costs nothing per row, while an
	// empty JSONB value costs bytes on every row that has no fields.
	for _, i := range []int{5, 6, 7} {
		if row[i] != nil {
			t.Errorf("column %s = %#v, want nil", logColumns[i], row[i])
		}
	}
}

// The pool hands back cleared slices: a recycled slice must not leak the previous
// shipment's records into the next one.
func TestRecordPoolRoundTrip(t *testing.T) {
	t.Parallel()

	s := NewRecords(3)
	s = append(s, model.LogRecord{Message: "leak"}, model.LogRecord{Message: "leak"})
	RecycleRecords(s)

	again := NewRecords(3)
	if len(again) != 0 {
		t.Fatalf("recycled slice has len %d, want 0", len(again))
	}
	if cap(again) < 3 {
		t.Fatalf("recycled slice has cap %d, want >= 3", cap(again))
	}
	if full := again[:cap(again)]; full[0].Message != "" || full[1].Message != "" {
		t.Errorf("recycled slice still holds records: %+v", full[:2])
	}

	// Asking for more than any pooled slice holds allocates rather than failing.
	if big := NewRecords(100_000); cap(big) < 100_000 {
		t.Errorf("NewRecords(100000) cap = %d", cap(big))
	}
}

func TestEncodeLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		extra map[string]string
		want  string
	}{
		// Always an object, never SQL NULL: the column is NOT NULL and containment
		// queries have no null case to handle.
		{name: "nil", extra: nil, want: "{}"},
		{name: "empty", extra: map[string]string{}, want: "{}"},
		{name: "sorted", extra: map[string]string{"z": "1", "a": "2"}, want: `{"a":"2","z":"1"}`},
		{name: "escapes quotes", extra: map[string]string{"k": `a"b`}, want: `{"k":"a\"b"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := encodeLabels(model.LabelSet{Extra: tc.extra}); got != tc.want {
				t.Fatalf("encodeLabels = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestStreamCacheRefreshWindow(t *testing.T) {
	t.Parallel()

	const refreshAfter = time.Minute
	cache := newStreamCache(4, refreshAfter, nil)
	base := time.Date(2026, 3, 14, 15, 0, 0, 0, time.UTC)
	const id model.StreamID = 1

	if !cache.needsUpsert(id, base) {
		t.Fatal("an unknown stream must be upserted")
	}

	cache.markUpserted(id, base)
	if cache.needsUpsert(id, base.Add(refreshAfter-time.Second)) {
		t.Fatal("a freshly written stream must not be upserted again")
	}
	// At exactly the refresh interval last_seen is stale enough to rewrite. Because
	// the upsert guards last_seen with GREATEST, being late here is a freshness
	// tradeoff and never a correctness one.
	if !cache.needsUpsert(id, base.Add(refreshAfter)) {
		t.Fatal("a stale stream must be upserted again")
	}
}

func TestStreamCacheEvictionForcesReupsert(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(2, time.Hour, nil)
	now := time.Now()
	for id := model.StreamID(1); id <= 3; id++ {
		cache.markUpserted(id, now)
	}

	// Eviction must fail safe: forgetting a stream costs one redundant upsert, while
	// remembering one that is not in the table costs a foreign key violation for a
	// whole batch.
	if !cache.needsUpsert(1, now) {
		t.Fatal("the evicted stream should need upserting again")
	}
	if cache.needsUpsert(3, now) {
		t.Fatal("the most recent stream should still be cached")
	}
}

func TestWriteBatchDeduplicatesStreams(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(16, time.Hour, nil)
	batch := newWriteBatch(8)
	now := time.Now()

	labels := model.LabelSet{Service: "api", Host: "h", Env: "prod"}
	stream := model.NewStream(labels, now)

	for i := 0; i < 3; i++ {
		sh := Shipment{Stream: stream, Records: []model.LogRecord{{Time: now, Seq: int64(i)}}}
		batch.add(&sh, cache, now)
	}

	// One upsert per distinct stream per batch. More than one would make ON CONFLICT
	// DO UPDATE touch the same row twice, which Postgres rejects outright.
	if got := len(batch.streams); got != 1 {
		t.Fatalf("batch holds %d streams, want 1", got)
	}
	if got := len(batch.records); got != 3 {
		t.Fatalf("batch holds %d records, want 3", got)
	}
	if got := len(batch.streamSlice()); got != 1 {
		t.Fatalf("streamSlice returned %d entries, want 1", got)
	}
}

func TestWriteBatchSkipsCachedStreams(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(16, time.Hour, nil)
	batch := newWriteBatch(8)
	now := time.Now()

	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, now)
	cache.markUpserted(stream.ID, now)

	sh := Shipment{Stream: stream, Records: []model.LogRecord{{Time: now}}}
	batch.add(&sh, cache, now)

	if len(batch.streams) != 0 {
		t.Fatalf("a cached stream should not be re-upserted, got %d", len(batch.streams))
	}
	if batch.streamSlice() != nil {
		t.Fatal("streamSlice should be nil when there is nothing to upsert")
	}
	if batch.empty() {
		t.Fatal("the records should still have been queued")
	}
}

func TestWriteBatchClonesLabels(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(16, time.Hour, nil)
	batch := newWriteBatch(8)
	now := time.Now()

	labels := model.LabelSet{Service: "api", Host: "h", Env: "prod", Extra: map[string]string{"k": "v"}}
	sh := Shipment{Stream: model.NewStream(labels, now), Records: []model.LogRecord{{Time: now}}}
	batch.add(&sh, cache, now)

	// The batch outlives Submit, and the caller may reuse its label map for the next
	// shipment.
	labels.Extra["k"] = "mutated"
	for _, s := range batch.streams {
		if s.Labels.Extra["k"] != "v" {
			t.Fatalf("batch aliases the caller's label map: %q", s.Labels.Extra["k"])
		}
	}
}

func TestWriteBatchResetKeepsCapacity(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(16, time.Hour, nil)
	batch := newWriteBatch(64)
	now := time.Now()

	sh := Shipment{
		Stream:  model.NewStream(model.LabelSet{Service: "s", Host: "h", Env: "e"}, now),
		Records: make([]model.LogRecord, 32),
		Ack:     func(error) {},
	}
	batch.add(&sh, cache, now)

	capacityBefore := cap(batch.records)
	batch.reset()

	if !batch.empty() || len(batch.streams) != 0 || len(batch.acks) != 0 {
		t.Fatal("reset left state behind")
	}
	// A steady-state writer must stop allocating after its first few batches.
	if cap(batch.records) != capacityBefore {
		t.Fatalf("capacity dropped from %d to %d", capacityBefore, cap(batch.records))
	}
}

func TestWriteBatchAckAllRunsEveryCallback(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(16, time.Hour, nil)
	batch := newWriteBatch(8)
	now := time.Now()
	stream := model.NewStream(model.LabelSet{Service: "s", Host: "h", Env: "e"}, now)

	got := make([]error, 0, 3)
	for i := 0; i < 3; i++ {
		sh := Shipment{
			Stream:  stream,
			Records: []model.LogRecord{{Time: now}},
			Ack:     func(err error) { got = append(got, err) },
		}
		batch.add(&sh, cache, now)
	}
	// A shipment with no Ack must not consume a slot, or the acks would be misaligned.
	noAck := Shipment{Stream: stream, Records: []model.LogRecord{{Time: now}}}
	batch.add(&noAck, cache, now)

	want := errors.New("write failed")
	batch.ackAll(want)

	if len(got) != 3 {
		t.Fatalf("ackAll invoked %d callbacks, want 3", len(got))
	}
	for _, err := range got {
		if !errors.Is(err, want) {
			t.Fatalf("callback got %v, want %v", err, want)
		}
	}
}

func TestSubmitBeforeStartFails(t *testing.T) {
	t.Parallel()

	w, err := NewWriter(&stubPool, testWriterConfig(), nil, nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Without this guard Submit would block on a channel nobody is reading, which
	// looks like a hung database rather than a wiring mistake.
	if _, err := w.Submit(context.Background(), Shipment{}); !errors.Is(err, ErrWriterNotStarted) {
		t.Fatalf("Submit error = %v, want ErrWriterNotStarted", err)
	}
}

func TestCloseWithoutStartIsANoOp(t *testing.T) {
	t.Parallel()

	w, err := NewWriter(&stubPool, testWriterConfig(), nil, nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close on an unstarted writer: %v", err)
	}
}

func TestWriteBatchParentsOnFirstTraceAndLinksTheRest(t *testing.T) {
	t.Parallel()

	cache := newStreamCache(16, time.Hour, nil)
	batch := newWriteBatch(8)
	now := time.Now()
	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, now)

	spanCtx := func(b byte) trace.SpanContext {
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{b}, SpanID: trace.SpanID{b}, TraceFlags: trace.FlagsSampled,
		})
	}
	unsampled := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{9}, SpanID: trace.SpanID{9}})
	untraced := Shipment{Stream: stream, Records: []model.LogRecord{{Time: now}}}
	batch.add(&untraced, cache, now)
	// An unsampled trace arrives first: it must not hold the parent slot once
	// a sampled one shows up, or a parent-based sampler drops the write span.
	first := Shipment{Stream: stream, Records: []model.LogRecord{{Time: now}},
		Context: trace.ContextWithSpanContext(context.Background(), unsampled)}
	batch.add(&first, cache, now)
	for i := byte(1); i <= 3; i++ {
		sh := Shipment{Stream: stream, Records: []model.LogRecord{{Time: now, Seq: int64(i)}},
			Context: trace.ContextWithSpanContext(context.Background(), spanCtx(i))}
		batch.add(&sh, cache, now)
	}

	if !batch.parent.Equal(spanCtx(1)) {
		t.Errorf("parent = %v, want the first sampled shipment", batch.parent)
	}
	if len(batch.links) != 3 {
		t.Fatalf("links = %d, want 3 (the unsampled one plus every sampled shipment after the first)", len(batch.links))
	}
	if !batch.links[0].SpanContext.Equal(unsampled) {
		t.Error("the demoted unsampled parent was not kept as a link")
	}
	batch.reset()
	if batch.parent.IsValid() || len(batch.links) != 0 {
		t.Error("reset kept trace state")
	}
}
