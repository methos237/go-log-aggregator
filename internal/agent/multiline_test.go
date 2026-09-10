package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// mustJoiner builds a Joiner or fails the test immediately.
func mustJoiner(t *testing.T, cfg MultilineConfig) *Joiner {
	t.Helper()
	j, err := NewJoiner(cfg)
	if err != nil {
		t.Fatalf("NewJoiner(%+v): %v", cfg, err)
	}
	return j
}

// startJoiner runs j in the background and arranges for it to be stopped when
// the test ends. It returns the error channel so a test can assert on Run's
// return value when it cares.
func startJoiner(t *testing.T, j *Joiner, in <-chan Line, out chan<- Line) (context.Context, context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- j.Run(ctx, in, out)
	}()
	t.Cleanup(cancel)
	return ctx, cancel, errCh
}

// recvLine waits for one Line on out, failing the test if it takes too long.
func recvLine(t *testing.T, out <-chan Line, timeout time.Duration) Line {
	t.Helper()
	select {
	case l := <-out:
		return l
	case <-time.After(timeout):
		t.Fatalf("timed out after %s waiting for a line", timeout)
		return Line{}
	}
}

func TestJoinerStackTraceJoin(t *testing.T) {
	re := regexp.MustCompile(`^\s+at `)
	j := mustJoiner(t, MultilineConfig{Continuation: re})

	in := make(chan Line, 8)
	out := make(chan Line, 8)
	_, _, errCh := startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lines := []Line{
		{Source: "app", Time: base, Bytes: []byte("java.lang.Exception: boom"),
			Cursor: Cursor{Start: 0, Offset: 27}},
		{Source: "app", Time: base.Add(time.Millisecond), Bytes: []byte("    at Foo.bar(Foo.java:10)"),
			Cursor: Cursor{Start: 27, Offset: 56}},
		{Source: "app", Time: base.Add(2 * time.Millisecond), Bytes: []byte("    at Foo.baz(Foo.java:20)"),
			Cursor: Cursor{Start: 56, Offset: 85}},
		{Source: "app", Time: base.Add(3 * time.Millisecond), Bytes: []byte("next unrelated line"),
			Cursor: Cursor{Start: 85, Offset: 106}},
	}
	for _, l := range lines {
		in <- l
	}

	got := recvLine(t, out, time.Second)

	wantBytes := "java.lang.Exception: boom\n    at Foo.bar(Foo.java:10)\n    at Foo.baz(Foo.java:20)"
	if string(got.Bytes) != wantBytes {
		t.Errorf("Bytes = %q, want %q", got.Bytes, wantBytes)
	}
	if got.Cursor.Start != 0 {
		t.Errorf("Cursor.Start = %d, want 0 (the first line's Start)", got.Cursor.Start)
	}
	if got.Cursor.Offset != 85 {
		t.Errorf("Cursor.Offset = %d, want 85 (the last line's Offset)", got.Cursor.Offset)
	}
	if !got.Time.Equal(base) {
		t.Errorf("Time = %s, want %s (the first line's Time)", got.Time, base)
	}
	if got.Source != "app" {
		t.Errorf("Source = %q, want %q", got.Source, "app")
	}

	close(in)
	tail := recvLine(t, out, time.Second)
	if string(tail.Bytes) != "next unrelated line" {
		t.Errorf("tail record Bytes = %q, want %q", tail.Bytes, "next unrelated line")
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Run returned %v, want nil after in closed cleanly", err)
	}
}

func TestJoinerTimeIsFirstLines(t *testing.T) {
	re := regexp.MustCompile(`^\s+at `)
	j := mustJoiner(t, MultilineConfig{Continuation: re})

	in := make(chan Line, 4)
	out := make(chan Line, 4)
	startJoiner(t, j, in, out)

	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	last := first.Add(time.Hour)

	in <- Line{Source: "app", Time: first, Bytes: []byte("start"), Cursor: Cursor{Start: 0, Offset: 6}}
	in <- Line{Source: "app", Time: last, Bytes: []byte("    at Foo.bar"), Cursor: Cursor{Start: 6, Offset: 21}}
	close(in)

	got := recvLine(t, out, time.Second)
	if !got.Time.Equal(first) {
		t.Errorf("Time = %s, want the first line's time %s, not the last line's %s", got.Time, first, last)
	}
}

func TestJoinerInterleavedSourcesDoNotContaminate(t *testing.T) {
	re := regexp.MustCompile(`^\s`)
	j := mustJoiner(t, MultilineConfig{Continuation: re})

	in := make(chan Line, 16)
	out := make(chan Line, 16)
	startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Interleave two sources, each building a two-line record.
	in <- Line{Source: "A", Time: base, Bytes: []byte("A-head"), Cursor: Cursor{Start: 0, Offset: 7}}
	in <- Line{Source: "B", Time: base, Bytes: []byte("B-head"), Cursor: Cursor{Start: 0, Offset: 7}}
	in <- Line{Source: "A", Time: base, Bytes: []byte(" A-cont"), Cursor: Cursor{Start: 7, Offset: 15}}
	in <- Line{Source: "B", Time: base, Bytes: []byte(" B-cont"), Cursor: Cursor{Start: 7, Offset: 15}}
	close(in)

	got := map[string]string{}
	for i := 0; i < 2; i++ {
		l := recvLine(t, out, time.Second)
		got[l.Source] = string(l.Bytes)
	}

	if got["A"] != "A-head\n A-cont" {
		t.Errorf("source A record = %q, want %q", got["A"], "A-head\n A-cont")
	}
	if got["B"] != "B-head\n B-cont" {
		t.Errorf("source B record = %q, want %q", got["B"], "B-head\n B-cont")
	}
}

func TestJoinerNilContinuationPassesThrough(t *testing.T) {
	j := mustJoiner(t, MultilineConfig{})

	in := make(chan Line, 4)
	out := make(chan Line, 4)
	startJoiner(t, j, in, out)

	want := Line{
		Source: "app",
		Time:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Bytes:  []byte("    at Foo.bar(Foo.java:10)"), // looks like a continuation, must not be treated as one
		Cursor: Cursor{Start: 5, Offset: 33, File: FileID{Dev: 1, Ino: 2}},
	}
	in <- want
	close(in)

	got := recvLine(t, out, time.Second)
	if string(got.Bytes) != string(want.Bytes) {
		t.Errorf("Bytes = %q, want %q", got.Bytes, want.Bytes)
	}
	if got.Cursor != want.Cursor {
		t.Errorf("Cursor = %+v, want %+v (untouched)", got.Cursor, want.Cursor)
	}
	if !got.Time.Equal(want.Time) {
		t.Errorf("Time = %s, want %s", got.Time, want.Time)
	}
}

func TestJoinerMaxBytesSplits(t *testing.T) {
	re := regexp.MustCompile(`^\s`)
	j := mustJoiner(t, MultilineConfig{Continuation: re})

	in := make(chan Line, 8)
	out := make(chan Line, 8)
	startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A head line already at the collector's limit (the assembler bounds every
	// line there) leaves no room for a separator plus any continuation, so the
	// next continuation line must start a new record instead of growing this
	// one past the bound.
	head := bytes.Repeat([]byte("h"), model.MaxMessageLen)
	sent := []Line{
		{Source: "app", Time: base, Bytes: head, Cursor: Cursor{Start: 0, Offset: int64(len(head)) + 1}},
		{Source: "app", Time: base, Bytes: []byte(" 87654321"), Cursor: Cursor{Start: int64(len(head)) + 1, Offset: int64(len(head)) + 11}},
	}
	for _, l := range sent {
		in <- l
	}
	close(in)

	first := recvLine(t, out, time.Second)
	second := recvLine(t, out, time.Second)

	if len(first.Bytes) > model.MaxMessageLen {
		t.Errorf("first record is %d bytes, exceeds MaxMessageLen %d", len(first.Bytes), model.MaxMessageLen)
	}
	if !bytes.Equal(first.Bytes, head) {
		t.Errorf("first record = %d bytes of %q..., want the head line alone", len(first.Bytes), first.Bytes[:1])
	}
	if string(second.Bytes) != " 87654321" {
		t.Errorf("second record (the offending line) = %q, want %q", second.Bytes, " 87654321")
	}

	if got := counterValue(t, j.metrics.MultilineMaxBytesSplits); got != 1 {
		t.Errorf("MultilineMaxBytesSplits = %v, want 1", got)
	}
}

func TestJoinerMaxLinesSplits(t *testing.T) {
	re := regexp.MustCompile(`^\s`)
	j := mustJoiner(t, MultilineConfig{Continuation: re})

	in := make(chan Line, defaultMaxLines+8)
	out := make(chan Line, 16)
	startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var offset int64
	send := func(source, body string) {
		start := offset
		offset += int64(len(body)) + 1
		in <- Line{Source: source, Time: base, Bytes: []byte(body), Cursor: Cursor{Start: start, Offset: offset}}
	}

	send("app", "head") // record 1, line 1
	for i := 1; i < defaultMaxLines; i++ {
		send("app", " a") // record 1, lines 2..defaultMaxLines
	}
	send("app", " c") // would be line defaultMaxLines+1: must start a new record instead
	send("app", " d") // record 2, line 2
	close(in)

	first := recvLine(t, out, time.Second)
	second := recvLine(t, out, time.Second)

	if want := "head" + strings.Repeat("\n a", defaultMaxLines-1); string(first.Bytes) != want {
		t.Errorf("first record has %d lines, want %d", strings.Count(string(first.Bytes), "\n")+1, defaultMaxLines)
	}
	if string(second.Bytes) != " c\n d" {
		t.Errorf("second record = %q, want %q", second.Bytes, " c\n d")
	}
	if got := counterValue(t, j.metrics.MultilineMaxLinesSplits); got != 1 {
		t.Errorf("MultilineMaxLinesSplits = %v, want 1", got)
	}
}

func TestJoinerFlushTimeout(t *testing.T) {
	re := regexp.MustCompile(`^\s`)
	const flushTimeout = 20 * time.Millisecond
	j := mustJoiner(t, MultilineConfig{Continuation: re, FlushTimeout: flushTimeout})

	in := make(chan Line, 4)
	out := make(chan Line, 4)
	startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in <- Line{Source: "app", Time: base, Bytes: []byte("only line"), Cursor: Cursor{Start: 0, Offset: 10}}

	// Nothing further arrives; assert the record shows up on its own, driven
	// only by the flush timer.
	got := recvLine(t, out, time.Second)
	if string(got.Bytes) != "only line" {
		t.Errorf("Bytes = %q, want %q", got.Bytes, "only line")
	}
	if got := counterValue(t, j.metrics.MultilineTimeoutFlushes); got != 1 {
		t.Errorf("MultilineTimeoutFlushes = %v, want 1", got)
	}
}

func TestJoinerCloseFlushesEverySource(t *testing.T) {
	re := regexp.MustCompile(`^\s`)
	// Long flush timeout: the flush under test must come from in closing, not
	// from the timer racing it.
	j := mustJoiner(t, MultilineConfig{Continuation: re, FlushTimeout: time.Hour})

	in := make(chan Line, 4)
	out := make(chan Line, 4)
	_, _, errCh := startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in <- Line{Source: "A", Time: base, Bytes: []byte("a-head"), Cursor: Cursor{Start: 0, Offset: 7}}
	in <- Line{Source: "B", Time: base, Bytes: []byte("b-head"), Cursor: Cursor{Start: 0, Offset: 7}}
	close(in)

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		l := recvLine(t, out, time.Second)
		seen[l.Source] = true
	}
	if !seen["A"] || !seen["B"] {
		t.Errorf("sources flushed on close = %v, want both A and B", seen)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestJoinerCancellationReturnsPromptly(t *testing.T) {
	// Nil continuation exercises the passthrough path; a full, undrained out
	// channel forces Run to block on the send so cancellation actually has
	// something to interrupt.
	j := mustJoiner(t, MultilineConfig{})

	in := make(chan Line, 4)
	out := make(chan Line, 1)
	ctx, cancel, errCh := startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Fill out's buffer, then send one more so Run is blocked sending it.
	in <- Line{Source: "app", Time: base, Bytes: []byte("first")}
	in <- Line{Source: "app", Time: base, Bytes: []byte("second")}

	// Give Run a moment to consume the first line into out's one slot and
	// block trying to send the second.
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of cancellation")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Run took %s to return after cancellation, want promptly", elapsed)
	}
	if ctx.Err() == nil {
		t.Error("ctx.Err() is nil after cancel")
	}
}

func TestJoinerNoDropUnderSlowConsumer(t *testing.T) {
	j := mustJoiner(t, MultilineConfig{})

	in := make(chan Line, 4)
	out := make(chan Line, 2) // deliberately small
	startJoiner(t, j, in, out)

	const n = 50
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	go func() {
		for i := 0; i < n; i++ {
			in <- Line{Source: "app", Time: base, Bytes: []byte(fmt.Sprintf("line-%d", i))}
		}
		close(in)
	}()

	got := make([]string, 0, n)
	for i := 0; i < n; i++ {
		// Slow consumer: a small pause before every read, so the pipeline is
		// under backpressure the whole time.
		time.Sleep(time.Millisecond)
		l := recvLine(t, out, time.Second)
		got = append(got, string(l.Bytes))
	}

	if len(got) != n {
		t.Fatalf("received %d lines, want %d: nothing should be dropped under backpressure", len(got), n)
	}
	for i, s := range got {
		want := fmt.Sprintf("line-%d", i)
		if s != want {
			t.Errorf("line %d = %q, want %q (order must be preserved too)", i, s, want)
		}
	}
}

func TestJoinerRotationSpanningRecordCarriesLastFile(t *testing.T) {
	re := regexp.MustCompile(`^\s`)
	j := mustJoiner(t, MultilineConfig{Continuation: re})

	in := make(chan Line, 4)
	out := make(chan Line, 4)
	startJoiner(t, j, in, out)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldFile := FileID{Dev: 1, Ino: 100}
	newFile := FileID{Dev: 1, Ino: 200}

	in <- Line{Source: "app", Time: base, Bytes: []byte("head"),
		Cursor: Cursor{Start: 0, Offset: 5, File: oldFile}}
	// The continuation line arrived after the file rotated: same Source, new
	// FileID, offset reset as a fresh file would have it.
	in <- Line{Source: "app", Time: base, Bytes: []byte(" cont"),
		Cursor: Cursor{Start: 0, Offset: 6, File: newFile}}
	close(in)

	got := recvLine(t, out, time.Second)
	if got.Cursor.File != newFile {
		t.Errorf("Cursor.File = %+v, want the last line's file %+v", got.Cursor.File, newFile)
	}
}

func TestNewJoinerValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  MultilineConfig
	}{
		{name: "negative flush timeout", cfg: MultilineConfig{FlushTimeout: -time.Second}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewJoiner(tt.cfg); err == nil {
				t.Errorf("NewJoiner(%+v): want error, got nil", tt.cfg)
			}
		})
	}
}

func TestNewJoinerDefaults(t *testing.T) {
	j, err := NewJoiner(MultilineConfig{})
	if err != nil {
		t.Fatalf("NewJoiner(zero value): %v", err)
	}
	if j.flushTimeout != defaultFlushTimeout {
		t.Errorf("flushTimeout = %s, want default %s", j.flushTimeout, defaultFlushTimeout)
	}
}
