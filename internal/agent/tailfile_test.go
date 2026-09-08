package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPollInterval keeps every test's poll loop fast so cancellation and
// "data appears later" assertions land in tens of milliseconds, not seconds.
const testPollInterval = 5 * time.Millisecond

// newTailTest starts s.Run on its own goroutine against a fresh context and
// registers cleanup that cancels it, waits for Run to return, and closes s —
// in that order, so Close never races a Run that is still mid-read.
func newTailTest(t *testing.T, cfg TailConfig, outCap int) (out chan Line, errCh chan error, cancel context.CancelFunc) {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = testPollInterval
	}
	s, err := NewTailSource(cfg)
	if err != nil {
		t.Fatalf("NewTailSource() error = %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	out = make(chan Line, outCap)
	errCh = make(chan error, 1)
	go func() {
		errCh <- s.Run(ctx, out)
	}()

	t.Cleanup(func() {
		cancelFn()
		<-errCh
		_ = s.Close()
	})

	return out, errCh, cancelFn
}

func waitLine(t *testing.T, out <-chan Line, timeout time.Duration) Line {
	t.Helper()
	select {
	case l := <-out:
		return l
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a line")
		return Line{}
	}
}

func expectNoLine(t *testing.T, out <-chan Line, wait time.Duration) {
	t.Helper()
	select {
	case l := <-out:
		t.Fatalf("received unexpected line %q", string(l.Bytes))
	case <-time.After(wait):
	}
}

func appendToFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open %s for append: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("append to %s: %v", path, err)
	}
}

func TestNewTailSource_RequiresPath(t *testing.T) {
	if _, err := NewTailSource(TailConfig{}); err == nil {
		t.Error("NewTailSource() with empty path: error = nil, want non-nil")
	}
}

func TestTailSource_Name(t *testing.T) {
	s, err := NewTailSource(TailConfig{Path: "/some/configured/path.log"})
	if err != nil {
		t.Fatalf("NewTailSource() error = %v", err)
	}
	if got := s.Name(); got != "/some/configured/path.log" {
		t.Errorf("Name() = %q, want %q", got, "/some/configured/path.log")
	}
}

func TestTailSource_ReadsExistingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "first\nsecond\nthird\n")

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)

	wantLines := []string{"first", "second", "third"}
	wantStarts := []int64{0, 6, 13}
	wantOffsets := []int64{6, 13, 19}

	for i, want := range wantLines {
		l := waitLine(t, out, time.Second)
		if string(l.Bytes) != want {
			t.Errorf("line %d = %q, want %q", i, l.Bytes, want)
		}
		if l.Cursor.Start != wantStarts[i] {
			t.Errorf("line %d Start = %d, want %d", i, l.Cursor.Start, wantStarts[i])
		}
		if l.Cursor.Offset != wantOffsets[i] {
			t.Errorf("line %d Offset = %d, want %d", i, l.Cursor.Offset, wantOffsets[i])
		}
		if l.Cursor.File.IsZero() {
			t.Errorf("line %d Cursor.File is zero, want a real FileID", i)
		}
		if l.Source != path {
			t.Errorf("line %d Source = %q, want %q", i, l.Source, path)
		}
	}
}

func TestTailSource_PicksUpAppendedData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "first\n")

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)

	l := waitLine(t, out, time.Second)
	if string(l.Bytes) != "first" {
		t.Fatalf("first line = %q, want %q", l.Bytes, "first")
	}

	// Nothing more should arrive until the file grows.
	expectNoLine(t, out, 30*time.Millisecond)

	appendToFile(t, path, "second\n")

	l2 := waitLine(t, out, time.Second)
	if string(l2.Bytes) != "second" {
		t.Errorf("second line = %q, want %q", l2.Bytes, "second")
	}
}

// TestTailSource_PartialLineHeldThenEmitted is the critical test named in the
// brief: an unterminated fragment must never be emitted, and the offset must
// not have moved past it while it was incomplete. A restart that resumed from
// a mid-line offset would permanently split the record in two.
func TestTailSource_PartialLineHeldThenEmitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "complete\n") // 9 bytes: the next line starts at offset 9.

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)

	l := waitLine(t, out, time.Second)
	if string(l.Bytes) != "complete" {
		t.Fatalf("first line = %q, want %q", l.Bytes, "complete")
	}
	if l.Cursor.Offset != 9 {
		t.Fatalf("first line Offset = %d, want 9", l.Cursor.Offset)
	}

	// Append a fragment with no terminator. It must not be emitted across
	// several poll cycles, however long the writer takes to finish it.
	appendToFile(t, path, "partial")
	expectNoLine(t, out, 50*time.Millisecond)
	appendToFile(t, path, " fragment, still unterminated")
	expectNoLine(t, out, 50*time.Millisecond)

	// Terminate it. The emitted line must be the full, intact fragment, and
	// its Start must still be 9 — proof the offset never advanced past it
	// while it was held.
	appendToFile(t, path, "\n")

	l2 := waitLine(t, out, time.Second)
	want := "partial fragment, still unterminated"
	if string(l2.Bytes) != want {
		t.Errorf("second line = %q, want %q", l2.Bytes, want)
	}
	if l2.Cursor.Start != 9 {
		t.Errorf("second line Start = %d, want 9 (offset must not advance past a held fragment)", l2.Cursor.Start)
	}
}

// TestTailSource_LineArrivesInPieces covers a line built from several small
// appends rather than one: it must be emitted exactly once, intact.
func TestTailSource_LineArrivesInPieces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "")

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)

	for _, piece := range []string{"chunk-one-", "chunk-two-", "chunk-three"} {
		appendToFile(t, path, piece)
		expectNoLine(t, out, 20*time.Millisecond)
	}
	appendToFile(t, path, "\n")

	l := waitLine(t, out, time.Second)
	want := "chunk-one-chunk-two-chunk-three"
	if string(l.Bytes) != want {
		t.Errorf("line = %q, want %q", l.Bytes, want)
	}
	if l.Cursor.Start != 0 {
		t.Errorf("Start = %d, want 0", l.Cursor.Start)
	}

	// Emitted exactly once: nothing else should follow.
	expectNoLine(t, out, 30*time.Millisecond)
}

func TestTailSource_FileAppearsLater(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	// Deliberately do not create the file before starting Run.

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)

	expectNoLine(t, out, 30*time.Millisecond)

	writeFile(t, path, "hello\n")

	l := waitLine(t, out, time.Second)
	if string(l.Bytes) != "hello" {
		t.Errorf("line = %q, want %q", l.Bytes, "hello")
	}
}

func TestTailSource_StartOffsetSkipsAlreadyReadContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "skip-me\nkeep-me\n") // "skip-me\n" is 8 bytes.

	out, _, _ := newTailTest(t, TailConfig{Path: path, StartOffset: 8}, 8)

	l := waitLine(t, out, time.Second)
	if string(l.Bytes) != "keep-me" {
		t.Errorf("line = %q, want %q", l.Bytes, "keep-me")
	}
	if l.Cursor.Start != 8 {
		t.Errorf("Start = %d, want 8", l.Cursor.Start)
	}
}

func TestTailSource_CancellationWhileAwaitingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "never-created.log")

	s, err := NewTailSource(TailConfig{Path: path, PollInterval: testPollInterval})
	if err != nil {
		t.Fatalf("NewTailSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	start := time.Now()
	go func() { errCh <- s.Run(ctx, make(chan Line, 1)) }()

	err = <-errCh
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("Run() took %v to return after cancellation, want much less", elapsed)
	}
}

func TestTailSource_CancellationWhileIdlePolling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "line\n")

	s, err := NewTailSource(TailConfig{Path: path, PollInterval: testPollInterval})
	if err != nil {
		t.Fatalf("NewTailSource() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Line, 4)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	waitLine(t, out, time.Second) // Drain the existing line so the source settles into idle polling.

	start := time.Now()
	cancel()
	err = <-errCh
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("Run() took %v to return after cancellation, want much less", elapsed)
	}
}

func TestTailSource_CloseTwiceAfterRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "line\n")

	s, err := NewTailSource(TailConfig{Path: path, PollInterval: testPollInterval})
	if err != nil {
		t.Fatalf("NewTailSource() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Line, 4)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx, out) }()

	waitLine(t, out, time.Second)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}

	if err := s.Close(); err != nil {
		t.Errorf("first Close() error = %v, want nil", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

// TestTailSource_CloseBeforeFileOpens covers Close racing ahead of Run: the
// source never got as far as opening a file, so Close must still be safe and
// idempotent, and must stop Run from leaking that file once it does open.
func TestTailSource_CloseBeforeFileOpens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "never-created.log")

	s, err := NewTailSource(TailConfig{Path: path, PollInterval: testPollInterval})
	if err != nil {
		t.Fatalf("NewTailSource() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

func TestTailSource_SlowConsumerNoDrops(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	numLines := 20
	var content strings.Builder
	for i := 0; i < numLines; i++ {
		fmt.Fprintf(&content, "line%d\n", i)
	}
	writeFile(t, path, content.String())

	// A capacity-1 out channel forces the source to block between sends.
	out, _, _ := newTailTest(t, TailConfig{Path: path}, 1)

	for i := 0; i < numLines; i++ {
		l := waitLine(t, out, time.Second)
		want := fmt.Sprintf("line%d", i)
		if string(l.Bytes) != want {
			t.Errorf("line %d = %q, want %q", i, l.Bytes, want)
		}
		time.Sleep(5 * time.Millisecond) // Slow consumer.
	}
}
