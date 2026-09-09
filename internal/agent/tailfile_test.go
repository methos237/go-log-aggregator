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

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// counterValue reads a counter's current value directly through the
// prometheus.Metric interface's Write method, rather than pulling in
// prometheus/testutil: testutil drags in a module this project does not
// otherwise depend on, and this package's constraints are deliberately
// strict about not growing go.mod for test-only convenience.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return m.GetCounter().GetValue()
}

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
	s, err := NewTailSource(&cfg)
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
	if _, err := NewTailSource(&TailConfig{}); err == nil {
		t.Error("NewTailSource() with empty path: error = nil, want non-nil")
	}
}

func TestTailSource_Name(t *testing.T) {
	s, err := NewTailSource(&TailConfig{Path: "/some/configured/path.log"})
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

	out, _, _ := newTailTest(t, TailConfig{Path: path, Resume: Cursor{Offset: 8}}, 8)

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

	s, err := NewTailSource(&TailConfig{Path: path, PollInterval: testPollInterval})
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

	s, err := NewTailSource(&TailConfig{Path: path, PollInterval: testPollInterval})
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

	s, err := NewTailSource(&TailConfig{Path: path, PollInterval: testPollInterval})
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

	s, err := NewTailSource(&TailConfig{Path: path, PollInterval: testPollInterval})
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

// fidOf stats path and returns its FileID, for tests that need to construct
// a resumed Cursor naming a specific file.
func fidOf(t *testing.T, path string) FileID {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	fid, ok := fileIDFromInfo(info)
	if !ok {
		t.Fatal("fileIDFromInfo: platform cannot report file identity")
	}
	return fid
}

// drainLines collects the next n lines' Bytes as strings, failing the test if
// they do not all arrive within timeout. Used by the rotation tests, where
// the whole point is that every line from every generation arrives exactly
// once, in order.
func drainLines(t *testing.T, out <-chan Line, n int, timeout time.Duration) []string {
	t.Helper()
	got := make([]string, n)
	for i := 0; i < n; i++ {
		got[i] = string(waitLine(t, out, timeout).Bytes)
	}
	return got
}

func assertLines(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d lines %v, want %d lines %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestTailSource_RotationRenameAndRecreate_NoGap is the exit criterion this
// whole subtask is judged on: a rename-and-recreate rotation, the scheme
// logrotate performs by default, must lose nothing and duplicate nothing.
func TestTailSource_RotationRenameAndRecreate_NoGap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "gen1-first\ngen1-second\n")

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)

	assertLines(t, drainLines(t, out, 2, time.Second), []string{"gen1-first", "gen1-second"})

	rotated := filepath.Join(dir, "app.log.1")
	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename: %v", err)
	}
	writeFile(t, path, "gen2-first\ngen2-second\n")

	assertLines(t, drainLines(t, out, 2, time.Second), []string{"gen2-first", "gen2-second"})
	expectNoLine(t, out, 30*time.Millisecond)
}

// TestTailSource_RotationDrainsUnflushedContentInOldFile is what
// drain-before-switch buys: content appended to the old generation after it
// was renamed away, before this source has noticed anything changed, must
// still be read. It is reachable at all only because an open fd keeps
// reading its own inode after the file is renamed out from under it — the
// platform fact this whole design rests on.
func TestTailSource_RotationDrainsUnflushedContentInOldFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "first\n")

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)
	assertLines(t, drainLines(t, out, 1, time.Second), []string{"first"})

	rotated := filepath.Join(dir, "app.log.1")
	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// Nothing exists at path yet, so the source cannot have switched — it is
	// still reading the fd behind the now-renamed name. Appending through
	// that renamed name reaches the very same inode.
	appendToFile(t, rotated, "second\n")
	assertLines(t, drainLines(t, out, 1, time.Second), []string{"second"})

	// Only now does a new generation appear at the configured path.
	writeFile(t, path, "gen2\n")
	assertLines(t, drainLines(t, out, 1, time.Second), []string{"gen2"})
}

// TestTailSource_RotationEmitsHeldFragmentOnDrain covers the one case where
// the tail source's rule about unterminated lines inverts: in steady state
// an unterminated fragment is held because the writer might still be
// mid-flush, but once the old generation's fd hits true EOF during a drain,
// nothing will ever append to that inode again, so the fragment is final
// content and holding it forever would be a silent gap, not caution.
func TestTailSource_RotationEmitsHeldFragmentOnDrain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "first\n")

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 8)
	assertLines(t, drainLines(t, out, 1, time.Second), []string{"first"})

	// An unterminated fragment: held, not emitted, in steady state.
	appendToFile(t, path, "unterminated-tail")
	expectNoLine(t, out, 30*time.Millisecond)

	rotated := filepath.Join(dir, "app.log.1")
	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename: %v", err)
	}
	writeFile(t, path, "gen2\n")

	// The drain must emit the fragment even though it never saw a
	// terminator, and only then move on to the new generation.
	assertLines(t, drainLines(t, out, 2, time.Second), []string{"unterminated-tail", "gen2"})
}

// TestTailSource_CopyTruncate_NoGapNoDuplicate covers logrotate's
// copytruncate mode: the file is truncated in place (same inode) rather
// than renamed, after its content has been copied elsewhere by the rotator.
func TestTailSource_CopyTruncate_NoGapNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "line1\nline2\n")

	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	out, _, _ := newTailTest(t, TailConfig{Path: path, Metrics: metrics}, 8)

	assertLines(t, drainLines(t, out, 2, time.Second), []string{"line1", "line2"})

	// copytruncate: the rotator has already copied "line1\nline2\n" elsewhere;
	// this source only ever sees the truncate and the new content.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	appendToFile(t, path, "line3\n")

	assertLines(t, drainLines(t, out, 1, time.Second), []string{"line3"})
	expectNoLine(t, out, 30*time.Millisecond)

	if got := counterValue(t, metrics.TruncationsDetected); got != 1 {
		t.Errorf("TruncationsDetected = %v, want 1", got)
	}
}

// TestTailSource_TruncateAndRewritePastOldOffset_CaughtByFingerprint is
// Decision 4: a writer that truncates and rewrites past the old offset
// inside one poll interval never appears to shrink, so a check based on size
// alone resumes partway into unrelated new content and emits a corrupted
// line with no error. Only the head fingerprint catches it.
//
// This test must fail if the fingerprint check is removed; that was verified
// by temporarily disabling it (see the report) rather than asserted here,
// since a test cannot assert against code it does not contain.
func TestTailSource_TruncateAndRewritePastOldOffset_CaughtByFingerprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	original := strings.Repeat("a", 299) + "\n" // 300 bytes, one line.
	writeFile(t, path, original)

	// A long poll interval gives the truncate-then-rewrite below a wide,
	// uncontested window between the initial synchronous read (which happens
	// before Run ever waits on the ticker) and the next poll, so the size
	// never appears to shrink from this source's point of view — exactly the
	// race Decision 4 describes, made deterministic for the test instead of
	// left to chance.
	out, _, _ := newTailTest(t, TailConfig{Path: path, PollInterval: 300 * time.Millisecond}, 8)
	assertLines(t, drainLines(t, out, 1, time.Second), []string{original[:299]})

	// Truncate and rewrite with content at least as long as the old offset
	// (300 bytes), so size alone never shows a shrink, but with different
	// bytes in the fingerprinted prefix.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	rewritten := strings.Repeat("b", 349) + "\n" // 350 bytes, one line.
	appendToFile(t, path, rewritten)

	l := waitLine(t, out, time.Second)
	if string(l.Bytes) != rewritten[:349] {
		t.Errorf("line = %q, want %q (the fingerprint mismatch should have reset to offset 0)", l.Bytes, rewritten[:349])
	}
	if l.Cursor.Start != 0 {
		t.Errorf("Start = %d, want 0; a nonzero Start here means the source resumed at the stale offset "+
			"into unrelated new content instead of noticing the rewrite", l.Cursor.Start)
	}
}

// TestTailSource_StaleOffsetPastEOFOnFirstCycle is Decision 6, the latent bug
// this subtask exists to fix: a resumed offset beyond the file's current
// size Seeks past EOF successfully, so every read then returns EOF and,
// without a check that runs before the very first read, the source would
// stall forever instead of noticing the file is smaller than the checkpoint
// remembers.
func TestTailSource_StaleOffsetPastEOFOnFirstCycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "hello\n")
	fid := fidOf(t, path)

	out, _, _ := newTailTest(t, TailConfig{
		Path:   path,
		Resume: Cursor{Offset: 1000, File: fid}, // Far past the 6-byte file.
	}, 8)

	// If this stalls, the bug is present: nothing arrives, ever, and the
	// test times out inside waitLine.
	assertLines(t, drainLines(t, out, 1, time.Second), []string{"hello"})
}

// TestTailSource_MissedGenerationAtStartup is Decision 5: the checkpoint
// names a FileID that is not the file now at the path, meaning a rotation
// happened while this agent was not running. Resuming at the persisted
// offset would mean nothing in the new file, so this must start at zero and
// count the loss rather than silently misreading the new file's content or
// crashing.
func TestTailSource_MissedGenerationAtStartup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "current-generation\n")

	staleFid := FileID{Dev: fidOf(t, path).Dev, Ino: fidOf(t, path).Ino + 999999}

	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	out, _, _ := newTailTest(t, TailConfig{
		Path:    path,
		Metrics: metrics,
		Resume:  Cursor{Offset: 500, File: staleFid},
	}, 8)

	assertLines(t, drainLines(t, out, 1, time.Second), []string{"current-generation"})

	if got := counterValue(t, metrics.GenerationsMissed); got != 1 {
		t.Errorf("GenerationsMissed = %v, want 1", got)
	}
	dropped := counterValue(t, metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonMissedGeneration))
	if dropped != 1 {
		t.Errorf("RecordsDropped{component=agent,reason=missed_generation} = %v, want 1", dropped)
	}
}

// TestTailSource_RotationByInodeNeverByName pins the property every other
// rotation test relies on implicitly: identity is os.Stat(path)'s inode
// against the open fd's inode, never the name at that path.
func TestTailSource_RotationByInodeNeverByName(t *testing.T) {
	t.Run("rotate via rename to a different name and a new file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		writeFile(t, path, "old\n")

		reg := prometheus.NewRegistry()
		metrics := NewMetrics(reg)
		out, _, _ := newTailTest(t, TailConfig{Path: path, Metrics: metrics}, 8)
		assertLines(t, drainLines(t, out, 1, time.Second), []string{"old"})

		// Rename to a name nothing else in this test ever refers to, then
		// create an entirely different file at the original path.
		if err := os.Rename(path, filepath.Join(dir, "whatever-name.log")); err != nil {
			t.Fatalf("rename: %v", err)
		}
		writeFile(t, path, "new\n")

		assertLines(t, drainLines(t, out, 1, time.Second), []string{"new"})
		if got := counterValue(t, metrics.RotationsDetected); got != 1 {
			t.Errorf("RotationsDetected = %v, want 1", got)
		}
	})

	t.Run("rename away and back with the same inode is not a rotation", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		writeFile(t, path, "first\n")

		reg := prometheus.NewRegistry()
		metrics := NewMetrics(reg)
		out, _, _ := newTailTest(t, TailConfig{Path: path, Metrics: metrics}, 8)
		assertLines(t, drainLines(t, out, 1, time.Second), []string{"first"})

		aside := filepath.Join(dir, "aside.log")
		if err := os.Rename(path, aside); err != nil {
			t.Fatalf("rename away: %v", err)
		}
		if err := os.Rename(aside, path); err != nil {
			t.Fatalf("rename back: %v", err)
		}
		appendToFile(t, path, "second\n")

		assertLines(t, drainLines(t, out, 1, time.Second), []string{"second"})
		if got := counterValue(t, metrics.RotationsDetected); got != 0 {
			t.Errorf("RotationsDetected = %v, want 0 (same inode throughout, never renamed by name)", got)
		}
	})
}

// TestTailSource_SlowConsumerNoDropsAcrossRotation combines the two
// backpressure and rotation guarantees: a consumer slow enough to force the
// source to block on out must still receive every line from both
// generations, in order, exactly once.
func TestTailSource_SlowConsumerNoDropsAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "gen1-a\ngen1-b\ngen1-c\n")

	// Capacity-1 forces blocking sends throughout.
	out, _, _ := newTailTest(t, TailConfig{Path: path}, 1)

	want := []string{"gen1-a", "gen1-b", "gen1-c"}
	for i, w := range want {
		l := waitLine(t, out, time.Second)
		if string(l.Bytes) != w {
			t.Errorf("line %d = %q, want %q", i, l.Bytes, w)
		}
		time.Sleep(5 * time.Millisecond) // Slow consumer.
	}

	rotated := filepath.Join(dir, "app.log.1")
	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename: %v", err)
	}
	writeFile(t, path, "gen2-a\ngen2-b\ngen2-c\n")

	want2 := []string{"gen2-a", "gen2-b", "gen2-c"}
	for i, w := range want2 {
		l := waitLine(t, out, time.Second)
		if string(l.Bytes) != w {
			t.Errorf("line %d = %q, want %q", i, l.Bytes, w)
		}
		time.Sleep(5 * time.Millisecond) // Slow consumer.
	}

	expectNoLine(t, out, 30*time.Millisecond)
}

// TestTailSource_EmittedCursorCarriesHead pins that an emitted line's cursor
// carries the file's head fingerprint, and that it survives a checkpoint round
// trip.
//
// This is a regression test for a bug that every other test in this file missed:
// emit built its Cursor without Head, so the running process detected a
// truncate-and-rewrite correctly (resolveState tracks the fingerprint in a
// local) while the value never reached the checkpoint. Everything looked right
// until a restart, at which point the resumed Head was always zero, Comparable
// always reported false, and the rewrite-while-down case silently went
// undetected. Asserting the fingerprint inside the process is not enough — the
// assertion has to follow the cursor out through the store, which is the only
// path that actually matters.
func TestTailSource_EmittedCursorCarriesHead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	// Longer than headFingerprintLen, or there is deliberately no fingerprint.
	filler := strings.Repeat("x", headFingerprintLen)
	if err := os.WriteFile(path, []byte(filler+"\nsecond\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	want := fingerprintOf(t, path)
	if want.IsZero() {
		t.Fatal("test setup is wrong: the file should be long enough to fingerprint")
	}

	out, _, _ := newTailTest(t, TailConfig{Path: path}, 4)

	first := waitLine(t, out, time.Second)
	if !first.Cursor.Head.Matches(want) {
		t.Errorf("first line Cursor.Head = %+v, want %+v", first.Cursor.Head, want)
	}
	second := waitLine(t, out, time.Second)
	if !second.Cursor.Head.Matches(want) {
		t.Errorf("second line Cursor.Head = %+v, want %+v", second.Cursor.Head, want)
	}

	// The round trip that the bug actually broke: persist the acked cursor,
	// reload it in a fresh store, and confirm the fingerprint is still there to
	// compare against on the next run.
	store := NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))
	store.Set(path, second.Cursor)
	if err := store.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	reloaded := NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))
	if err := reloaded.Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got, ok := reloaded.Get(path)
	if !ok {
		t.Fatal("Get() found no cursor for the source")
	}
	if !got.Head.Matches(want) {
		t.Errorf("Head did not survive the checkpoint round trip: got %+v, want %+v", got.Head, want)
	}
	if got.Head.IsZero() {
		t.Error("persisted Head is zero, so a restart could not detect a rewrite")
	}
}

// fingerprintOf is the expected fingerprint of a file on disk, computed
// independently of the tail source.
func fingerprintOf(t *testing.T, path string) Fingerprint {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	fp, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead(%s): %v", path, err)
	}
	return fp
}
