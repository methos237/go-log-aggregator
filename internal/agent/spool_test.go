package agent

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// fixedPayload returns a 10-byte, index-distinguishable payload. Every test
// that needs to reason about exact on-disk offsets (segment rolls, torn
// tails, MaxBytes eviction) uses this so every entry has the same encoded
// size and the arithmetic is simple; i must be in [0, 100).
func fixedPayload(i int) []byte {
	return []byte(fmt.Sprintf("payload-%02d", i))
}

func mustNewSpool(t *testing.T, dir string, maxBytes, segmentBytes int64) *Spool {
	t.Helper()
	sp, err := NewSpool(SpoolConfig{Dir: dir, MaxBytes: maxBytes, SegmentBytes: segmentBytes})
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	return sp
}

func TestSpoolPeekDoesNotConsume(t *testing.T) {
	dir := t.TempDir()
	sp := mustNewSpool(t, dir, 1<<20, 1<<20)
	t.Cleanup(func() { _ = sp.Close() })

	want := fixedPayload(0)
	if err := sp.Append(want); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for i := 0; i < 2; i++ {
		got, ok, err := sp.Peek()
		if err != nil {
			t.Fatalf("Peek #%d: %v", i, err)
		}
		if !ok {
			t.Fatalf("Peek #%d: ok = false, want true", i)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Peek #%d = %q, want %q", i, got, want)
		}
	}

	if err := sp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, ok, err := sp.Peek(); err != nil || ok {
		t.Fatalf("Peek after Release: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
}

func TestSpoolFIFOAcrossSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	// Three entries per segment, forcing several rolls across seven appends.
	sp := mustNewSpool(t, dir, 1<<20, entrySize*3)
	t.Cleanup(func() { _ = sp.Close() })

	const n = 7
	for i := 0; i < n; i++ {
		if err := sp.Append(fixedPayload(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		got, ok, err := sp.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		if want := fixedPayload(i); !bytes.Equal(got, want) {
			t.Fatalf("entry %d = %q, want %q", i, got, want)
		}
		if err := sp.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}

	if _, ok, err := sp.Peek(); err != nil || ok {
		t.Fatalf("Peek after draining all: ok=%v err=%v", ok, err)
	}
	if got := sp.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
	if got := sp.Bytes(); got != 0 {
		t.Errorf("Bytes() = %d, want 0", got)
	}
}

func TestSpoolReleaseOnEmptyIsNoop(t *testing.T) {
	dir := t.TempDir()
	sp := mustNewSpool(t, dir, 1<<20, 1<<20)
	t.Cleanup(func() { _ = sp.Close() })

	if _, ok, err := sp.Peek(); err != nil || ok {
		t.Fatalf("Peek on fresh spool: ok=%v err=%v, want false/nil", ok, err)
	}
	// Documented behavior: Release with nothing to release is a no-op, not an
	// error, and calling it repeatedly must not panic or misbehave.
	for i := 0; i < 3; i++ {
		if err := sp.Release(); err != nil {
			t.Fatalf("Release #%d on empty spool: %v", i, err)
		}
	}
}

func TestSpoolReopenPreservesUnreleased(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	sp := mustNewSpool(t, dir, 1<<20, entrySize*3)

	const n = 5
	for i := 0; i < n; i++ {
		if err := sp.Append(fixedPayload(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	// Release the first two: three should survive a restart.
	for i := 0; i < 2; i++ {
		if err := sp.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	if err := sp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := mustNewSpool(t, dir, 1<<20, entrySize*3)
	t.Cleanup(func() { _ = reopened.Close() })

	if got := reopened.Len(); got != 3 {
		t.Fatalf("Len() after reopen = %d, want 3", got)
	}
	// Bytes() counts whole segment files on disk, including the two already-
	// released entries still sitting in segment 0 (it has 3 slots, 2 of them
	// consumed, and is not deleted until all 3 are) — so this is all 5
	// entries' worth, not just the 3 logically unreleased ones; see the
	// Bytes doc comment.
	if got, want := reopened.Bytes(), entrySize*int64(n); got != want {
		t.Fatalf("Bytes() after reopen = %d, want %d", got, want)
	}
	for i := 2; i < n; i++ {
		got, ok, err := reopened.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		if want := fixedPayload(i); !bytes.Equal(got, want) {
			t.Fatalf("entry %d = %q, want %q", i, got, want)
		}
		if err := reopened.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	if _, ok, err := reopened.Peek(); err != nil || ok {
		t.Fatalf("Peek after full drain: ok=%v err=%v", ok, err)
	}
}

// TestSpoolReopenMidDrainBoundedReplay exercises the documented tradeoff in
// Release: a durable cursor persist is forced on every segment boundary, but
// between boundaries the cheap persist does not fsync, so a crash can
// replay up to cursorDurableInterval-1 already-released entries in the
// current oldest segment. It simulates that crash directly, by reverting
// the persisted cursor to what it was right after the last durable
// (boundary-forced) persist, since a real test cannot force the OS to lose
// a write the way a power failure would.
//
// The point of the assertion is not "replay happens" on its own — it is
// that the replay is bounded to the still-live segment. Entries in a
// segment that was fully consumed and deleted before the simulated crash
// can never come back, no matter how stale the persisted cursor is, because
// the bytes are simply gone from disk.
func TestSpoolReopenMidDrainBoundedReplay(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	sp := mustNewSpool(t, dir, 1<<20, entrySize*3) // three entries per segment

	const n = 9 // three whole segments
	for i := 0; i < n; i++ {
		if err := sp.Append(fixedPayload(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	// Release entries 0,1,2: this finishes segment 0 exactly, which forces a
	// durable persist (segment boundary), landing the cursor at {segment: 1,
	// offset: 0} durably.
	for i := 0; i < 3; i++ {
		if err := sp.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	// Snapshot the cursor file exactly as it stands right after that durable
	// persist — this is the state a crash would roll back to.
	durableCursor, err := os.ReadFile(filepath.Join(dir, cursorFileName))
	if err != nil {
		t.Fatalf("read cursor after boundary persist: %v", err)
	}

	// Release entry 3: this only advances within segment 1, so it takes the
	// cheap, non-durable path (releasesSincePersist is 1, well under
	// cursorDurableInterval).
	if err = sp.Release(); err != nil {
		t.Fatalf("Release(3): %v", err)
	}

	// Simulate a crash that lost that last cheap persist: put the cursor file
	// back to the durable snapshot, then abandon sp without calling Close
	// (Close would durably persist the true state and defeat the point).
	if err = os.WriteFile(filepath.Join(dir, cursorFileName), durableCursor, 0o600); err != nil {
		t.Fatalf("revert cursor to simulate crash: %v", err)
	}

	reopened := mustNewSpool(t, dir, 1<<20, entrySize*3)
	t.Cleanup(func() { _ = reopened.Close() })

	// Entry 3 replays: its release was "lost" to the simulated crash.
	got, ok, err := reopened.Peek()
	if err != nil || !ok {
		t.Fatalf("Peek after simulated crash: ok=%v err=%v", ok, err)
	}
	if want := fixedPayload(3); !bytes.Equal(got, want) {
		t.Fatalf("replayed entry = %q, want %q (the bounded duplicate)", got, want)
	}

	// Drain the rest and assert entries 0,1,2 (deleted segment) never
	// reappear, and 4..8 still arrive in order exactly once.
	for i := 3; i < n; i++ {
		got, ok, err := reopened.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		if want := fixedPayload(i); !bytes.Equal(got, want) {
			t.Fatalf("entry %d = %q, want %q", i, got, want)
		}
		if err := reopened.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	if _, ok, err := reopened.Peek(); err != nil || ok {
		t.Fatalf("Peek after full drain: ok=%v err=%v", ok, err)
	}
}

func TestSpoolTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	sp := mustNewSpool(t, dir, 1<<20, entrySize*10)

	for i := 0; i < 3; i++ {
		if err := sp.Append(fixedPayload(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := sp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segPath := filepath.Join(dir, segmentFileName(0))
	fullSize := entrySize * 3
	// Chop off the last few bytes: guaranteed to land inside the third
	// entry's payload or checksum, never on an entry boundary.
	if err := os.Truncate(segPath, fullSize-3); err != nil {
		t.Fatalf("truncate torn tail: %v", err)
	}

	reopened, err := NewSpool(SpoolConfig{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: entrySize * 10})
	if err != nil {
		t.Fatalf("NewSpool over torn tail returned an error, want silent recovery: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if got := reopened.Len(); got != 2 {
		t.Fatalf("Len() after torn tail = %d, want 2", got)
	}
	for i := 0; i < 2; i++ {
		got, ok, err := reopened.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		if want := fixedPayload(i); !bytes.Equal(got, want) {
			t.Fatalf("entry %d = %q, want %q", i, got, want)
		}
		if err := reopened.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	if _, ok, err := reopened.Peek(); err != nil || ok {
		t.Fatalf("Peek after draining survivors: ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestSpoolTornTailBadChecksum(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	sp := mustNewSpool(t, dir, 1<<20, entrySize*10)

	for i := 0; i < 3; i++ {
		if err := sp.Append(fixedPayload(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := sp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segPath := filepath.Join(dir, segmentFileName(0))
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	// Flip the first byte of the third entry's payload (right after its
	// header) so its CRC no longer matches.
	thirdPayloadStart := int(entrySize)*2 + entryHeaderSize
	data[thirdPayloadStart] ^= 0xFF
	if err = os.WriteFile(segPath, data, 0o600); err != nil {
		t.Fatalf("rewrite corrupted segment: %v", err)
	}

	reopened, err := NewSpool(SpoolConfig{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: entrySize * 10})
	if err != nil {
		t.Fatalf("NewSpool over bad checksum returned an error, want silent recovery: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if got := reopened.Len(); got != 2 {
		t.Fatalf("Len() after bad checksum = %d, want 2", got)
	}
	for i := 0; i < 2; i++ {
		got, ok, err := reopened.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		if want := fixedPayload(i); !bytes.Equal(got, want) {
			t.Fatalf("entry %d = %q, want %q", i, got, want)
		}
		if err := reopened.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	if _, ok, err := reopened.Peek(); err != nil || ok {
		t.Fatalf("Peek after draining survivors: ok=%v err=%v, want false/nil", ok, err)
	}
}

// TestSpoolAbsurdLengthPrefix hand-writes a segment containing only a header
// claiming a huge payload length, with no payload bytes behind it at all.
// Recovery must reject it without trying to allocate anything close to that
// length — it checks the claimed length against what is actually left in
// the file before ever calling make([]byte, length).
func TestSpoolAbsurdLengthPrefix(t *testing.T) {
	dir := t.TempDir()
	segPath := filepath.Join(dir, segmentFileName(0))

	header := make([]byte, entryHeaderSize)
	byteOrder.PutUint32(header[0:4], 0xFFFFFFF0) // ~4 GiB claimed length
	byteOrder.PutUint32(header[4:8], 0)
	if err := os.WriteFile(segPath, header, 0o600); err != nil {
		t.Fatalf("write hand-crafted segment: %v", err)
	}

	done := make(chan struct{})
	var sp *Spool
	var err error
	go func() {
		sp, err = NewSpool(SpoolConfig{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: 1 << 20})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("NewSpool did not return promptly; suspect it tried to allocate the absurd length")
	}
	if err != nil {
		t.Fatalf("NewSpool over absurd length prefix: %v", err)
	}
	t.Cleanup(func() { _ = sp.Close() })

	if got := sp.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
	if got := sp.Bytes(); got != 0 {
		t.Errorf("Bytes() = %d, want 0", got)
	}
	if _, statErr := os.Stat(segPath); !os.IsNotExist(statErr) {
		t.Errorf("hand-crafted segment survived recovery, want it removed (0 valid entries): stat err = %v", statErr)
	}
}

// TestSpoolMaxBytesEviction sizes segments to hold exactly one entry each,
// so the arithmetic on which segments survive is exact.
func TestSpoolMaxBytesEviction(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	maxBytes := entrySize * 3
	sp := mustNewSpool(t, dir, maxBytes, entrySize) // one entry per segment
	t.Cleanup(func() { _ = sp.Close() })

	const n = 6
	for i := 0; i < n; i++ {
		if err := sp.Append(fixedPayload(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	if got, want := counterValue(t, sp.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonSpoolEvicted)), float64(3); got != want {
		t.Fatalf("RecordsDropped{reason=spool_evicted} = %v, want %v", got, want)
	}
	if got, want := sp.Len(), 3; got != want {
		t.Fatalf("Len() = %d, want %d", got, want)
	}
	if got := sp.Bytes(); got > maxBytes {
		t.Fatalf("Bytes() = %d, exceeds MaxBytes %d", got, maxBytes)
	}

	// The newest three survive; the oldest three were dropped.
	for i := 3; i < n; i++ {
		got, ok, err := sp.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		if want := fixedPayload(i); !bytes.Equal(got, want) {
			t.Fatalf("surviving entry = %q, want %q", got, want)
		}
		if err := sp.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
	if _, ok, err := sp.Peek(); err != nil || ok {
		t.Fatalf("Peek after draining survivors: ok=%v err=%v, want false/nil", ok, err)
	}
}

// TestSpoolOversizedPayload decides the case left open by the spec: a
// payload bigger than SegmentBytes is stored, in a segment of its own,
// rather than rejected — refusing it would throw away data this package
// exists to protect, and the collector already bounds batch size
// independently of what the spool considers plausible.
func TestSpoolOversizedPayload(t *testing.T) {
	dir := t.TempDir()
	sp := mustNewSpool(t, dir, 1<<20, 20) // SegmentBytes smaller than the big payload
	t.Cleanup(func() { _ = sp.Close() })

	big := bytes.Repeat([]byte("x"), 50)
	small := []byte("hi")

	if err := sp.Append(big); err != nil {
		t.Fatalf("Append(big): %v", err)
	}
	if err := sp.Append(small); err != nil {
		t.Fatalf("Append(small): %v", err)
	}

	segments := 0
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, de := range entries {
		if _, ok := parseSegmentSeq(de.Name()); ok {
			segments++
		}
	}
	if segments != 2 {
		t.Fatalf("segment file count = %d, want 2 (oversized entry forced its own segment)", segments)
	}

	got, ok, err := sp.Peek()
	if err != nil || !ok || !bytes.Equal(got, big) {
		t.Fatalf("Peek(big) = %q ok=%v err=%v, want %q ok=true err=nil", got, ok, err, big)
	}
	if err = sp.Release(); err != nil {
		t.Fatalf("Release(big): %v", err)
	}
	got, ok, err = sp.Peek()
	if err != nil || !ok || !bytes.Equal(got, small) {
		t.Fatalf("Peek(small) = %q ok=%v err=%v, want %q ok=true err=nil", got, ok, err, small)
	}
	if err := sp.Release(); err != nil {
		t.Fatalf("Release(small): %v", err)
	}
}

func TestSpoolConcurrentAppendPeekRelease(t *testing.T) {
	dir := t.TempDir()
	entrySize := int64(len(encodeEntry(fixedPayload(0))))
	sp := mustNewSpool(t, dir, 1<<20, entrySize*3)
	t.Cleanup(func() { _ = sp.Close() })

	const n = 80

	var wg sync.WaitGroup
	wg.Add(2)

	var appendErr error
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if err := sp.Append(fixedPayload(i)); err != nil {
				appendErr = err
				return
			}
		}
	}()

	var drainErr error
	go func() {
		defer wg.Done()
		released := 0
		for released < n {
			got, ok, err := sp.Peek()
			if err != nil {
				drainErr = fmt.Errorf("Peek(%d): %w", released, err)
				return
			}
			if !ok {
				time.Sleep(time.Millisecond)
				continue
			}
			if want := fixedPayload(released); !bytes.Equal(got, want) {
				drainErr = fmt.Errorf("entry %d = %q, want %q", released, got, want)
				return
			}
			if err := sp.Release(); err != nil {
				drainErr = fmt.Errorf("Release(%d): %w", released, err)
				return
			}
			released++
		}
	}()

	wg.Wait()
	if appendErr != nil {
		t.Fatalf("appender: %v", appendErr)
	}
	if drainErr != nil {
		t.Fatalf("drainer: %v", drainErr)
	}
	if got := sp.Len(); got != 0 {
		t.Errorf("Len() after full drain = %d, want 0", got)
	}
}

func TestSpoolRecoveryDirectoryEdgeCases(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		dir := t.TempDir()
		sp := mustNewSpool(t, dir, 1<<20, 1<<20)
		t.Cleanup(func() { _ = sp.Close() })
		if got := sp.Len(); got != 0 {
			t.Errorf("Len() = %d, want 0", got)
		}
	})

	t.Run("unrelated file", func(t *testing.T) {
		dir := t.TempDir()
		unrelated := filepath.Join(dir, "notes.txt")
		if err := os.WriteFile(unrelated, []byte("not a segment"), 0o600); err != nil {
			t.Fatalf("write unrelated file: %v", err)
		}

		sp := mustNewSpool(t, dir, 1<<20, 1<<20)
		t.Cleanup(func() { _ = sp.Close() })

		if got := sp.Len(); got != 0 {
			t.Errorf("Len() = %d, want 0", got)
		}
		if _, err := os.Stat(unrelated); err != nil {
			t.Errorf("unrelated file did not survive recovery: %v", err)
		}
	})
}
