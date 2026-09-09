package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

func TestStdinSource_Name(t *testing.T) {
	s := NewStdinSource(strings.NewReader(""))
	if got := s.Name(); got != "stdin" {
		t.Errorf("Name() = %q, want %q", got, "stdin")
	}
}

func TestStdinSource_Run(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantLines []string
		wantErr   error
	}{
		{
			name:      "single line with newline",
			input:     "hello\n",
			wantLines: []string{"hello"},
		},
		{
			name:      "multiple lines",
			input:     "first\nsecond\nthird\n",
			wantLines: []string{"first", "second", "third"},
		},
		{
			name:      "final line without newline",
			input:     "first\nsecond",
			wantLines: []string{"first", "second"},
		},
		{
			name:      "empty lines",
			input:     "first\n\nthird\n",
			wantLines: []string{"first", "", "third"},
		},
		{
			name:      "only empty lines",
			input:     "\n\n\n",
			wantLines: []string{"", "", ""},
		},
		{
			name:      "CRLF input",
			input:     "windows\r\nstyle\r\nlines\r\n",
			wantLines: []string{"windows", "style", "lines"},
		},
		{
			name:      "mixed line endings",
			input:     "unix\nwindows\r\nmac\rend",
			wantLines: []string{"unix", "windows", "mac\rend"},
		},
		{
			name:      "empty input",
			input:     "",
			wantLines: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStdinSource(strings.NewReader(tt.input))
			defer s.Close()

			ctx := context.Background()
			out := make(chan Line, 10)
			errCh := make(chan error, 1)

			go func() {
				errCh <- s.Run(ctx, out)
				close(out)
			}()

			var got []string
			for line := range out {
				got = append(got, string(line.Bytes))
				// Verify Line fields.
				if line.Source != "stdin" {
					t.Errorf("line.Source = %q, want %q", line.Source, "stdin")
				}
				if line.Time.IsZero() {
					t.Error("line.Time is zero")
				}
				if line.Cursor.File != (FileID{}) {
					t.Errorf("line.Cursor.File = %v, want zero FileID", line.Cursor.File)
				}
			}

			err := <-errCh
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Run() error = %v, want %v", err, tt.wantErr)
			}

			if len(got) != len(tt.wantLines) {
				t.Fatalf("got %d lines, want %d\nlines: %v", len(got), len(tt.wantLines), got)
			}
			for i := range got {
				if got[i] != tt.wantLines[i] {
					t.Errorf("line %d: got %q, want %q", i, got[i], tt.wantLines[i])
				}
			}
		})
	}
}

func TestStdinSource_Truncation(t *testing.T) {
	// Build a line longer than MaxMessageLen followed by a normal line.
	long := strings.Repeat("x", model.MaxMessageLen+1000)
	input := long + "\nnext line\n"

	s := NewStdinSource(strings.NewReader(input))
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	// First line: truncated to MaxMessageLen.
	line1 := <-out
	if len(line1.Bytes) != model.MaxMessageLen {
		t.Fatalf("truncated line length = %d, want %d", len(line1.Bytes), model.MaxMessageLen)
	}
	if string(line1.Bytes) != strings.Repeat("x", model.MaxMessageLen) {
		t.Error("truncated line content incorrect")
	}
	if line1.Cursor.Start != 0 {
		t.Errorf("truncated line Start = %d, want 0", line1.Cursor.Start)
	}

	// Second line: should be read correctly after discarding overflow.
	line2 := <-out
	if string(line2.Bytes) != "next line" {
		t.Errorf("line after truncation = %q, want %q", string(line2.Bytes), "next line")
	}

	// The Start of line2 should equal the total consumed bytes of line1 (including overflow and \n).
	expectedLine2Start := int64(model.MaxMessageLen + 1000 + 1) // All 'x's + overflow + \n
	if line2.Cursor.Start != expectedLine2Start {
		t.Errorf("line after truncation Start = %d, want %d", line2.Cursor.Start, expectedLine2Start)
	}

	if err := <-errCh; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestStdinSource_Offset(t *testing.T) {
	input := "ab\ndefg\n\nhi"
	// Offsets:
	// "ab\n"      -> offset 3
	// "defg\n"    -> offset 8
	// "\n"        -> offset 9
	// "hi"        -> offset 11
	wantOffsets := []int64{3, 8, 9, 11}

	s := NewStdinSource(strings.NewReader(input))
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	var offsets []int64
	for line := range out {
		offsets = append(offsets, line.Cursor.Offset)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	if len(offsets) != len(wantOffsets) {
		t.Fatalf("got %d offsets, want %d", len(offsets), len(wantOffsets))
	}
	for i := range offsets {
		if offsets[i] != wantOffsets[i] {
			t.Errorf("offset %d = %d, want %d", i, offsets[i], wantOffsets[i])
		}
	}

	// Verify monotonic.
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Errorf("offsets not monotonic: %d at index %d <= %d at index %d",
				offsets[i], i, offsets[i-1], i-1)
		}
	}
}

func TestStdinSource_CursorStart(t *testing.T) {
	input := "ab\ndefg\n\nhi"
	// Expected (Start, Offset) pairs:
	// "ab\n"      -> Start 0, Offset 3
	// "defg\n"    -> Start 3, Offset 8
	// "\n"        -> Start 8, Offset 9
	// "hi"        -> Start 9, Offset 11
	wantPairs := []struct {
		start  int64
		offset int64
	}{
		{0, 3},
		{3, 8},
		{8, 9},
		{9, 11},
	}

	s := NewStdinSource(strings.NewReader(input))
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	var pairs []struct {
		start  int64
		offset int64
	}
	for line := range out {
		pairs = append(pairs, struct {
			start  int64
			offset int64
		}{line.Cursor.Start, line.Cursor.Offset})
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	if len(pairs) != len(wantPairs) {
		t.Fatalf("got %d lines, want %d", len(pairs), len(wantPairs))
	}
	for i := range pairs {
		if pairs[i].start != wantPairs[i].start || pairs[i].offset != wantPairs[i].offset {
			t.Errorf("line %d: got (Start=%d, Offset=%d), want (Start=%d, Offset=%d)",
				i, pairs[i].start, pairs[i].offset, wantPairs[i].start, wantPairs[i].offset)
		}
	}
}

func TestStdinSource_Cancellation(t *testing.T) {
	// Use a reader that blocks forever so we can test cancellation.
	pr, pw := io.Pipe()
	defer pw.Close()
	defer pr.Close()

	s := NewStdinSource(pr)
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan Line, 1)
	errCh := make(chan error, 1)

	start := time.Now()
	go func() {
		errCh <- s.Run(ctx, out)
	}()

	err := <-errCh
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run() error = %v, want context.DeadlineExceeded", err)
	}

	// Verify it returned promptly (within 200ms of the 50ms timeout).
	if elapsed > 250*time.Millisecond {
		t.Errorf("Run() took %v to return after cancellation, want < 250ms", elapsed)
	}
}

func TestStdinSource_ReadError(t *testing.T) {
	readErr := errors.New("synthetic read error")
	r := &failingReader{err: readErr}

	s := NewStdinSource(r)
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	err := <-errCh
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if !errors.Is(err, readErr) {
		t.Errorf("Run() error = %v, does not wrap %v", err, readErr)
	}
}

func TestStdinSource_Close(t *testing.T) {
	t.Run("close closeable reader", func(t *testing.T) {
		cr := &closeTracker{Reader: strings.NewReader("test\n")}
		s := NewStdinSource(cr)

		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
		if !cr.closed {
			t.Error("Close() did not close the underlying reader")
		}

		// Second close should be idempotent.
		if err := s.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
		if cr.closeCount != 1 {
			t.Errorf("Close() called %d times on reader, want 1", cr.closeCount)
		}
	})

	t.Run("close non-closeable reader", func(t *testing.T) {
		r := strings.NewReader("test\n")
		s := NewStdinSource(r)

		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}

		// Should be safe to call twice.
		if err := s.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
	})
}

func TestStdinSource_BytesOwnership(t *testing.T) {
	// Verify that each emitted Line.Bytes is a distinct allocation.
	input := "first\nsecond\nthird\n"

	s := NewStdinSource(strings.NewReader(input))
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 10)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	line1 := <-out
	// Mutate the first line's bytes.
	for i := range line1.Bytes {
		line1.Bytes[i] = 'X'
	}

	line2 := <-out
	// The second line should be unaffected.
	if string(line2.Bytes) != "second" {
		t.Errorf("line2.Bytes = %q after mutating line1, want %q", string(line2.Bytes), "second")
	}

	// Drain remaining.
	for range out {
	}
	if err := <-errCh; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestStdinSource_NonNilBytes(t *testing.T) {
	// Empty lines should produce non-nil empty slices.
	input := "\n"

	s := NewStdinSource(strings.NewReader(input))
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	line := <-out
	if line.Bytes == nil {
		t.Error("line.Bytes is nil, want non-nil empty slice")
	}
	if len(line.Bytes) != 0 {
		t.Errorf("line.Bytes length = %d, want 0", len(line.Bytes))
	}

	if err := <-errCh; err != nil {
		t.Errorf("Run() error = %v, want nil", err)
	}
}

func TestStdinSource_SlowConsumer(t *testing.T) {
	// This test verifies that lines are not dropped when the consumer is slow.
	// We send more lines than the internal buffer capacity (lineHandoff=8)
	// and read them with a small delay, ensuring all lines arrive in order.

	// Build input with more lines than lineHandoff.
	numLines := 20
	var input strings.Builder
	for i := 0; i < numLines; i++ {
		fmt.Fprintf(&input, "line%d\n", i)
	}

	s := NewStdinSource(strings.NewReader(input.String()))
	defer s.Close()

	ctx := context.Background()
	out := make(chan Line, 1) // Small buffer to force slow consumption.
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.Run(ctx, out)
		close(out)
	}()

	// Consume lines with a small delay, tracking them.
	var received []string
	for line := range out {
		received = append(received, string(line.Bytes))
		time.Sleep(5 * time.Millisecond) // Slow consumer.
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	// Verify all lines arrived.
	if len(received) != numLines {
		t.Fatalf("got %d lines, want %d", len(received), numLines)
	}

	// Verify no gaps and correct order.
	for i := 0; i < numLines; i++ {
		want := fmt.Sprintf("line%d", i)
		if received[i] != want {
			t.Errorf("line %d: got %q, want %q", i, received[i], want)
		}
	}
}

// failingReader is a reader that returns an error.
type failingReader struct {
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// closeTracker tracks Close calls.
type closeTracker struct {
	io.Reader
	closed     bool
	closeCount int
}

func (c *closeTracker) Close() error {
	c.closed = true
	c.closeCount++
	return nil
}
