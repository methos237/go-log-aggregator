package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// StdinSource reads newline-delimited lines from a reader.
//
// It exists as a Source implementation rather than a one-shot function because
// the agent's checkpoint store and pipeline treat all sources uniformly: the
// source owns its goroutine and knows nothing about acks, and the fact that
// stdin has a single definite end is incidental.
type StdinSource struct {
	r      io.Reader
	closer io.Closer // nil if r does not implement io.Closer

	closeOnce sync.Once
	closeErr  error

	// assembler is driven only by the read goroutine (no lock needed: read runs
	// on exactly one goroutine, so no synchronization is required).
	assembler *lineAssembler
}

// NewStdinSource wraps a reader as a Source.
//
// The reader is passed in rather than reaching for os.Stdin so tests need no
// process plumbing; cmd/agent will pass os.Stdin.
func NewStdinSource(r io.Reader) *StdinSource {
	s := &StdinSource{
		r:         r,
		assembler: newLineAssembler(),
	}
	// Sniff for io.Closer once at construction so Close does not allocate.
	if c, ok := r.(io.Closer); ok {
		s.closer = c
	}
	return s
}

// Name returns "stdin", which is the checkpoint key and the source label on metrics.
func (s *StdinSource) Name() string {
	return "stdin"
}

// lineHandoff is the buffer capacity for the lines channel. It decouples
// the reader from the consumer for short stalls and is small because it is a
// handoff, not a queue — the real bounded queue is out, whose capacity comes
// from configuration.
const lineHandoff = 8

// Run reads lines until ctx is canceled or the reader reaches EOF.
//
// Run must be called at most once per StdinSource. A second call would put a
// second reader goroutine on the line buffer this source reuses, and the result
// would be silently interleaved lines rather than a panic or a race report. The
// agent constructs one source per origin and runs it once, so this costs nothing;
// it is stated because the alternative failure is invisible.
//
// os.Stdin.Read blocks and does NOT unblock on context cancellation, so Run
// cannot do the blocking read on its own goroutine. Instead it spawns one
// reader goroutine that sends lines on a small buffered channel, and Run
// selects between that channel and ctx.Done(). The reader goroutine may still
// be parked in Read when Run returns — that is an accepted, bounded cost:
// exactly one goroutine, for at most the life of the process, and Close
// unblocks it when the reader is closeable.
func (s *StdinSource) Run(ctx context.Context, out chan<- Line) error {
	done := make(chan struct{})
	defer close(done)

	lines := make(chan Line, lineHandoff)
	errCh := make(chan error, 1)

	go s.read(done, lines, errCh)

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				// Reader goroutine closed the channel after sending EOF or error.
				err := <-errCh
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("stdin read: %w", err)
			}
			// Forward the line to out.
			select {
			case out <- line:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// read is the reader goroutine: assemble lines from the reader and send them
// until EOF or an error.
//
// This goroutine may outlive Run when the reader is blocking: Close unblocks it
// if the reader is closeable, otherwise it stays parked until the process exits
// or the reader produces data. The channel send is guarded with a select on the
// done channel so that when Run has returned, the reader can abort rather than
// block forever trying to send on a drained channel.
func (s *StdinSource) read(done <-chan struct{}, lines chan<- Line, errCh chan<- error) {
	defer close(lines)

	br := bufio.NewReader(s.r)
	var offset int64

	for {
		start := offset
		// terminated is not consulted: stdin's EOF is a real end of input, so
		// the final line is emitted whether or not it had a trailing \n. That is
		// the one rule the tail source must NOT copy.
		line, consumed, _, err := s.assembler.next(br)
		offset += consumed

		if consumed > 0 {
			l := Line{
				Source: s.Name(),
				Time:   time.Now(),
				Bytes:  line,
				Cursor: Cursor{
					Start:  start,
					Offset: offset,
					File:   FileID{}, // Zero value for stdin.
				},
			}
			// Blocking send, but can be abandoned if Run has returned.
			select {
			case lines <- l:
			case <-done:
				return
			}
		}

		if err != nil {
			errCh <- err
			return
		}
	}
}

// Close releases the underlying reader if it implements io.Closer.
//
// It is idempotent, safe to call after Run returns, and safe to call
// concurrently with Run. Calling Close unblocks a goroutine parked in Read when
// the reader is closeable.
func (s *StdinSource) Close() error {
	s.closeOnce.Do(func() {
		if s.closer != nil {
			s.closeErr = s.closer.Close()
		}
	})
	return s.closeErr
}
