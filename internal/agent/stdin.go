package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
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

	// buf is reused by readLine on the read goroutine (no lock needed: read
	// runs on exactly one goroutine, so no synchronization is required).
	buf []byte
}

// NewStdinSource wraps a reader as a Source.
//
// The reader is passed in rather than reaching for os.Stdin so tests need no
// process plumbing; cmd/agent will pass os.Stdin.
func NewStdinSource(r io.Reader) *StdinSource {
	s := &StdinSource{
		r:   r,
		buf: make([]byte, 0, 4096),
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
		line, consumed, err := s.readLine(br)
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

// readLine assembles one line from br, truncating at model.MaxMessageLen.
//
// It returns a fresh copy of the line with the terminator stripped, the count
// of bytes consumed from the reader (including the terminator and any discarded
// overflow), and any error. A final line with no trailing \n is still emitted.
func (s *StdinSource) readLine(br *bufio.Reader) (line []byte, consumed int64, err error) {
	s.buf = s.buf[:0]
	var totalBytes int64

	for {
		fragment, readErr := br.ReadSlice('\n')
		totalBytes += int64(len(fragment))

		// Accumulate into s.buf up to MaxMessageLen.
		if len(s.buf) < model.MaxMessageLen {
			space := model.MaxMessageLen - len(s.buf)
			if len(fragment) <= space {
				s.buf = append(s.buf, fragment...)
			} else {
				// This fragment would exceed the limit; take only what fits.
				s.buf = append(s.buf, fragment[:space]...)
				// A counter belongs here once the pipeline exists.
			}
		}
		// If len(s.buf) >= MaxMessageLen, just count bytes without appending.

		if readErr != nil {
			if errors.Is(readErr, bufio.ErrBufferFull) {
				// Buffer filled without finding \n; continue reading.
				continue
			}
			// io.EOF or real error.
			err = readErr
			break
		}

		// Found \n; stop reading.
		break
	}

	// Strip trailing \n and \r if present (but only from what we accumulated).
	if len(s.buf) > 0 && s.buf[len(s.buf)-1] == '\n' {
		s.buf = s.buf[:len(s.buf)-1]
	}
	if len(s.buf) > 0 && s.buf[len(s.buf)-1] == '\r' {
		s.buf = s.buf[:len(s.buf)-1]
	}

	// Return a fresh copy; s.buf is reused.
	line = make([]byte, len(s.buf))
	copy(line, s.buf)
	return line, totalBytes, err
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
