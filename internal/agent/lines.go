package agent

import (
	"bufio"
	"errors"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// lineBufCap is the initial capacity of a lineAssembler's reused buffer. It is
// sized for an ordinary line so most reads never need to grow it; growth past
// this is normal, not exceptional, and is itself bounded by model.MaxMessageLen.
const lineBufCap = 4096

// lineAssembler reads terminated lines from a *bufio.Reader, bounded at
// model.MaxMessageLen.
//
// It exists because stdin and the tail source share every byte-level rule of
// line assembly — ReadSlice in a loop, truncation at the bound, CRLF stripping
// — and differ in exactly one thing: whether an unterminated final line should
// ever be emitted. That is a policy decision the two sources make oppositely
// (stdin's EOF is a real end of input; the tail source's is a writer mid-flush),
// so it belongs to the caller, which is why next reports terminated separately
// from err instead of deciding for it.
type lineAssembler struct {
	// buf is reused across calls to next. Callers drive it from a single
	// goroutine each, so no synchronization is required here.
	buf []byte
}

// newLineAssembler returns an assembler ready to read from any *bufio.Reader.
func newLineAssembler() *lineAssembler {
	return &lineAssembler{buf: make([]byte, 0, lineBufCap)}
}

// next reads one line from br into a fresh copy, with its terminator stripped.
//
// It returns the line, the number of bytes consumed from br (including the
// terminator and any overflow discarded past model.MaxMessageLen), whether the
// line was newline-terminated, and any read error. line is never nil, even at
// length zero, and is always a fresh allocation distinct from br's internal
// buffer: callers hand the result on as Line.Bytes, which downstream code
// retains past this call, and a slice aliasing a reused buffer would corrupt
// on the next read.
//
// This uses ReadSlice in a loop rather than bufio.Scanner because Scanner's
// 64KB token limit collides with model.MaxMessageLen and errors exactly where
// truncation is wanted instead of truncating. It is not ReadBytes/ReadString
// because those allocate without bound on a line that never terminates, which
// is the one input this function must survive without blowing up memory.
func (a *lineAssembler) next(br *bufio.Reader) (line []byte, consumed int64, terminated bool, err error) {
	a.buf = a.buf[:0]
	var totalBytes int64

	for {
		fragment, readErr := br.ReadSlice('\n')
		totalBytes += int64(len(fragment))

		// Accumulate into buf up to MaxMessageLen; bytes beyond that are counted
		// in consumed but never stored — that is the truncation.
		if len(a.buf) < model.MaxMessageLen {
			space := model.MaxMessageLen - len(a.buf)
			if len(fragment) <= space {
				a.buf = append(a.buf, fragment...)
			} else {
				a.buf = append(a.buf, fragment[:space]...)
			}
		}

		if readErr != nil {
			if errors.Is(readErr, bufio.ErrBufferFull) {
				// The line continues past bufio's internal buffer; ReadSlice
				// cannot return until it sees \n or drains further, so keep
				// reading rather than treating this as an error.
				continue
			}
			// io.EOF, or a real error: the line ends here, terminated or not.
			err = readErr
			break
		}

		// ReadSlice found \n without ErrBufferFull: the line is complete.
		terminated = true
		break
	}

	// Strip the terminator, but only if it actually landed in buf: a truncated
	// line's \n was counted as discarded overflow, never appended, so there is
	// nothing to strip in that case.
	if len(a.buf) > 0 && a.buf[len(a.buf)-1] == '\n' {
		a.buf = a.buf[:len(a.buf)-1]
	}
	if len(a.buf) > 0 && a.buf[len(a.buf)-1] == '\r' {
		a.buf = a.buf[:len(a.buf)-1]
	}

	line = make([]byte, len(a.buf))
	copy(line, a.buf)
	return line, totalBytes, terminated, err
}
