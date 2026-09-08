package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// defaultPollInterval is how often the tail source checks a file for new data
// once it has caught up to EOF, used when TailConfig.PollInterval is zero.
// 250ms is frequent enough that a line shows up promptly without the poll loop
// competing meaningfully with the writer for disk I/O.
const defaultPollInterval = 250 * time.Millisecond

// errTailClosed means Close was called before Run ever opened the file. Run
// reports it instead of silently opening a file nobody will ever close.
var errTailClosed = errors.New("tail source: closed before the file was opened")

// TailConfig configures a TailSource.
type TailConfig struct {
	// Path is the file to tail. It becomes Name(), which is the checkpoint key,
	// so it is the path the operator configured, not whatever the path resolves
	// to after a rotation.
	Path string
	// PollInterval is how often to look for new data once the source has caught
	// up to EOF. Zero uses defaultPollInterval.
	PollInterval time.Duration
	// StartOffset is where to begin reading. Zero reads the file from the
	// beginning; subtask 4 will supply a resumed value from the checkpoint
	// store.
	StartOffset int64
}

// TailSource reads a file from a starting offset, emitting complete lines and
// waiting for more when it reaches EOF.
//
// This is steady-state tailing only: open, read to EOF, poll, repeat. Rotation
// and truncation — noticing that the FileID under the path changed, reopening,
// handling a file that shrank — are the next subtask's job and are
// deliberately absent here. A rotation while this source is running is read as
// more of the same file until Close.
type TailSource struct {
	path         string
	pollInterval time.Duration
	startOffset  int64

	mu     sync.Mutex
	file   *os.File
	closed bool

	closeOnce sync.Once
	closeErr  error
}

// NewTailSource validates cfg and returns a TailSource ready to Run.
//
// It does not open the file: the file may not exist yet, which is ordinary
// startup ordering for a log source configured before its writer, and it is
// Run's job to wait for that, not NewTailSource's job to fail on it.
//
// It does probe that this platform can report a FileID at all. fileIDFromInfo
// reports false only depending on GOOS (see fileid_other.go), never on which
// file is stat'd, so any real file answers the question — the current
// directory always exists. Failing here means a platform the agent cannot
// tail correctly is caught at startup, instead of running with a Cursor.File
// that is silently always zero and can never tell rotation apart from a
// truncate.
func NewTailSource(cfg TailConfig) (*TailSource, error) {
	if cfg.Path == "" {
		return nil, errors.New("tail source: path is required")
	}

	info, err := os.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("tail source: probing file identity support: %w", err)
	}
	if _, ok := fileIDFromInfo(info); !ok {
		return nil, errors.New("tail source: this platform cannot report file identity (dev/inode); tailing is unsupported here")
	}

	interval := cfg.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}

	return &TailSource{
		path:         cfg.Path,
		pollInterval: interval,
		startOffset:  cfg.StartOffset,
	}, nil
}

// Name returns the configured path, not the resolved one: it is the
// checkpoint key, and the resolved path changes on rotation while the
// configured one is what the operator wrote.
func (s *TailSource) Name() string {
	return s.path
}

// Run tails the file until ctx is canceled.
//
// It returns only ctx.Err(): a tailed file has no clean end the way stdin's
// EOF is one, so there is no successful nil-return path here. Backpressure on
// out blocks rather than sheds — the bytes are still on disk behind the
// reader, so stalling loses nothing, while dropping would lose a line that
// was never at risk.
func (s *TailSource) Run(ctx context.Context, out chan<- Line) error {
	file, fid, err := s.awaitFile(ctx)
	if err != nil {
		return err
	}
	if !s.setFile(file) {
		return errTailClosed
	}

	assembler := newLineAssembler()
	offset := s.startOffset

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		if err := s.readToEOF(ctx, out, file, assembler, fid, &offset); err != nil {
			return err
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// readToEOF seeks to *offset and emits every complete line up to the current
// end of file, advancing *offset past each one. It never emits a line that
// has not seen its terminator, and never advances *offset past a fragment
// still waiting for one: a restart that resumed from a mid-line offset would
// permanently split the record in two, which is the one mistake this subtask
// exists to avoid.
func (s *TailSource) readToEOF(ctx context.Context, out chan<- Line, file *os.File, assembler *lineAssembler, fid FileID, offset *int64) error {
	if _, err := file.Seek(*offset, io.SeekStart); err != nil {
		return fmt.Errorf("tail %s: seek: %w", s.path, err)
	}
	// Re-seeking every cycle means a held fragment is simply re-read from its
	// start rather than resumed mid-buffer. That keeps "offset only advances
	// past complete lines" trivially true instead of something bufio state has
	// to preserve across polls.
	br := bufio.NewReader(file)

	for {
		start := *offset
		line, consumed, terminated, readErr := assembler.next(br)

		if terminated {
			if err := s.emit(ctx, out, fid, start, *offset+consumed, line); err != nil {
				return err
			}
			*offset += consumed
			continue
		}

		// The assembler only returns without a terminator when it hit an error;
		// terminated=true is the only other exit from next.
		if !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("tail %s: read: %w", s.path, readErr)
		}

		if len(line) >= model.MaxMessageLen {
			// The held fragment hit its bound. Waiting longer for a terminator
			// would mean an unbounded wait fed by whatever the writer appends
			// next, so emit the truncated line now and treat whatever comes after
			// it as unrelated, the same tradeoff truncation already makes for a
			// line that terminates late instead of never.
			if err := s.emit(ctx, out, fid, start, *offset+consumed, line); err != nil {
				return err
			}
			*offset += consumed
		}
		// Otherwise this is a genuine partial line: leave *offset untouched so
		// the next cycle re-reads this same fragment from its start once more
		// data has arrived.

		return nil // Reached EOF; the caller will poll and retry.
	}
}

// emit sends one line on out, blocking until it is delivered or ctx is
// canceled.
func (s *TailSource) emit(ctx context.Context, out chan<- Line, fid FileID, start, offset int64, bytes []byte) error {
	l := Line{
		Source: s.path,
		Time:   time.Now(),
		Bytes:  bytes,
		Cursor: Cursor{
			Start:  start,
			Offset: offset,
			File:   fid,
		},
	}
	select {
	case out <- l:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// awaitFile opens s.path, polling at s.pollInterval while the file does not
// exist yet. Log files routinely postdate the agent configured to tail them,
// so a missing file here is normal startup ordering, not a fault to report.
func (s *TailSource) awaitFile(ctx context.Context) (*os.File, FileID, error) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		file, err := os.Open(s.path)
		if err == nil {
			info, statErr := file.Stat()
			if statErr != nil {
				_ = file.Close()
				return nil, FileID{}, fmt.Errorf("tail %s: stat: %w", s.path, statErr)
			}
			fid, ok := fileIDFromInfo(info)
			if !ok {
				_ = file.Close()
				return nil, FileID{}, fmt.Errorf("tail %s: could not determine file identity", s.path)
			}
			return file, fid, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, FileID{}, fmt.Errorf("tail %s: open: %w", s.path, err)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, FileID{}, ctx.Err()
		}
	}
}

// setFile records the open file so Close can release it. It reports false,
// closing the file itself, if Close already ran: that only happens when a
// caller closes a TailSource whose Run is still waiting for the file to
// appear, and the file must not outlive the Close that was meant to release
// everything this source holds.
func (s *TailSource) setFile(f *os.File) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = f.Close()
		return false
	}
	s.file = f
	return true
}

// Close closes the tailed file descriptor, if one is open.
//
// It is idempotent and safe to call whether or not Run has been started, and
// safe to call after Run has returned.
func (s *TailSource) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		f := s.file
		s.file = nil
		s.mu.Unlock()
		if f != nil {
			s.closeErr = f.Close()
		}
	})
	return s.closeErr
}
