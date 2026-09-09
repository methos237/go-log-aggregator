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
	"github.com/jamespolk/go-log-aggregator/internal/observability"
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
	// Resume is where to continue from: a Cursor returned by
	// CheckpointStore.Get, or the zero Cursor for a source with no history,
	// which reads the file from the beginning. Offset alone is not enough to
	// resume correctly — File and Head are what let Run tell "the same file,
	// pick up at Offset" apart from "a rotation happened while this agent was
	// down" and "the same inode, but truncated and rewritten while down",
	// which Offset by itself cannot distinguish.
	Resume Cursor
	// Metrics records rotations, truncations, and missed generations. Nil
	// builds an unregistered set via NewMetrics(nil), which is what tests
	// want; production wiring supplies one built against the real registry.
	Metrics *Metrics
}

// TailSource reads a file from a resumed cursor, emitting complete lines and
// waiting for more when it reaches EOF.
//
// Beyond steady-state reading it detects and survives, without a gap and with
// bounded duplicates:
//
//   - Rename-and-recreate rotation: the path's inode changes under an fd this
//     source already has open. The old fd is drained to true EOF before the
//     switch, because an open fd keeps reading its own inode after the file
//     is renamed or unlinked out from under it — switching immediately would
//     silently drop whatever the writer flushed between the last read and the
//     rename.
//   - In-place truncation (copytruncate, or any writer that truncates and
//     rewrites within one poll interval): detected by a size shrink, or,
//     when the rewrite lands past the old offset so the size never appears
//     to shrink, by a head fingerprint that no longer matches. Resumes at
//     offset zero on the same fd.
//   - A missed generation: the checkpoint names a file that is not the one
//     now at the path, discovered before the first read after a restart.
//
// It never opens anything but the configured path: no globbing for rotated
// siblings, no chasing ".1" or ".gz" files. If the agent was down across a
// rotation, or two rotations land inside one poll interval, that generation's
// content is simply never read — Name() means exactly one file, always, and
// this is the accepted, counted cost of that.
type TailSource struct {
	path         string
	pollInterval time.Duration
	resume       Cursor
	metrics      *Metrics

	mu     sync.Mutex
	file   *os.File
	closed bool

	closeOnce sync.Once
	closeErr  error
}

// NewTailSource validates cfg and returns a TailSource ready to Run.
//
// cfg is taken by pointer rather than by value: Resume's embedded Cursor
// carries a FileID and a Fingerprint on top of Offset, which puts TailConfig
// at exactly the size gocritic's hugeParam check flags for pass-by-value. A
// nil cfg is treated as the zero TailConfig, which fails validation the same
// way an empty Path does, rather than panicking on the field access.
//
// It does not open the file: the file may not exist yet, which is ordinary
// startup ordering for a log source configured before its writer, and it is
// Run's job to wait for that, not NewTailSource's job to fail on it.
func NewTailSource(cfg *TailConfig) (*TailSource, error) {
	if cfg == nil {
		cfg = &TailConfig{}
	}
	if cfg.Path == "" {
		return nil, errors.New("tail source: path is required")
	}

	interval := cfg.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}

	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &TailSource{
		path:         cfg.Path,
		pollInterval: interval,
		resume:       cfg.Resume,
		metrics:      metrics,
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

	offset, head, err := s.resolveResumedCursor(file, fid)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		file, fid, offset, head, err = s.resolveState(ctx, out, assembler, file, fid, offset, head)
		if err != nil {
			return err
		}

		if err := s.readToEOF(ctx, out, file, assembler, fid, head, &offset, false); err != nil {
			return err
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// resolveResumedCursor decides where Run's very first read should start,
// given s.resume.
//
// This exists separately from resolveState's per-cycle check because it
// compares against what the checkpoint persisted from a previous process,
// not against what this process last saw — and because it must run before
// the very first read, not only when a later cycle notices a change. Without
// it, a resumed Offset past the file's current size would Seek past EOF
// (which succeeds), every subsequent read would return EOF, and the source
// would silently tail nothing forever.
func (s *TailSource) resolveResumedCursor(file *os.File, fid FileID) (int64, Fingerprint, error) {
	head, err := fingerprintHead(file)
	if err != nil {
		return 0, Fingerprint{}, fmt.Errorf("tail %s: fingerprint: %w", s.path, err)
	}

	resume := s.resume

	if !resume.File.IsZero() && resume.File != fid {
		// Decision 5: the checkpoint names a file that is not the one now at
		// the path. A rotation happened while this agent was down (or more
		// than one — that looks identical from here, since this source never
		// opens anything but the configured path and so never observed the
		// intermediate generations). Resuming at the persisted offset would
		// mean nothing in this file, so start at zero and count the loss
		// honestly instead of guessing at a byte position that has no
		// relationship to this file's content.
		s.metrics.GenerationsMissed.Inc()
		s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonMissedGeneration).Inc()
		return 0, head, nil
	}

	info, err := file.Stat()
	if err != nil {
		return 0, Fingerprint{}, fmt.Errorf("tail %s: stat: %w", s.path, err)
	}

	// Decision 6: a stale offset past the current size. Decision 4: the file
	// was truncated and rewritten past the old offset while this agent was
	// down, so size alone never shows a shrink — Differs reports false
	// whenever either fingerprint is zero (unknown file, or a file too short
	// to fingerprint), so this only fires when there is content to actually
	// compare.
	truncated := info.Size() < resume.Offset || resume.Head.Differs(head)
	if truncated {
		s.metrics.TruncationsDetected.Inc()
		return 0, head, nil
	}

	return resume.Offset, head, nil
}

// resolveState runs before every read, including the first one after
// resolveResumedCursor, and decides which of the tail source's transitions
// applies: steady state, rotation, or truncation. Rotation and truncation are
// always told apart by comparing os.Stat(s.path) — the inode currently at
// that name — against file.Stat() — the inode this fd is reading — never by
// comparing filenames, which every real rotation scheme leaves unchanged.
func (s *TailSource) resolveState(
	ctx context.Context,
	out chan<- Line,
	assembler *lineAssembler,
	file *os.File,
	fid FileID,
	offset int64,
	head Fingerprint,
) (*os.File, FileID, int64, Fingerprint, error) {
	pathInfo, err := os.Stat(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Nothing at the path right now: a writer's unlink-then-recreate
			// (or a rename-away-then-rename-back) hasn't reached "recreate"
			// yet. There is no new inode to compare against, so there is
			// nothing to decide this cycle; keep reading the fd already open
			// and let the next poll see whatever appears.
			return file, fid, offset, head, nil
		}
		return file, fid, offset, head, fmt.Errorf("tail %s: stat: %w", s.path, err)
	}
	pathID, ok := fileIDFromInfo(pathInfo)
	if !ok {
		return file, fid, offset, head, fmt.Errorf("tail %s: could not determine file identity", s.path)
	}

	if pathID != fid {
		return s.rotate(ctx, out, assembler, file, fid, head, offset)
	}

	fdInfo, err := file.Stat()
	if err != nil {
		return file, fid, offset, head, fmt.Errorf("tail %s: stat: %w", s.path, err)
	}
	newHead, err := fingerprintHead(file)
	if err != nil {
		return file, fid, offset, head, fmt.Errorf("tail %s: fingerprint: %w", s.path, err)
	}

	// Decision 3: a plain size shrink, same inode — copytruncate. Decision 4:
	// the size never shrank because the rewrite already reached past the old
	// offset, which only the fingerprint catches.
	truncated := fdInfo.Size() < offset || head.Differs(newHead)
	if truncated {
		s.metrics.TruncationsDetected.Inc()
		return file, fid, 0, newHead, nil
	}

	return file, fid, offset, newHead, nil
}

// rotate handles a path whose inode no longer matches the fd this source has
// open.
//
// It drains the old fd to true EOF before switching, which is the one
// property this subtask exists to guarantee: an open fd keeps reading its
// own inode after the file is renamed or unlinked, so whatever the writer
// flushed between the last read and the rename is still reachable through
// oldFile, and only through oldFile — switching first would lose it
// silently. It drains exactly once rather than continuing to poll oldFile:
// in a rename-and-recreate rotation nothing ever writes to that inode again,
// so one drain to EOF is already everything it will ever hold. If a
// misconfigured writer keeps appending to its own fd on the old inode after
// rotation, those bytes are lost — an accepted consequence of not chasing
// rotated files (Decision 1), not a bug in this drain.
func (s *TailSource) rotate(
	ctx context.Context,
	out chan<- Line,
	assembler *lineAssembler,
	oldFile *os.File,
	oldFid FileID,
	oldHead Fingerprint,
	offset int64,
) (*os.File, FileID, int64, Fingerprint, error) {
	// The drained lines carry oldHead, not the incoming file's fingerprint:
	// their offsets refer to the rotated-away inode, and a cursor's Head must
	// describe the file its Offset points into or a restart would compare a
	// fingerprint against the wrong generation.
	//
	// final=true: nothing will ever append to oldFile's inode again, so a
	// fragment still held when this hits true EOF is final content, not a
	// writer mid-flush. That is the opposite of readToEOF's steady-state
	// rule, where an unterminated line is left for the next poll because more
	// bytes may still be coming on the same fd.
	if err := s.readToEOF(ctx, out, oldFile, assembler, oldFid, oldHead, &offset, true); err != nil {
		return oldFile, oldFid, offset, Fingerprint{}, err
	}

	// Reuse awaitFile rather than a bare os.Open: it already polls for the
	// path to exist, which covers the gap between a writer unlinking the old
	// name and recreating the new one, and it is already exercised by every
	// startup test.
	//
	// The old descriptor is closed only after the replacement has been registered,
	// never before. Closing first leaves s.file pointing at a closed descriptor
	// for as long as awaitFile takes -- which is unbounded, since it waits for the
	// new file to appear -- and a Close arriving in that window would close the
	// same descriptor twice and report a spurious "file already closed".
	newFile, newFid, err := s.awaitFile(ctx)
	if err != nil {
		_ = oldFile.Close()
		return nil, FileID{}, 0, Fingerprint{}, err
	}
	if !s.setFile(newFile) {
		_ = oldFile.Close()
		return nil, FileID{}, 0, Fingerprint{}, errTailClosed
	}
	_ = oldFile.Close()

	head, err := fingerprintHead(newFile)
	if err != nil {
		return newFile, newFid, 0, Fingerprint{}, fmt.Errorf("tail %s: fingerprint after rotation: %w", s.path, err)
	}

	s.metrics.RotationsDetected.Inc()

	return newFile, newFid, 0, head, nil
}

// readToEOF seeks to *offset and emits every complete line up to the current
// end of file, advancing *offset past each one. It never emits a line that
// has not seen its terminator, and never advances *offset past a fragment
// still waiting for one — except when final is set, for exactly the reason
// documented on rotate: a held fragment on a draining, rotated-away fd will
// never see a terminator because nothing will ever write to that inode
// again, so holding it would not defer emission, it would lose the line.
func (s *TailSource) readToEOF(ctx context.Context, out chan<- Line, file *os.File, assembler *lineAssembler, fid FileID, head Fingerprint, offset *int64, final bool) error {
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
			if err := s.emit(ctx, out, fid, head, start, *offset+consumed, line); err != nil {
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

		// Emit now if the held fragment hit its length bound (the existing
		// steady-state tradeoff: waiting longer for a terminator that may
		// never come would mean an unbounded wait), or if this is a final
		// drain and there is any fragment at all — see the comment above.
		if len(line) >= model.MaxMessageLen || (final && len(line) > 0) {
			if err := s.emit(ctx, out, fid, head, start, *offset+consumed, line); err != nil {
				return err
			}
			*offset += consumed
		}
		// Otherwise (not final, under the bound) this is a genuine partial
		// line: leave *offset untouched so the next cycle re-reads this same
		// fragment from its start once more data has arrived.

		return nil // Reached EOF; the caller will poll and retry, unless final.
	}
}

// emit sends one line on out, blocking until it is delivered or ctx is
// canceled.
//
// head must describe the file that offset refers to, not whatever is at the
// path right now. It travels with the line because the checkpoint persists the
// cursor of the last acked record, and Head is the field that lets the next
// process tell "the same inode, resume at Offset" from "the same inode, but its
// contents were replaced while we were down" — a distinction inode and size
// cannot make. A line emitted with a zero Head silently disables that check on
// the next restart.
func (s *TailSource) emit(ctx context.Context, out chan<- Line, fid FileID, head Fingerprint, start, offset int64, bytes []byte) error {
	l := Line{
		Source: s.path,
		Time:   time.Now(),
		Bytes:  bytes,
		Cursor: Cursor{
			Start:  start,
			Offset: offset,
			File:   fid,
			Head:   head,
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
// rotate also uses this to wait out the gap between a writer unlinking the
// old name and recreating the new one.
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
// closing the file itself, if Close already ran: that happens when a caller
// closes a TailSource whose Run is still waiting for a file (the initial one,
// or the replacement after a rotation), and the file must not outlive the
// Close that was meant to release everything this source holds.
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
