package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
)

// checkpointVersion is the schema written by this package. Load rejects any
// other value instead of guessing at a migration: a future format change
// must be a loud error, not a silent misparse of fields that happen to still
// unmarshal.
const checkpointVersion = 1

// checkpointFile is the on-disk shape of a checkpoint. The cursor's own json
// tags (see source.go) are the per-source format; adding an in-memory-only
// field to Cursor is therefore a file-format change and must be tagged
// `json:"-"`.
type checkpointFile struct {
	Version int               `json:"version"`
	Cursors map[string]Cursor `json:"cursors"`
}

// CheckpointStore persists per-source cursors so a restarted agent knows
// where to resume each source.
//
// The store is passive about what it is told: it does not decide when a
// cursor is safe to write. That decision belongs to the caller, and it is
// the one that makes the phase's no-gaps guarantee hold. The caller MUST
// call Set only with a cursor for records the collector has acknowledged,
// never merely read. If it checkpoints on read instead, a crash between the
// read and the ack loses those lines forever, silently converting this
// store's replay-on-restart design into a lossy one — nothing in this type
// would fail, and nothing would fail a test, because the store has no way to
// know the difference. Read-but-unacked data is always safe to lose from the
// checkpoint's point of view: it is re-read and re-sent on restart, and
// storage deduplicates it on (stream_id, seq, time), which is why Start must
// be the value that regenerates an identical seq on replay.
type CheckpointStore struct {
	path string

	mu      sync.Mutex
	cursors map[string]Cursor
}

// NewCheckpointStore returns a store backed by path. It does not read the
// file; call Load.
func NewCheckpointStore(path string) *CheckpointStore {
	return &CheckpointStore{
		path:    path,
		cursors: make(map[string]Cursor),
	}
}

// Load reads persisted cursors. A missing file is not an error: a first run
// has no checkpoint, and that must be indistinguishable from a normal empty
// start.
//
// A file that exists but fails to parse, carries an unknown schema version,
// or contains an invalid cursor returns an error instead of falling back to
// an empty set. Starting from zero after corruption either replays
// everything or skips everything depending on what the sources still have on
// disk, and both are worse than refusing to start: refusing lets the
// operator delete the file deliberately, which is a decision they should
// make consciously rather than one made for them by a parse failure.
func (s *CheckpointStore) Load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read checkpoint %s: %w", s.path, err)
	}

	var file checkpointFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("parse checkpoint %s: %w", s.path, err)
	}

	if file.Version != checkpointVersion {
		return fmt.Errorf("checkpoint %s: unsupported schema version %d (this build understands %d)",
			s.path, file.Version, checkpointVersion)
	}

	for source, c := range file.Cursors {
		if err := validateCursor(source, c); err != nil {
			return fmt.Errorf("checkpoint %s: %w", s.path, err)
		}
	}
	if file.Cursors == nil {
		file.Cursors = make(map[string]Cursor)
	}

	s.mu.Lock()
	s.cursors = file.Cursors
	s.mu.Unlock()

	return nil
}

// validateCursor rejects the cases a corrupt or hand-edited file could
// produce that would otherwise resume a source silently in the wrong place:
// a negative offset reads before the start of the file, Start greater than
// Offset describes a line that ends before it begins, an empty source name
// collides with every other unnamed source instead of failing to look up,
// and a negative Head.Len is not a length any fingerprintHead call could
// have produced.
func validateCursor(source string, c Cursor) error {
	switch {
	case source == "":
		return errors.New("empty source name")
	case c.Start < 0:
		return fmt.Errorf("source %q: negative start %d", source, c.Start)
	case c.Offset < 0:
		return fmt.Errorf("source %q: negative offset %d", source, c.Offset)
	case c.Start > c.Offset:
		return fmt.Errorf("source %q: start %d is greater than offset %d", source, c.Start, c.Offset)
	case c.Head.Len < 0:
		return fmt.Errorf("source %q: negative head length %d", source, c.Head.Len)
	}
	return nil
}

// Get returns the cursor recorded for a source and whether one exists.
func (s *CheckpointStore) Get(source string) (Cursor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cursors[source]
	return c, ok
}

// Set records acked progress for a source in memory. It is cheap and does
// not touch disk — the caller is expected to batch many Sets between each
// Commit, and a per-record fsync here would make the checkpoint the
// pipeline's bottleneck instead of the network or the broker.
func (s *CheckpointStore) Set(source string, c Cursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[source] = c
}

// Commit atomically persists the current cursors to the store's path.
//
// A reader — this process on its next Load, or an operator with cat — sees
// either the complete previous file or the complete new one, never a
// truncated or interleaved one, because the new content is written and
// synced under a temporary name in the same directory and only then renamed
// over the target; a rename is atomic only within a filesystem, which is why
// the temp file is not in os.TempDir.
//
// The lock is held only long enough to snapshot the in-memory cursors, not
// across the file I/O: holding a mutex across an fsync would stall every
// other source's Set and Get behind however long the disk takes, and the
// snapshot is all Commit needs since Set only ever replaces a whole entry.
func (s *CheckpointStore) Commit() error {
	s.mu.Lock()
	file := checkpointFile{Version: checkpointVersion, Cursors: maps.Clone(s.cursors)}
	s.mu.Unlock()

	data, err := json.MarshalIndent(&file, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	return writeAtomic(s.path, data, true)
}

// writeAtomic writes data to path via a temp file in the same directory and
// renames it into place, so a crash mid-write can only ever leave the previous
// complete file or the new complete one, never a torn one. The temp file is
// removed on any failure so a crash loop cannot fill the directory with
// abandoned partial files.
//
// sync=true also fsyncs the file before the rename and the directory after it.
// The file sync comes before the rename, not after: without it the rename can
// be durable while the content behind it is not, which after power loss looks
// like a valid file full of zeros rather than a missing file — far more
// dangerous, because a loader has no way to tell it apart from a real one. The
// directory sync is the step that is almost always omitted: the rename is
// metadata, and without it a crash can leave the directory entry still
// pointing at the old inode even though the new file's content is safely on
// disk. It is deliberately best-effort: some environments (a directory whose
// mode disallows opening it for read, some non-POSIX or networked
// filesystems) reject opening or syncing a directory at all, and the file
// content is already durable, so this degrades to a weaker crash guarantee
// instead of refusing to commit progress that is otherwise safely on disk.
//
// sync=false skips both fsyncs: the temp write and the rename are ordinary
// buffered operations, a small fraction of the cost of two fsyncs, at the
// cost of not knowing exactly when the update becomes durable. Spool.Release
// uses that on most calls — see cursorDurableInterval for what bounds the
// resulting replay window.
//
// The file is created 0o600 (os.CreateTemp's mode): offsets are not secret,
// but they name log paths on the host, and there is no reason for that to be
// world-readable.
func writeAtomic(path string, data []byte, sync bool) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", path, err)
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		return fmt.Errorf("write temp for %s: %w", path, err)
	}
	if sync {
		if err = tmp.Sync(); err != nil {
			return fmt.Errorf("sync temp for %s: %w", path, err)
		}
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp for %s: %w", path, err)
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("rename %s into place: %w", path, err)
	}

	if sync {
		if d, derr := os.Open(dir); derr == nil {
			_ = d.Sync()
			_ = d.Close()
		}
	}
	return nil
}
