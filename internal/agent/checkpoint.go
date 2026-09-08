package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// checkpointVersion is the schema written by this package. Load rejects any
// other value instead of guessing at a migration: a future format change
// must be a loud error, not a silent misparse of fields that happen to still
// unmarshal.
const checkpointVersion = 1

// checkpointFile is the on-disk shape of a checkpoint.
//
// Field names are lowercase in JSON deliberately: this file's whole purpose
// is to be inspected with `cat` while diagnosing a restart, and short
// lowercase keys read better in a terminal than exported Go names would.
type checkpointFile struct {
	Version int                       `json:"version"`
	Cursors map[string]checkpointDisk `json:"cursors"`
}

// checkpointDisk is one source's cursor as it appears on disk. It mirrors
// Cursor field-for-field rather than embedding it, so a change to Cursor's Go
// shape (say, adding an in-memory-only field) does not silently change the
// file format.
type checkpointDisk struct {
	Start  int64      `json:"start"`
	Offset int64      `json:"offset"`
	File   fileIDDisk `json:"file"`
}

// fileIDDisk is FileID's on-disk shape.
type fileIDDisk struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
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

	cursors := make(map[string]Cursor, len(file.Cursors))
	for source, c := range file.Cursors {
		cur, err := validateCursor(source, c)
		if err != nil {
			return fmt.Errorf("checkpoint %s: %w", s.path, err)
		}
		cursors[source] = cur
	}

	s.mu.Lock()
	s.cursors = cursors
	s.mu.Unlock()

	return nil
}

// validateCursor rejects the cases a corrupt or hand-edited file could
// produce that would otherwise resume a source silently in the wrong place:
// a negative offset reads before the start of the file, Start greater than
// Offset describes a line that ends before it begins, and an empty source
// name collides with every other unnamed source instead of failing to look
// up.
func validateCursor(source string, c checkpointDisk) (Cursor, error) {
	if source == "" {
		return Cursor{}, errors.New("empty source name")
	}
	if c.Start < 0 {
		return Cursor{}, fmt.Errorf("source %q: negative start %d", source, c.Start)
	}
	if c.Offset < 0 {
		return Cursor{}, fmt.Errorf("source %q: negative offset %d", source, c.Offset)
	}
	if c.Start > c.Offset {
		return Cursor{}, fmt.Errorf("source %q: start %d is greater than offset %d", source, c.Start, c.Offset)
	}
	return Cursor{
		Start:  c.Start,
		Offset: c.Offset,
		File:   FileID{Dev: c.File.Dev, Ino: c.File.Ino},
	}, nil
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
	file := checkpointFile{
		Version: checkpointVersion,
		Cursors: make(map[string]checkpointDisk, len(s.cursors)),
	}
	for source, c := range s.cursors {
		file.Cursors[source] = checkpointDisk{
			Start:  c.Start,
			Offset: c.Offset,
			File:   fileIDDisk{Dev: c.File.Dev, Ino: c.File.Ino},
		}
	}
	s.mu.Unlock()

	data, err := json.MarshalIndent(&file, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	return writeAtomic(s.path, data)
}

// writeAtomic writes data to path via a temp file in the same directory,
// syncing both the file and the directory before returning, and removes the
// temp file on any failure so a crash loop cannot fill the directory with
// abandoned partial checkpoints.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp checkpoint in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()

	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := commitTemp(tmp, data, path, tmpPath); err != nil {
		return err
	}
	renamed = true

	syncDirBestEffort(dir)

	return nil
}

// commitTemp writes, mode-restricts, syncs, closes, and renames the temp
// file into place. Split out of writeAtomic so the cleanup defer there has a
// single, simple condition to guard.
func commitTemp(tmp *os.File, data []byte, targetPath, tmpPath string) error {
	// 0o600: offsets are not secret, but they name log paths on the host, and
	// there is no reason for that to be world-readable.
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp checkpoint: %w", err)
	}

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp checkpoint: %w", err)
	}

	// Sync before rename, not after: without this the rename can be durable
	// while the content behind it is not, which after power loss looks like
	// a valid checkpoint full of zeros rather than a missing file — far more
	// dangerous, because Load has no way to tell it apart from a real one.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp checkpoint: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		return fmt.Errorf("rename checkpoint into place: %w", err)
	}

	return nil
}

// syncDirBestEffort fsyncs the directory containing a just-renamed file.
// This is the step that is almost always omitted: the rename is metadata,
// and without a directory sync a crash can leave the directory entry still
// pointing at the old inode even though the new file's content is safely on
// disk. It is deliberately not permitted to fail Commit: some environments
// (a directory whose mode disallows opening it for read, some non-POSIX or
// networked filesystems) reject opening or syncing a directory at all, and
// the file content is already durable from the sync in commitTemp, so this
// degrades to a weaker crash guarantee instead of refusing to commit
// progress that is otherwise safely on disk.
func syncDirBestEffort(dir string) {
	dirFile, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = dirFile.Sync()
	_ = dirFile.Close()
}
