package agent

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// sampleCursors returns a small, distinct set of cursors to round-trip: two
// sources with different File identities and one with the zero FileID, since
// stdin and the Docker source legitimately have no backing file. The first
// also carries a Head fingerprint, since that field round-trips too.
func sampleCursors() map[string]Cursor {
	return map[string]Cursor{
		"/var/log/app.log": {Start: 100, Offset: 142, File: FileID{Dev: 1, Ino: 55}, Head: Fingerprint{Len: 256, Hash: 0xdeadbeef}},
		"/var/log/db.log":  {Start: 0, Offset: 0, File: FileID{Dev: 2, Ino: 9001}},
		"stdin":            {Start: 7, Offset: 7, File: FileID{}},
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	want := sampleCursors()

	store := NewCheckpointStore(path)
	for source, c := range want {
		store.Set(source, c)
	}
	if err := store.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reloaded := NewCheckpointStore(path)
	if err := reloaded.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	for source, wantCursor := range want {
		got, ok := reloaded.Get(source)
		if !ok {
			t.Errorf("Get(%q): missing after round trip", source)
			continue
		}
		if got != wantCursor {
			t.Errorf("Get(%q) = %+v, want %+v", source, got, wantCursor)
		}
	}
}

// TestCheckpointRoundTrip_Head is the dedicated check that Head survives a
// round trip with both Len and Hash intact, called out separately from
// TestCheckpointRoundTrip (which also covers it via sampleCursors) because
// this is the one field added by this subtask and deserves a test that
// fails on its own, not only as a side effect of a broader comparison.
func TestCheckpointRoundTrip_Head(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	want := Cursor{Start: 10, Offset: 20, File: FileID{Dev: 3, Ino: 44}, Head: Fingerprint{Len: 256, Hash: 0x1234567890abcdef}}

	store := NewCheckpointStore(path)
	store.Set("app.log", want)
	if err := store.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reloaded := NewCheckpointStore(path)
	if err := reloaded.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	got, ok := reloaded.Get("app.log")
	if !ok {
		t.Fatal("Get(app.log): missing after round trip")
	}
	if got.Head.Len != want.Head.Len {
		t.Errorf("Head.Len = %d, want %d", got.Head.Len, want.Head.Len)
	}
	if got.Head.Hash != want.Head.Hash {
		t.Errorf("Head.Hash = %#x, want %#x", got.Head.Hash, want.Head.Hash)
	}
}

// TestCheckpointLoadMissingHeadIsZeroFingerprint is what a checkpoint file
// written by a build before Head existed looks like: the "head" key is
// simply absent, not present-and-invalid. Load must treat that as the zero
// Fingerprint — "unknown, fall back to size" — not as a parse error, since
// an old, valid checkpoint file is not corrupt just because it predates a
// field.
func TestCheckpointLoadMissingHeadIsZeroFingerprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	content := `{"version": 1, "cursors": {"a": {"start": 0, "offset": 10, "file": {"dev": 1, "ino": 2}}}}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	store := NewCheckpointStore(path)
	if err := store.Load(); err != nil {
		t.Fatalf("Load on a pre-Head checkpoint: %v", err)
	}

	got, ok := store.Get("a")
	if !ok {
		t.Fatal("Get(a): missing")
	}
	if !got.Head.IsZero() {
		t.Errorf("Head = %+v, want the zero Fingerprint for a checkpoint with no head key", got.Head)
	}
}

func TestCheckpointLoadMissingFileIsNotError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.json")

	store := NewCheckpointStore(path)
	if err := store.Load(); err != nil {
		t.Fatalf("Load on missing file: %v", err)
	}

	if _, ok := store.Get("anything"); ok {
		t.Error("Get after Load on missing file: want no cursors, got one")
	}
}

func TestCheckpointLoadCorrupt(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "truncated JSON", content: `{"version": 1, "cursors": {"a": {"start": 1,`},
		{name: "garbage bytes", content: "\x00\x01\x02not json at all\xff\xfe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "checkpoint.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("seed corrupt file: %v", err)
			}

			store := NewCheckpointStore(path)
			if err := store.Load(); err == nil {
				t.Error("Load on corrupt file: want error, got nil")
			}
		})
	}
}

func TestCheckpointLoadUnknownVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	content := `{"version": 99, "cursors": {}}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	store := NewCheckpointStore(path)
	err := store.Load()
	if err == nil {
		t.Fatal("Load on unknown schema version: want error, got nil")
	}
	if !strings.Contains(err.Error(), "99") {
		t.Errorf("Load error %q: want it to name the offending version 99", err.Error())
	}
}

func TestCheckpointLoadInvalidCursor(t *testing.T) {
	tests := []struct {
		name    string
		cursors string
	}{
		{name: "negative offset", cursors: `{"a": {"start": 0, "offset": -1, "file": {"dev": 0, "ino": 0}}}`},
		{name: "negative start", cursors: `{"a": {"start": -5, "offset": 10, "file": {"dev": 0, "ino": 0}}}`},
		{name: "start greater than offset", cursors: `{"a": {"start": 50, "offset": 10, "file": {"dev": 0, "ino": 0}}}`},
		{name: "empty source name", cursors: `{"": {"start": 0, "offset": 10, "file": {"dev": 0, "ino": 0}}}`},
		{name: "negative head length", cursors: `{"a": {"start": 0, "offset": 10, "file": {"dev": 0, "ino": 0}, "head": {"len": -1, "hash": 0}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "checkpoint.json")
			content := `{"version": 1, "cursors": ` + tt.cursors + `}`
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatalf("seed file: %v", err)
			}

			store := NewCheckpointStore(path)
			if err := store.Load(); err == nil {
				t.Errorf("Load with %s: want error, got nil", tt.name)
			}
		})
	}
}

func TestCheckpointCommitLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store := NewCheckpointStore(path)
	store.Set("a", Cursor{Start: 1, Offset: 2})
	if err := store.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("directory after Commit: want exactly the checkpoint file, got %v", names)
	}
	if entries[0].Name() != filepath.Base(path) {
		t.Errorf("directory after Commit: got %q, want %q", entries[0].Name(), filepath.Base(path))
	}
}

func TestCheckpointCommitOverwritesRatherThanAppends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store := NewCheckpointStore(path)
	store.Set("a", Cursor{Start: 1, Offset: 2})
	if err := store.Commit(); err != nil {
		t.Fatalf("first Commit: %v", err)
	}

	store.Set("a", Cursor{Start: 10, Offset: 20})
	store.Set("b", Cursor{Start: 3, Offset: 4})
	if err := store.Commit(); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	reloaded := NewCheckpointStore(path)
	if err := reloaded.Load(); err != nil {
		t.Fatalf("Load after second Commit: %v", err)
	}

	got, ok := reloaded.Get("a")
	if !ok {
		t.Fatal("Get(a) after overwrite: missing")
	}
	if want := (Cursor{Start: 10, Offset: 20}); got != want {
		t.Errorf("Get(a) after overwrite = %+v, want %+v", got, want)
	}
	if _, ok := reloaded.Get("b"); !ok {
		t.Error("Get(b) after overwrite: missing")
	}
}

func TestCheckpointGetUnknownSource(t *testing.T) {
	store := NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json"))
	if _, ok := store.Get("unknown"); ok {
		t.Error("Get on unknown source: want false, got true")
	}
}

func TestCheckpointConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	store := NewCheckpointStore(path)

	const goroutines = 8
	const iterations = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			source := "source-" + string(rune('a'+id))
			for n := 0; n < iterations; n++ {
				store.Set(source, Cursor{Start: int64(n), Offset: int64(n + 1)})
				store.Get(source)
				if n%10 == 0 {
					if err := store.Commit(); err != nil {
						t.Errorf("Commit: %v", err)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestCheckpointCommitUnwritableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: root ignores directory permissions, so this case cannot be exercised")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store := NewCheckpointStore(path)
	store.Set("a", Cursor{Start: 1, Offset: 2})

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod dir read-only: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
	})

	if err := store.Commit(); err == nil {
		t.Error("Commit into unwritable directory: want error, got nil")
	}

	if _, err := os.Stat(path); err == nil {
		t.Error("Commit into unwritable directory: target file should not exist, but it does")
	} else if !os.IsNotExist(err) {
		t.Errorf("Stat target after failed Commit: %v", err)
	}
}
