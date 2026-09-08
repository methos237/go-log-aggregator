package agent

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFileIDFromInfo pins the property the tail source's rotation detection rests
// on: identity follows the file, not the name.
//
// It is worth a test of its own because the alternative implementation — compare
// paths, or compare modification times — passes every casual manual check and
// then loses data on the first real rotation.
func TestFileIDFromInfo(t *testing.T) {
	dir := t.TempDir()

	pathA := filepath.Join(dir, "a.log")
	pathB := filepath.Join(dir, "b.log")
	writeFile(t, pathA, "a\n")
	writeFile(t, pathB, "b\n")

	idA, ok := fileIDFromInfo(statFile(t, pathA))
	if !ok {
		t.Fatal("fileIDFromInfo reported no ID for a real file; this platform cannot tail correctly")
	}
	if idA.IsZero() {
		t.Error("fileIDFromInfo returned the zero ID for a real file, which means 'no backing file'")
	}

	idB, ok := fileIDFromInfo(statFile(t, pathB))
	if !ok {
		t.Fatal("fileIDFromInfo reported no ID for a real file")
	}
	if idA == idB {
		t.Errorf("two distinct files share an ID: %+v", idA)
	}

	t.Run("stable across reopen", func(t *testing.T) {
		again, ok := fileIDFromInfo(statFile(t, pathA))
		if !ok {
			t.Fatal("fileIDFromInfo reported no ID on reopen")
		}
		if again != idA {
			t.Errorf("ID changed across reopen: %+v then %+v", idA, again)
		}
	})

	t.Run("follows the file through a rename", func(t *testing.T) {
		// This is rotation as logrotate performs it by default: the file the agent
		// was configured with is renamed out of the way, and a new one takes its
		// place. Identity must follow the bytes.
		rotated := filepath.Join(dir, "a.log.1")
		if err := os.Rename(pathA, rotated); err != nil {
			t.Fatalf("rename: %v", err)
		}
		writeFile(t, pathA, "fresh\n")

		afterRename, ok := fileIDFromInfo(statFile(t, rotated))
		if !ok {
			t.Fatal("fileIDFromInfo reported no ID for the renamed file")
		}
		if afterRename != idA {
			t.Errorf("ID did not follow the rename: was %+v, now %+v", idA, afterRename)
		}

		replacement, ok := fileIDFromInfo(statFile(t, pathA))
		if !ok {
			t.Fatal("fileIDFromInfo reported no ID for the replacement file")
		}
		if replacement == idA {
			t.Error("the replacement file at the original path reused the original ID; " +
				"rotation would be undetectable")
		}
	})

	t.Run("not a filesystem FileInfo", func(t *testing.T) {
		// A FileInfo that did not come from the OS has no device or inode to
		// report. Reporting false rather than a zero ID keeps "no backing file"
		// distinguishable from "could not tell".
		if _, ok := fileIDFromInfo(fakeInfo{}); ok {
			t.Error("fileIDFromInfo claimed an ID for a non-filesystem FileInfo")
		}
	})
}

// fakeInfo is a FileInfo that did not come from the filesystem, so Sys reports
// nothing a stat could have produced.
type fakeInfo struct{}

func (fakeInfo) Name() string       { return "fake" }
func (fakeInfo) Size() int64        { return 0 }
func (fakeInfo) Mode() fs.FileMode  { return 0 }
func (fakeInfo) ModTime() time.Time { return time.Time{} }
func (fakeInfo) IsDir() bool        { return false }
func (fakeInfo) Sys() any           { return nil }

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func statFile(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info
}
