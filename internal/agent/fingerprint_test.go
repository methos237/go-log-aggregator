package agent

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// openForFingerprint opens path read-only and registers cleanup to close it.
func openForFingerprint(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestFingerprintHead_FixedLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	// Well past headFingerprintLen, so the fingerprint is taken over exactly
	// headFingerprintLen bytes, not the whole file.
	writeFile(t, path, string(bytes.Repeat([]byte("x"), headFingerprintLen*4)))

	f := openForFingerprint(t, path)
	fp, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead: %v", err)
	}
	if fp.IsZero() {
		t.Fatal("fingerprintHead on a long file returned the zero Fingerprint")
	}
	if fp.Len != headFingerprintLen {
		t.Errorf("Len = %d, want %d", fp.Len, headFingerprintLen)
	}
}

func TestFingerprintHead_ExactlyPrefixLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, string(bytes.Repeat([]byte("y"), headFingerprintLen)))

	f := openForFingerprint(t, path)
	fp, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead: %v", err)
	}
	if fp.IsZero() {
		t.Fatal("fingerprintHead on a file exactly headFingerprintLen long returned the zero Fingerprint")
	}
	if fp.Len != headFingerprintLen {
		t.Errorf("Len = %d, want %d", fp.Len, headFingerprintLen)
	}
}

func TestFingerprintHead_BelowPrefixLengthIsZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, string(bytes.Repeat([]byte("z"), headFingerprintLen-1)))

	f := openForFingerprint(t, path)
	fp, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead: %v", err)
	}
	if !fp.IsZero() {
		t.Errorf("fingerprintHead on a short file = %+v, want the zero Fingerprint", fp)
	}
}

func TestFingerprintHead_EmptyFileIsZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, "")

	f := openForFingerprint(t, path)
	fp, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead: %v", err)
	}
	if !fp.IsZero() {
		t.Errorf("fingerprintHead on an empty file = %+v, want the zero Fingerprint", fp)
	}
}

// TestFingerprintHead_AppendPastPrefixDoesNotChange is the false-positive
// case the fixed-length rule exists to avoid: a fingerprint over
// min(prefix, size) would change every time this file grew, and would wrongly
// report an ordinary append as a truncation.
func TestFingerprintHead_AppendPastPrefixDoesNotChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, string(bytes.Repeat([]byte("a"), headFingerprintLen+50)))

	f := openForFingerprint(t, path)
	before, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead before append: %v", err)
	}

	appendToFile(t, path, string(bytes.Repeat([]byte("b"), 500)))

	after, err := fingerprintHead(f)
	if err != nil {
		t.Fatalf("fingerprintHead after append: %v", err)
	}

	if before != after {
		t.Errorf("fingerprint changed across an append past the prefix: before %+v, after %+v", before, after)
	}
	if !before.Matches(after) {
		t.Error("Matches(before, after) = false, want true for an append past the prefix")
	}
}

// TestFingerprintHead_LeavesOffsetUndisturbed pins the ReadAt requirement: a
// fingerprint that moved the file's read position would silently skip or
// re-emit lines in the tail source, which reads and seeks the same *os.File.
func TestFingerprintHead_LeavesOffsetUndisturbed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeFile(t, path, string(bytes.Repeat([]byte("c"), headFingerprintLen*2)))

	f := openForFingerprint(t, path)

	const wantOffset = 123
	if _, err := f.Seek(wantOffset, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}

	if _, err := fingerprintHead(f); err != nil {
		t.Fatalf("fingerprintHead: %v", err)
	}

	got, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("seek to query position: %v", err)
	}
	if got != wantOffset {
		t.Errorf("read offset after fingerprintHead = %d, want %d (unchanged)", got, wantOffset)
	}
}

func TestFingerprint_MatchesAndComparable(t *testing.T) {
	tests := []struct {
		name           string
		a, b           Fingerprint
		wantComparable bool
		wantMatches    bool
	}{
		{
			name:           "equal, same length",
			a:              Fingerprint{Len: 256, Hash: 42},
			b:              Fingerprint{Len: 256, Hash: 42},
			wantComparable: true,
			wantMatches:    true,
		},
		{
			name:           "same length, different hash: rewritten content",
			a:              Fingerprint{Len: 256, Hash: 1},
			b:              Fingerprint{Len: 256, Hash: 2},
			wantComparable: true,
			wantMatches:    false,
		},
		{
			name:           "different length: not comparable at all",
			a:              Fingerprint{Len: 256, Hash: 1},
			b:              Fingerprint{Len: 128, Hash: 1},
			wantComparable: false,
			wantMatches:    false,
		},
		{
			name:           "one zero: unknown, falls back to size",
			a:              Fingerprint{},
			b:              Fingerprint{Len: 256, Hash: 1},
			wantComparable: false,
			wantMatches:    false,
		},
		{
			name:           "both zero",
			a:              Fingerprint{},
			b:              Fingerprint{},
			wantComparable: false,
			wantMatches:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Comparable(tt.b); got != tt.wantComparable {
				t.Errorf("Comparable = %v, want %v", got, tt.wantComparable)
			}
			if got := tt.b.Comparable(tt.a); got != tt.wantComparable {
				t.Errorf("Comparable (reversed) = %v, want %v", got, tt.wantComparable)
			}
			if got := tt.a.Matches(tt.b); got != tt.wantMatches {
				t.Errorf("Matches = %v, want %v", got, tt.wantMatches)
			}
		})
	}
}
