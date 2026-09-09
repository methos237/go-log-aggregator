package agent

import (
	"context"
	"time"
)

// FileID identifies a file independently of its name.
//
// Rotation is detected by comparing this across reads, never by comparing
// filenames: every rotation scheme in common use — rename-and-recreate,
// copytruncate, symlink swap — keeps the name the agent was configured with and
// changes what is behind it. The device number is part of the identity because
// inode numbers are only unique within a filesystem, and a watched directory can
// be a mount point.
//
// The zero value means "no backing file", which is the honest answer for stdin
// and for the Docker source.
//
// The json tags on this and the other Cursor types are the checkpoint file's
// on-disk format (see checkpoint.go). Lowercase deliberately: that file's whole
// purpose is to be inspected with `cat` while diagnosing a restart, and short
// lowercase keys read better in a terminal than exported Go names would.
type FileID struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

// IsZero reports whether the ID refers to no file at all.
func (f FileID) IsZero() bool { return f == FileID{} }

// Fingerprint is a hash of a file's first bytes, used to notice that a file was
// truncated and rewritten rather than appended to.
//
// It exists because size alone cannot detect that under polling. A writer that
// truncates a file and rewrites past the old offset inside one poll interval
// never appears to shrink, so an agent comparing sizes resumes partway into
// unrelated new content and emits corrupted lines with no error at all — a
// silent-corruption failure, which is worse than a gap because nothing reports
// it.
//
// Len is the number of bytes hashed and is the field that says whether the
// fingerprint means anything: zero means "not fingerprinted", which is the
// honest answer for a file shorter than the fixed prefix length and for a cursor
// written by a build that predates this field. Comparing hashes taken over
// different lengths is meaningless, which is why the length is stored rather
// than assumed — a fingerprint over min(prefix, size) would change every time a
// short file grew and would report truncation on ordinary appends.
type Fingerprint struct {
	Len  int64  `json:"len"`
	Hash uint64 `json:"hash"`
}

// IsZero reports whether no fingerprint was taken.
func (f Fingerprint) IsZero() bool { return f.Len == 0 }

// Differs reports whether two fingerprints were taken over the same, non-zero
// length and disagree — the one case that means the file was rewritten. Two
// fingerprints of differing length, or one never taken, are not comparable at
// all, so this reports false rather than guessing: "cannot tell" and
// "definitely different" call for opposite responses, and a caller that cannot
// compare should fall back to size and inode rather than treat it as truncation.
func (f Fingerprint) Differs(o Fingerprint) bool {
	return f.Len > 0 && f.Len == o.Len && f.Hash != o.Hash
}

// Cursor is where a Line came from, expressed in terms the checkpoint store can
// persist and a restarted process can trust.
//
// It deliberately holds no in-process generation counter. A counter would be a
// convenient way to notice rotation while running and completely worthless in a
// checkpoint file, because it resets to zero on the restart the checkpoint exists
// to survive. File plus Offset is the pair that still means something after a
// crash.
type Cursor struct {
	// Start is the byte offset of this line's first byte. It becomes the record's
	// model.LogRecord.Seq, which is why it is carried explicitly instead of being
	// derived as Offset minus the line length: that arithmetic is wrong for CRLF
	// input and wrong for every truncated line, and a seq that does not reproduce
	// exactly on replay turns deduplication into duplicate rows.
	Start int64 `json:"start"`
	// Offset is the byte offset immediately past this line in the file it came
	// from, counting the line terminator. Resuming from it reads the next line.
	// Zero for sources without seekable input.
	Offset int64 `json:"offset"`
	// File identifies the backing file. A change here under an unchanged Source
	// name is a rotation; see the tail source for how truncation is told apart.
	File FileID `json:"file"`
	// Head fingerprints the start of the file this cursor points into. It is
	// persisted so that a restart can tell "the same file, resume at Offset" from
	// "the same inode, but its contents were replaced while we were down", which
	// inode and size together cannot distinguish.
	//
	// An absent "head" key in a checkpoint written by a build before this field
	// existed unmarshals to the zero Fingerprint — "unknown, fall back to size" —
	// not a parse error. A file written by an older build is not corrupt; it
	// simply predates a field, and Load must not treat that as the same failure
	// as hand-edited garbage.
	Head Fingerprint `json:"head,omitempty"`
}

// Line is one unparsed log line with its provenance.
//
// Bytes is not copied defensively at every stage: it is owned by the receiver
// from the moment it is sent on the channel, and a Source MUST NOT retain or
// reuse the slice after sending. Doing the copy once in the source is cheaper
// than doing it defensively in every consumer, and the alternative — a shared
// read buffer handed downstream — is a data race that only shows up under load.
type Line struct {
	// Source is the emitting source's Name. It is the checkpoint key and the
	// value of the source label on this package's metrics, so it must be stable
	// across restarts.
	Source string
	// Time is when the line was observed, as close to production as the source
	// can get. The collector rejects records outside its acceptance window
	// (see model.MaxClockSkewFuture and model.MaxBackfill), so a source that
	// parses a timestamp out of the line must still produce something sane when
	// the parse fails.
	Time time.Time
	// Bytes is the line with its terminator stripped. It is never nil, may be
	// empty, and is at most model.MaxMessageLen long — a source that reads a
	// longer line truncates it and counts the truncation rather than emitting
	// something the collector will reject.
	Bytes []byte
	// Cursor is where to resume if this line is the last one durably shipped.
	Cursor Cursor
}

// Source produces lines from one origin: a file, a container, or stdin.
//
// Implementations own their goroutine and their file descriptors. They know
// nothing about batching, spooling, or acknowledgement — a Source's job ends when
// the Line is on the channel, and durability is decided downstream by the
// checkpoint store from the collector's acks.
type Source interface {
	// Name identifies the source in metrics and in the checkpoint store, so it
	// must be stable across restarts and unique within one agent. For file
	// sources it is the configured path, not the resolved one: the resolved path
	// changes on rotation and the configured one is what the operator wrote.
	Name() string

	// Run reads until ctx is canceled or the source ends, sending every line to
	// out. It returns nil for a clean end of input — which stdin has and a tailed
	// file does not — and ctx.Err() when canceled.
	//
	// Run blocks when out is full, and that is the intended backpressure: the
	// data is still on disk behind the reader, so stalling the reader loses
	// nothing, while dropping here would lose a line that was never at risk.
	// Run MUST NOT close out; one channel is fed by several sources.
	Run(ctx context.Context, out chan<- Line) error

	// Close releases descriptors and watches. It is safe to call after Run has
	// returned and safe to call twice.
	Close() error
}
