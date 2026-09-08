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
type FileID struct {
	Dev uint64
	Ino uint64
}

// IsZero reports whether the ID refers to no file at all.
func (f FileID) IsZero() bool { return f == FileID{} }

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
	Start int64
	// Offset is the byte offset immediately past this line in the file it came
	// from, counting the line terminator. Resuming from it reads the next line.
	// Zero for sources without seekable input.
	Offset int64
	// File identifies the backing file. A change here under an unchanged Source
	// name is a rotation; see the tail source for how truncation is told apart.
	File FileID
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
