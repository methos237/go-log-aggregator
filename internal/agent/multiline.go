package agent

import (
	"context"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// Defaults applied when the corresponding MultilineConfig field is zero.
const (
	// defaultFlushTimeout is long enough that a legitimately slow producer (a
	// handler that logs once every few hundred milliseconds mid-stack-trace)
	// never gets its lines split apart, but short enough that a quiet source's
	// last record ships promptly instead of sitting held indefinitely.
	defaultFlushTimeout = 5 * time.Second
	// defaultMaxLines bounds a runaway loop that logs one short line per
	// iteration. At that line count MaxBytes has almost certainly not fired
	// yet, so without an independent line bound the record grows without limit.
	defaultMaxLines = 500
	// noHeldWait is the flush timer's duration whenever nothing is held. Its
	// value is arbitrary: the timer is reset the instant a record is added, so
	// this only needs to not fire spuriously while the joiner is idle.
	noHeldWait = time.Hour
)

// MultilineConfig configures a Joiner.
type MultilineConfig struct {
	// Continuation matches a line that belongs to the record before it. A nil
	// Continuation disables joining entirely: every line passes through
	// unchanged. That is the expected configuration for most sources, and Run
	// takes a cheaper path for it, so a source that never needs joining never
	// pays for matching a regexp against every line.
	Continuation *regexp.Regexp
	// FlushTimeout flushes a held record when no further line arrives within
	// it, measured from the last line appended to that record rather than from
	// a fixed tick. Defaults to defaultFlushTimeout when zero.
	FlushTimeout time.Duration
	// MaxLines caps how many lines one record may absorb. Defaults to
	// defaultMaxLines when zero.
	MaxLines int
	// MaxBytes caps how large joining may grow a record. Defaults to
	// model.MaxMessageLen when zero and must never exceed it.
	//
	// It bounds the joining, not the individual line: a single line that is
	// already longer than MaxBytes is emitted whole rather than split or
	// truncated. That is deliberate. Lines reach this stage already bounded at
	// model.MaxMessageLen by the assembler, so such a record is still one the
	// collector accepts, and chopping up a line that arrived intact would
	// destroy a record to satisfy a limit that exists to stop *accumulation*.
	// Setting MaxBytes below the longest single line a source produces
	// therefore disables joining for those lines rather than truncating them.
	MaxBytes int
}

// Joiner is the multiline stage: it reads Lines from one channel and emits
// Lines on another, folding continuation lines into the record they belong
// to.
//
// A Joiner carries no per-run state itself; the held record per source and
// the flush timer live inside Run, so one Joiner may in principle be reused
// across Run calls. Its counters are the deliberate exception: they are
// cumulative across calls, which is the correct behavior for a metric.
type Joiner struct {
	continuation *regexp.Regexp
	flushTimeout time.Duration
	maxLines     int
	maxBytes     int

	// Counters kept local to this file rather than routed through
	// internal/agent/metrics.go, which another change is adding concurrently.
	// A later consolidation can read these through the exported accessors
	// below instead of duplicating the bookkeeping here.
	maxBytesSplits atomic.Int64
	maxLinesSplits atomic.Int64
	timeoutFlushes atomic.Int64
}

// NewJoiner validates cfg and returns a Joiner, applying defaults for zero
// fields.
func NewJoiner(cfg MultilineConfig) (*Joiner, error) {
	if cfg.FlushTimeout < 0 {
		return nil, fmt.Errorf("multiline: flush timeout must not be negative, got %s", cfg.FlushTimeout)
	}
	if cfg.MaxLines < 0 {
		return nil, fmt.Errorf("multiline: max lines must not be negative, got %d", cfg.MaxLines)
	}
	if cfg.MaxBytes < 0 {
		return nil, fmt.Errorf("multiline: max bytes must not be negative, got %d", cfg.MaxBytes)
	}
	// Clamped rather than trusted: the collector rejects any record longer than
	// model.MaxMessageLen outright, so a Joiner configured above that limit
	// would build records guaranteed to be dropped at ingest instead of caught
	// here, where the operator can see why.
	if cfg.MaxBytes > model.MaxMessageLen {
		return nil, fmt.Errorf("multiline: max bytes %d exceeds collector limit %d", cfg.MaxBytes, model.MaxMessageLen)
	}

	flushTimeout := cfg.FlushTimeout
	if flushTimeout == 0 {
		flushTimeout = defaultFlushTimeout
	}
	maxLines := cfg.MaxLines
	if maxLines == 0 {
		maxLines = defaultMaxLines
	}
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = model.MaxMessageLen
	}

	return &Joiner{
		continuation: cfg.Continuation,
		flushTimeout: flushTimeout,
		maxLines:     maxLines,
		maxBytes:     maxBytes,
	}, nil
}

// MaxBytesSplits reports how many held records were emitted early because the
// next line would have pushed them past MaxBytes.
func (j *Joiner) MaxBytesSplits() int64 { return j.maxBytesSplits.Load() }

// MaxLinesSplits reports how many held records were emitted early because the
// next line would have pushed them past MaxLines.
func (j *Joiner) MaxLinesSplits() int64 { return j.maxLinesSplits.Load() }

// TimeoutFlushes reports how many held records were emitted because
// FlushTimeout elapsed with no further line arriving.
func (j *Joiner) TimeoutFlushes() int64 { return j.timeoutFlushes.Load() }

// Run reads lines from in, emits joined records on out, and returns when in
// is closed or ctx is canceled.
func (j *Joiner) Run(ctx context.Context, in <-chan Line, out chan<- Line) error {
	if j.continuation == nil {
		return j.runPassthrough(ctx, in, out)
	}
	return j.runJoin(ctx, in, out)
}

// runPassthrough is the fast path for a Joiner with no Continuation pattern:
// every line is forwarded unchanged, with its cursor untouched. It exists so
// a source that does not need joining never pays for a per-line regexp match,
// a held-record map, or a flush timer it will never use.
func (j *Joiner) runPassthrough(ctx context.Context, in <-chan Line, out chan<- Line) error {
	for {
		select {
		case line, ok := <-in:
			if !ok {
				return nil
			}
			if err := send(ctx, out, &line); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// heldRecord is the record being assembled for one source.
//
// runJoin keys these by Line.Source because a single in channel carries lines
// from every configured source interleaved: a line from source A must never
// be folded into a record held for source B.
//
// The map of held records is not bounded by source count. That is safe here
// because sources are the operator's static configuration — file paths,
// container names — never attacker- or end-user-supplied at runtime, so the
// map's size is bounded by the config file that started this agent, not by
// anything arriving on in.
type heldRecord struct {
	source string
	// start and t are the first constituent line's Cursor.Start and Time. See
	// flushOne's comment for why these are the first line's while offset, file
	// and head below are the last line's.
	start  int64
	t      time.Time
	bytes  []byte
	lines  int
	offset int64
	file   FileID
	head   Fingerprint
	// deadline is when this record flushes if nothing more arrives for it. It
	// is recomputed from FlushTimeout every time a line is appended, not held
	// against a fixed tick: a record that just received a line should get a
	// fresh window, not be judged against how long it has existed in total.
	deadline time.Time
}

// runJoin is the joining path: a continuation line is folded into the record
// held for its source; anything else ends that held record (which is
// emitted) and starts a new one.
func (j *Joiner) runJoin(ctx context.Context, in <-chan Line, out chan<- Line) error {
	held := make(map[string]*heldRecord)

	// One timer, reused for the life of the loop. Firing a fresh time.After per
	// line would allocate a timer per line and would still not answer "which
	// of several sources with different deadlines is due right now" — this
	// timer is instead reset to the single earliest deadline across every held
	// record whenever that set changes, and re-armed after each firing.
	timer := time.NewTimer(noHeldWait)
	defer timer.Stop()

	for {
		select {
		case line, ok := <-in:
			if !ok {
				// Flush every held record before returning: dropping them here
				// would lose the tail of every source that was mid-record when
				// its Source stopped.
				return j.flushAll(ctx, out, held)
			}
			if err := j.absorb(ctx, out, held, &line, time.Now()); err != nil {
				return err
			}
			timer.Reset(nextWait(held, time.Now()))

		case now := <-timer.C:
			if err := j.flushDue(ctx, out, held, now); err != nil {
				return err
			}
			timer.Reset(nextWait(held, time.Now()))

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// absorb applies one line to held, sending a flush first if the line ends,
// replaces, or would overflow the record held for its source.
func (j *Joiner) absorb(ctx context.Context, out chan<- Line, held map[string]*heldRecord, line *Line, now time.Time) error {
	h, ok := held[line.Source]
	if !ok {
		held[line.Source] = j.newHeld(line, now)
		return nil
	}

	if !j.continuation.Match(line.Bytes) {
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
		held[line.Source] = j.newHeld(line, now)
		return nil
	}

	sep := 0
	if len(h.bytes) > 0 {
		sep = 1
	}

	switch {
	case len(h.bytes)+sep+len(line.Bytes) > j.maxBytes:
		j.maxBytesSplits.Add(1)
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
		held[line.Source] = j.newHeld(line, now)

	case h.lines+1 > j.maxLines:
		j.maxLinesSplits.Add(1)
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
		held[line.Source] = j.newHeld(line, now)

	default:
		if sep == 1 {
			h.bytes = append(h.bytes, '\n')
		}
		h.bytes = append(h.bytes, line.Bytes...)
		h.lines++
		h.offset = line.Cursor.Offset
		h.file = line.Cursor.File
		h.head = line.Cursor.Head
		h.deadline = now.Add(j.flushTimeout)
	}
	return nil
}

// newHeld starts a held record from line. line.Bytes becomes the record's
// buffer directly rather than being copied: per Line's contract the receiver
// owns Bytes from the moment it arrives on the channel, and the first append
// (if any) will reallocate anyway since the slice arrives with no spare
// capacity.
func (j *Joiner) newHeld(line *Line, now time.Time) *heldRecord {
	return &heldRecord{
		source:   line.Source,
		start:    line.Cursor.Start,
		t:        line.Time,
		bytes:    line.Bytes,
		lines:    1,
		offset:   line.Cursor.Offset,
		file:     line.Cursor.File,
		head:     line.Cursor.Head,
		deadline: now.Add(j.flushTimeout),
	}
}

// flushDue emits and removes every held record whose deadline has passed as
// of now. Deleting from held while ranging it is safe: Go's range over a map
// tolerates deletion of the current key.
func (j *Joiner) flushDue(ctx context.Context, out chan<- Line, held map[string]*heldRecord, now time.Time) error {
	for source, h := range held {
		if h.deadline.After(now) {
			continue
		}
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
		delete(held, source)
		j.timeoutFlushes.Add(1)
	}
	return nil
}

// flushAll emits every held record, in no particular order. Called once, on
// shutdown, so held is not mutated afterward and there is no need to delete
// as it goes.
func (j *Joiner) flushAll(ctx context.Context, out chan<- Line, held map[string]*heldRecord) error {
	for _, h := range held {
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
	}
	return nil
}

// flushOne emits h as one joined Line.
//
// Cursor.Start is the first constituent line's Start and Cursor.Offset is the
// last constituent line's Offset — deliberately asymmetric. The checkpoint
// advances only on acked records, using Offset, so a joined record's Offset
// must sit past every line it absorbed: if it carried the first line's
// offset instead, a restart would resume before the continuation lines and
// re-read them as a brand-new record. Start feeds the record's Seq (see
// Cursor.Start's doc comment in source.go), and that must be the position
// the record actually began at or the sequence number would not reproduce on
// replay, breaking dedup. File and Head are the last line's because they
// describe the file generation the record ended in, which matters when a
// record's continuation lines span a rotation. Time is the first line's: the
// record happened when it started, not when its last stack frame arrived.
func (j *Joiner) flushOne(ctx context.Context, out chan<- Line, h *heldRecord) error {
	line := Line{
		Source: h.source,
		Time:   h.t,
		Bytes:  h.bytes,
		Cursor: Cursor{
			Start:  h.start,
			Offset: h.offset,
			File:   h.file,
			Head:   h.head,
		},
	}
	return send(ctx, out, &line)
}

// nextWait returns how long the flush timer should sleep before the earliest
// held record becomes due, or noHeldWait if nothing is held.
func nextWait(held map[string]*heldRecord, now time.Time) time.Duration {
	var earliest time.Time
	for _, h := range held {
		if earliest.IsZero() || h.deadline.Before(earliest) {
			earliest = h.deadline
		}
	}
	if earliest.IsZero() {
		return noHeldWait
	}
	if d := earliest.Sub(now); d > 0 {
		return d
	}
	return 0
}

// send delivers line to out, blocking rather than shedding. The data behind
// this line is still on disk (see doc.go's rationale for the package), so
// waiting for a slow consumer loses nothing, while a select with a default
// branch would silently drop it — the failure mode an earlier source in this
// package shipped and this package now deliberately avoids everywhere.
func send(ctx context.Context, out chan<- Line, line *Line) error {
	select {
	case out <- *line:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
