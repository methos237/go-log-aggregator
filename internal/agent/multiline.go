package agent

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

const (
	// defaultFlushTimeout is long enough that a legitimately slow producer (a
	// handler that logs once every few hundred milliseconds mid-stack-trace)
	// never gets its lines split apart, but short enough that a quiet source's
	// last record ships promptly instead of sitting held indefinitely. Applied
	// when MultilineConfig.FlushTimeout is zero.
	defaultFlushTimeout = 5 * time.Second
	// defaultMaxLines bounds a runaway loop that logs one short line per
	// iteration. At that line count model.MaxMessageLen has almost certainly
	// not fired yet, so without an independent line bound the record grows
	// without limit.
	defaultMaxLines = 500
	// idleWait is the duration of a deadline-driven timer (the Joiner's flush
	// timer, the Shipper's accumulator timer) whenever nothing is pending. Its
	// value is arbitrary: the timer is reset the instant something is added, so
	// this only needs to not fire spuriously while idle.
	idleWait = time.Hour
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
	// Metrics records splits and timeout flushes. Nil builds an unregistered
	// set via NewMetrics(nil), which is what tests want; production wiring
	// supplies one built against the real registry.
	Metrics *Metrics
}

// Joiner is the multiline stage: it reads Lines from one channel and emits
// Lines on another, folding continuation lines into the record they belong
// to.
//
// A Joiner carries no per-run state itself; the held record per source and
// the flush timer live inside Run, so one Joiner may in principle be reused
// across Run calls. Its metrics are the deliberate exception: they are
// cumulative across calls, which is the correct behavior for a metric.
type Joiner struct {
	continuation *regexp.Regexp
	flushTimeout time.Duration
	metrics      *Metrics
}

// NewJoiner validates cfg and returns a Joiner, applying defaults for zero
// fields.
func NewJoiner(cfg MultilineConfig) (*Joiner, error) {
	if cfg.FlushTimeout < 0 {
		return nil, fmt.Errorf("multiline: flush timeout must not be negative, got %s", cfg.FlushTimeout)
	}

	flushTimeout := cfg.FlushTimeout
	if flushTimeout == 0 {
		flushTimeout = defaultFlushTimeout
	}

	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &Joiner{
		continuation: cfg.Continuation,
		flushTimeout: flushTimeout,
		metrics:      metrics,
	}, nil
}

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

// heldRecord is the record being assembled for one source: the Line that will
// eventually be emitted, grown in place as continuation lines are absorbed.
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
	line  Line
	lines int
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
	heldDeadline := func(h *heldRecord) time.Time { return h.deadline }

	// One timer, reused for the life of the loop. Firing a fresh time.After per
	// line would allocate a timer per line and would still not answer "which
	// of several sources with different deadlines is due right now" — this
	// timer is instead reset to the single earliest deadline across every held
	// record whenever that set changes, and re-armed after each firing.
	timer := time.NewTimer(idleWait)
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
			timer.Reset(nextWait(held, heldDeadline, time.Now()))

		case now := <-timer.C:
			if err := j.flushDue(ctx, out, held, now); err != nil {
				return err
			}
			timer.Reset(nextWait(held, heldDeadline, time.Now()))

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// absorb applies one line to held, sending a flush first if the line ends,
// replaces, or would overflow the record held for its source.
//
// The byte bound (model.MaxMessageLen) bounds the joining, not the individual
// line: a single line that is already at the limit is emitted whole rather
// than split or truncated. That is deliberate. Lines reach this stage already
// bounded at model.MaxMessageLen by the assembler, so such a record is still
// one the collector accepts, and chopping up a line that arrived intact would
// destroy a record to satisfy a limit that exists to stop *accumulation*.
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
	if len(h.line.Bytes) > 0 {
		sep = 1
	}

	switch {
	case len(h.line.Bytes)+sep+len(line.Bytes) > model.MaxMessageLen:
		j.metrics.MultilineMaxBytesSplits.Inc()
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
		held[line.Source] = j.newHeld(line, now)

	case h.lines+1 > defaultMaxLines:
		j.metrics.MultilineMaxLinesSplits.Inc()
		if err := j.flushOne(ctx, out, h); err != nil {
			return err
		}
		held[line.Source] = j.newHeld(line, now)

	default:
		if sep == 1 {
			h.line.Bytes = append(h.line.Bytes, '\n')
		}
		h.line.Bytes = append(h.line.Bytes, line.Bytes...)
		h.lines++
		// Start and Time stay the first line's; Offset, File and Head advance
		// to the last line's — see flushOne for why.
		h.line.Cursor.Offset = line.Cursor.Offset
		h.line.Cursor.File = line.Cursor.File
		h.line.Cursor.Head = line.Cursor.Head
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
	return &heldRecord{line: *line, lines: 1, deadline: now.Add(j.flushTimeout)}
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
		j.metrics.MultilineTimeoutFlushes.Inc()
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
	return send(ctx, out, &h.line)
}

// nextWait returns how long a deadline timer should sleep before the earliest
// deadline across m is due, or idleWait if m is empty. Shared by the Joiner's
// held records and the Shipper's accumulators.
func nextWait[K comparable, V any](m map[K]V, deadline func(V) time.Time, now time.Time) time.Duration {
	var earliest time.Time
	for _, v := range m {
		if d := deadline(v); earliest.IsZero() || d.Before(earliest) {
			earliest = d
		}
	}
	if earliest.IsZero() {
		return idleWait
	}
	return max(earliest.Sub(now), 0)
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
