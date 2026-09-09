package agent

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// byteOrder is the explicit encoding for every on-disk integer in a segment
// file. Explicit rather than native so a segment written on one architecture
// reads back correctly on another — this agent's binaries run on more than
// one.
var byteOrder = binary.BigEndian

const (
	// entryHeaderSize is the length prefix plus the checksum that precede every
	// payload:
	//
	//   +----------------+----------------+-------------------------+
	//   | length (4B BE) | CRC32  (4B BE) | payload (length bytes)  |
	//   +----------------+----------------+-------------------------+
	//
	// A length-prefixed record is what lets a scan find the next entry without
	// a delimiter that the payload itself might legitimately contain, and the
	// checksum is what lets recovery tell a genuine entry apart from a torn or
	// bit-flipped one without any help from the writer.
	entryHeaderSize = 8

	// maxEntryPayloadBytes bounds any single length prefix this package will
	// trust before allocating a buffer for it. It is independent of
	// SegmentBytes on purpose: SegmentBytes governs when to roll to a new file,
	// not the largest single entry that could ever legitimately be spooled (an
	// oversized entry still gets its own segment; see Append). This ceiling
	// exists only to stop a corrupt length field — a few flipped bits turning
	// "200" into "3 billion" — from making a scan call make([]byte, length)
	// with a length an attacker or bit-flip chose, which is the classic way a
	// length-prefixed format turns corruption into an out-of-memory crash.
	maxEntryPayloadBytes = 256 << 20 // 256 MiB

	// segmentDigits is the zero-padded width of a segment's sequence number in
	// its filename. os.ReadDir returns entries sorted by filename, and fixed-
	// width zero-padding is what makes that lexical order equal chronological
	// order: without it, "10.seg" would sort before "2.seg" as strings, and
	// recovery would replay segments out of order. 12 digits is comfortably
	// more than this agent will ever create even shipping continuously for
	// decades at a segment every few seconds.
	segmentDigits = 12
	segmentSuffix = ".seg"

	// cursorFileName holds the persisted read position. It deliberately does
	// not match the segment filename pattern, so recovery's directory scan
	// skips it the same way it skips any other unrelated file.
	cursorFileName = "cursor.json"

	// cursorVersion is the schema written by this package, mirroring
	// checkpointVersion's role in checkpoint.go: a future format change must
	// be a loud decision, not a silent misparse.
	cursorVersion = 1

	// cursorDurableInterval bounds how many Release calls may pass between
	// durably fsynced persists of the read cursor (see Release). It is also
	// exactly the bound on duplicate replay after a crash: at most
	// cursorDurableInterval-1 already-released entries in the current oldest
	// segment can be re-read on the next restart, because a segment boundary
	// always forces a durable persist regardless of this counter (see
	// Release), and Close always does too. Small enough that a crash between
	// durable persists never replays much; large enough that most Releases
	// pay only the cheap, non-fsync persist described there.
	cursorDurableInterval = 8
)

// SpoolConfig configures a Spool.
type SpoolConfig struct {
	// Dir holds the segment files and the persisted read cursor. It is created
	// if it does not exist, since a first run has no spool.
	Dir string
	// MaxBytes is the total on-disk bound across every segment. Past it,
	// whole oldest segments are deleted — see enforceMaxBytesLocked for why
	// oldest rather than newest.
	MaxBytes int64
	// SegmentBytes is the size past which Append rolls to a new segment file.
	SegmentBytes int64
	// Metrics records MaxBytes evictions. Nil builds an unregistered set via
	// NewMetrics(nil), which is what tests want; production wiring supplies
	// one built against the real registry.
	Metrics *Metrics
}

// segmentInfo is one segment file's bookkeeping: how many whole valid
// entries it holds and how many bytes they occupy. bytes is always the exact
// on-disk size of the file — segments are never rewritten in place, only
// appended to (by Append) or deleted whole (by Release finishing one, or by
// MaxBytes eviction), so this field never needs to be recomputed once set.
type segmentInfo struct {
	seq     uint64
	path    string
	entries int
	bytes   int64
}

// Spool is a bounded on-disk buffer of opaque payloads.
//
// It exists for exactly one problem: when the collector is unreachable for a
// long time, the agent must keep reading rather than stall, because the file
// a source is tailing can rotate away underneath it during the outage, and
// that generation of data is gone for good the moment that happens. Buffering
// failed sends to disk is what lets reading continue past a rotation while
// the network is down.
//
// It is deliberately NOT a write-ahead log, and that is the load-bearing
// design decision here: Append is called only when a send fails, never on
// every record. The normal path — collector reachable — goes straight from a
// source to the network and never touches disk a second time. A source's
// input is already durable on the filesystem it reads from, and the
// checkpoint (see checkpoint.go) only advances on the collector's
// acknowledgement, so a crash never loses data that was merely read: it is
// simply re-read from the original file on restart. Spooling everything,
// always, the way a WAL would, would buy this package nothing — the source
// file already is the WAL — while doubling every write's disk cost and
// giving corruption in the spool a chance to lose data the source could
// still have replayed.
//
// One consequence of that framing matters for durability choices throughout
// this file: Append never fsyncs. Anything sitting in a segment file is, by
// definition, un-acknowledged — Append only runs after a send failed — so
// the checkpoint has not advanced past it either, and losing it to a hard
// crash before it reaches disk is exactly as safe as never having spooled it
// at all: the source still has it and will hand it to a source again. Only
// the read cursor (what Release has already gotten off this process's
// plate) needs any durability at all, because getting that wrong in the
// wrong direction — believing something was consumed when it was not — would
// be the one way this package could actually lose data. See Release.
//
// Peek and Release are kept separate rather than merged into one Pop for the
// same reason: an entry must not disappear from the spool until the
// collector has acknowledged it, and a send can still fail after Peek
// returns the bytes. Release is the caller's promise that this specific
// payload made it.
//
// Append can itself evict entries — see its own doc comment and
// enforceMaxBytesLocked — whenever adding one pushes the spool over
// MaxBytes. Any caller keeping parallel bookkeeping indexed against the
// spool's FIFO order (again, Shipper.spoolMeta) must trim that bookkeeping
// by Append's evicted return value or it will silently drift out of
// alignment with what Peek actually returns.
type Spool struct {
	dir          string
	maxBytes     int64
	segmentBytes int64
	cursorPath   string

	mu sync.Mutex
	// segments holds every live segment, oldest first, tail (the one Append
	// writes to) last. A fresh spool with nothing appended yet has an empty
	// slice; Append creates the first segment lazily.
	segments []segmentInfo
	// consumedEntries and consumedBytes describe how much of segments[0] —
	// and only segments[0], never any other index — has been released.
	// Every other segment is either fully unconsumed (anything past index 0)
	// or already gone (anything that was index 0 and got fully consumed), so
	// there is never a need to track this per segment.
	consumedEntries int
	consumedBytes   int64
	// nextSeq is the sequence number the next newly created segment will use.
	// It only ever increases, including across restarts (see recover), so a
	// sequence number is never reused and lexical order stays chronological.
	nextSeq uint64
	// tail is the open file handle Append writes to — "one open file at the
	// tail for appends," kept open across calls rather than reopened every
	// time, since this is the path a live outage runs hot.
	tail *os.File
	// releasesSincePersist counts Release calls since the last durable cursor
	// persist, and is what cursorDurableInterval bounds.
	releasesSincePersist int

	// metrics records entries MaxBytes eviction discards as a genuine drop
	// (see enforceMaxBytesLocked): an outage that outlasted the configured
	// spool size, and those entries' records are gone for good.
	metrics *Metrics
}

// NewSpool opens or creates the spool rooted at cfg.Dir.
//
// A missing directory is created and treated as an empty spool, not an
// error: a first run has no buffered data. An existing directory is fully
// adopted — segments are sorted, the persisted read cursor is applied, the
// tail segment's framing is verified, and Len/Bytes are recomputed from what
// survives — because forgetting a directory's contents on restart would
// silently drop everything an outage forced onto disk.
func NewSpool(cfg SpoolConfig) (*Spool, error) {
	if cfg.Dir == "" {
		return nil, errors.New("spool: dir must not be empty")
	}
	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("spool: max bytes must be positive, got %d", cfg.MaxBytes)
	}
	if cfg.SegmentBytes <= 0 {
		return nil, fmt.Errorf("spool: segment bytes must be positive, got %d", cfg.SegmentBytes)
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("spool: create dir %s: %w", cfg.Dir, err)
	}

	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	s := &Spool{
		dir:          cfg.Dir,
		maxBytes:     cfg.MaxBytes,
		segmentBytes: cfg.SegmentBytes,
		cursorPath:   filepath.Join(cfg.Dir, cursorFileName),
		metrics:      metrics,
	}

	if err := s.recover(); err != nil {
		return nil, err
	}

	return s, nil
}

// recover runs once from NewSpool, before the Spool is handed to any caller,
// so unlike every other method here it needs no lock even though it shares
// helpers whose names end in "Locked" by the convention used elsewhere in
// this file.
func (s *Spool) recover() error {
	dirEntries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("spool: read dir %s: %w", s.dir, err)
	}

	var lastSeq uint64
	var haveAny bool
	// os.ReadDir returns entries sorted by filename, and segmentDigits' fixed
	// width is exactly what makes that the same as sequence order, so no
	// separate sort is needed here.
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		seq, ok := parseSegmentSeq(de.Name())
		if !ok {
			// cursor.json, or anything else left in the directory that isn't a
			// segment: an unrelated file here must not break recovery.
			continue
		}

		path := filepath.Join(s.dir, de.Name())
		info, err := de.Info()
		if err != nil {
			return fmt.Errorf("spool: stat %s: %w", path, err)
		}

		entries, validBytes, err := scanEntries(path, -1)
		if err != nil {
			return fmt.Errorf("spool: scan %s: %w", path, err)
		}
		if entries == 0 {
			// Nothing here parsed as a whole valid entry: an empty file left by
			// a crash between create and first write, or one where every byte is
			// corrupt. Either way there is nothing to keep.
			_ = os.Remove(path)
			continue
		}
		if validBytes < info.Size() {
			// A torn tail from a crash mid-append. Truncating it now means a
			// future append lands right after the last good entry instead of
			// after garbage, and nothing downstream ever has to special-case a
			// segment with trailing junk.
			if err := os.Truncate(path, validBytes); err != nil {
				return fmt.Errorf("spool: truncate torn tail in %s: %w", path, err)
			}
		}

		s.segments = append(s.segments, segmentInfo{seq: seq, path: path, entries: entries, bytes: validBytes})
		lastSeq = seq
		haveAny = true
	}

	if haveAny {
		s.nextSeq = lastSeq + 1
	}

	if err := s.recoverCursor(); err != nil {
		return err
	}

	s.enforceMaxBytesLocked()

	if len(s.segments) > 0 {
		if err := s.reopenTailLocked(); err != nil {
			return err
		}
	}

	return nil
}

// recoverCursor applies the persisted read position to the segments recover
// already found on disk.
func (s *Spool) recoverCursor() error {
	cursor, loaded, err := loadCursorFile(s.cursorPath)
	if err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	if !loaded {
		return nil
	}

	// Any segment older than the persisted cursor was fully released before
	// whatever ended the previous run: a clean Close, or a crash after the
	// durable persist that named the next segment but before the os.Remove
	// that would have deleted this one (see Release). Either way every entry
	// in it is already accounted for, so deleting it now loses nothing.
	for len(s.segments) > 0 && s.segments[0].seq < cursor.Segment {
		_ = os.Remove(s.segments[0].path)
		s.segments = s.segments[1:]
	}

	if cursor.Segment >= s.nextSeq {
		s.nextSeq = cursor.Segment + 1
	}

	if len(s.segments) == 0 || s.segments[0].seq != cursor.Segment {
		// The cursor named a segment that is not (or is no longer) the oldest
		// survivor — a fresh spool that never persisted one, or a cursor left
		// behind by a run whose segments have since all been consumed or
		// evicted. The safe fallback is to treat nothing as consumed: that can
		// replay a surviving segment in full, which is the bounded-duplicate
		// tradeoff this package already accepts, but it can never skip data
		// that was never released.
		return nil
	}

	consumedEntries, consumedBytes, err := scanEntries(s.segments[0].path, cursor.Offset)
	if err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	s.consumedEntries = consumedEntries
	s.consumedBytes = consumedBytes
	return nil
}

// Append adds a payload to the tail.
//
// It rolls to a new segment first if the current tail already has data and
// this entry would push it past SegmentBytes. A tail with nothing in it yet
// always accepts the entry regardless of size: rolling an empty segment
// would protect nothing, which is also what lets a payload larger than
// SegmentBytes be stored at all — it gets a fresh segment to itself, and the
// same check then forces the very next Append to roll again, since the tail
// is already past the bound with nothing further able to fit beside it.
// Rejecting an oversized payload outright would be simpler, but the
// collector already bounds batch size independently, and refusing to spool
// a batch that was merely too big to share a segment would throw away data
// this package exists to protect.
//
// Append can itself evict: adding this entry can push the spool over
// MaxBytes, which enforceMaxBytesLocked answers by deleting whole oldest
// segments (see its own doc comment for why oldest, and why the segment this
// entry just landed in is never one of them). evicted reports how many
// entries — always taken from the front of the spool's logical FIFO, oldest
// first — that eviction removed, so a caller keeping its own parallel
// bookkeeping of what is in the spool (see Shipper.spoolMeta) can trim that
// bookkeeping by the same amount from its own front and stay aligned with
// what Peek will actually return next.
func (s *Spool) Append(payload []byte) (evicted int, err error) {
	if len(payload) > maxEntryPayloadBytes {
		return 0, fmt.Errorf("spool: payload of %d bytes exceeds the %d-byte limit", len(payload), maxEntryPayloadBytes)
	}
	entry := encodeEntry(payload)

	s.mu.Lock()
	defer s.mu.Unlock()

	needNew := len(s.segments) == 0
	if !needNew {
		tail := s.segments[len(s.segments)-1]
		if tail.bytes > 0 && tail.bytes+int64(len(entry)) > s.segmentBytes {
			needNew = true
		}
	}
	if needNew {
		if err := s.openNewTailLocked(); err != nil {
			return 0, err
		}
	}

	// No fsync here: see the Spool doc comment on why an un-acknowledged
	// entry losing its trip to disk on a hard crash is exactly as safe as it
	// never having been spooled — the source that produced it still has it.
	if _, err := s.tail.Write(entry); err != nil {
		return 0, fmt.Errorf("spool: append: %w", err)
	}

	tailIdx := len(s.segments) - 1
	s.segments[tailIdx].bytes += int64(len(entry))
	s.segments[tailIdx].entries++

	return s.enforceMaxBytesLocked(), nil
}

// openNewTailLocked closes the current tail handle, if any, and opens a
// fresh segment file to become the new one.
func (s *Spool) openNewTailLocked() error {
	if s.tail != nil {
		if err := s.tail.Close(); err != nil {
			return fmt.Errorf("spool: close previous segment: %w", err)
		}
		s.tail = nil
	}

	seq := s.nextSeq
	path := s.segmentPath(seq)
	// O_EXCL: nextSeq must never collide with a segment already on disk. If it
	// somehow did, overwriting it would silently destroy unreleased entries.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("spool: create segment %s: %w", path, err)
	}
	s.tail = f
	s.nextSeq++
	s.segments = append(s.segments, segmentInfo{seq: seq, path: path})
	return nil
}

// reopenTailLocked opens the existing last segment found during recovery in
// append mode, so Append can keep writing to it without disturbing the
// content already verified there.
func (s *Spool) reopenTailLocked() error {
	tail := s.segments[len(s.segments)-1]
	f, err := os.OpenFile(tail.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("spool: reopen tail %s: %w", tail.path, err)
	}
	s.tail = f
	return nil
}

// enforceMaxBytesLocked deletes whole oldest segments while the spool is
// over MaxBytes, counting every entry in a deleted segment that had not yet
// been released as dropped.
//
// It never removes the last remaining segment, even if that one segment
// alone is over MaxBytes: that segment is always the tail Append is
// currently writing to when there is only one, and evicting the file a
// write is in flight to would corrupt the open handle's bookkeeping, not
// just lose data. In that pathological case (a single entry, or MaxBytes
// configured smaller than SegmentBytes) Bytes briefly exceeds MaxBytes
// rather than the spool refusing the write; stalling the reader instead is
// the one outcome this package exists to avoid.
//
// Oldest is dropped rather than newest on purpose. During an outage, the
// newest data is what an operator is most likely looking at when they
// notice the outage at all, and the alternative to dropping something is
// blocking Append until room frees up — which stalls the reader upstream of
// it, which is the exact failure this whole component exists to prevent.
//
// It returns the total number of entries removed from the spool's logical
// FIFO — i.e. summed across every evicted segment, victim.entries minus
// whatever of that segment had already been released — which is exactly how
// far a caller's own front-aligned bookkeeping (see Append) needs to be
// trimmed to match.
func (s *Spool) enforceMaxBytesLocked() int {
	var evicted int
	for s.totalBytesLocked() > s.maxBytes && len(s.segments) > 1 {
		victim := s.segments[0]
		lost := victim.entries - s.consumedEntries
		s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonSpoolEvicted).Add(float64(lost))
		evicted += lost
		s.segments = s.segments[1:]
		s.consumedEntries = 0
		s.consumedBytes = 0
		_ = os.Remove(victim.path)
	}
	return evicted
}

func (s *Spool) totalBytesLocked() int64 {
	var total int64
	for _, seg := range s.segments {
		total += seg.bytes
	}
	return total
}

// segmentPath returns the on-disk path for a segment sequence number.
func (s *Spool) segmentPath(seq uint64) string {
	return filepath.Join(s.dir, segmentFileName(seq))
}

// Peek returns the oldest payload without removing it. Calling it again
// before the next Release returns the same bytes: this method only reads,
// it never advances anything, which is what lets a caller retry a failed
// send without losing its place.
//
// It returns ok=false with a nil error once the reader has caught up with
// everything appended so far. That is the ordinary "nothing to send right
// now" outcome, not a fault, and callers should treat it that way.
func (s *Spool) Peek() ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, _, ok, err := s.peekEntryLocked()
	return payload, ok, err
}

// peekEntryLocked reads the oldest unreleased entry, if any, from segments[0]
// and also returns its total on-disk size (header plus payload) so Release
// can advance past it without a second read.
//
// This never encounters a torn or corrupt entry the way recovery's scan
// must: Append only ever completes one whole entry write before returning,
// under the same lock this method runs under, and recovery has already
// discarded anything torn or corrupt before Len/Bytes were ever computed
// from a segment's byte count. A checksum failure here would mean the
// bookkeeping in this struct disagrees with what is actually on disk, which
// is a bug in this package, not a fact about the data — hence the error
// return rather than a silent skip.
func (s *Spool) peekEntryLocked() (payload []byte, size int64, ok bool, err error) {
	if len(s.segments) == 0 {
		return nil, 0, false, nil
	}
	seg := s.segments[0]
	if s.consumedBytes >= seg.bytes {
		return nil, 0, false, nil
	}

	f, err := os.Open(seg.path)
	if err != nil {
		return nil, 0, false, fmt.Errorf("spool: open %s: %w", seg.path, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(s.consumedBytes, io.SeekStart); err != nil {
		return nil, 0, false, fmt.Errorf("spool: seek %s: %w", seg.path, err)
	}

	header := make([]byte, entryHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, 0, false, fmt.Errorf("spool: read entry header in %s: %w", seg.path, err)
	}
	length := byteOrder.Uint32(header[:4])
	wantCRC := byteOrder.Uint32(header[4:])

	body := make([]byte, length)
	if _, err := io.ReadFull(f, body); err != nil {
		return nil, 0, false, fmt.Errorf("spool: read entry payload in %s: %w", seg.path, err)
	}
	if crc32.ChecksumIEEE(body) != wantCRC {
		return nil, 0, false, fmt.Errorf("spool: checksum mismatch in %s at offset %d", seg.path, s.consumedBytes)
	}

	return body, entryHeaderSize + int64(length), true, nil
}

// Release discards the oldest payload. Call it only once the collector has
// acknowledged that payload.
//
// Unlike Append, Release never needs to report anything back for a caller's
// parallel bookkeeping to trim: the segment-boundary deletion below only
// ever removes a segment once every entry in it — including the one this
// very call is releasing — has been released. It can never remove an entry
// nothing has acknowledged yet, which is exactly the property that would
// make an evicted-style return value necessary here the way it is for
// Append.
//
// Release on a spool with nothing left to release is not an error: it is a
// no-op. A caller that raced a Peek against a concurrent drain, or that
// simply calls Release once too often while draining down to empty, must
// not be punished for it.
//
// The read cursor this advances is persisted, but not with a full fsync on
// every call — see cursorDurableInterval. Crossing a segment boundary (this
// segment is now fully consumed and gets deleted) always forces a durable
// persist regardless of that counter, because a deleted segment can never
// be replayed no matter what the cursor says, so the cursor must not be
// allowed to lag behind that fact by more than a crash window. This is a
// deliberate bounded-duplicate tradeoff, not an oversight: a crash between
// two durable persists can replay up to cursorDurableInterval-1 entries that
// were already released, and that is safe here specifically because those
// entries were never acknowledged as far as the checkpoint is concerned
// either — the collector's ack that released them and storage's dedup on
// (stream_id, seq, time) are exactly what absorbs the resulting duplicate.
//
// Release must be called from one goroutine at a time. Every other method here
// is fully mutex-guarded, but this one deliberately drops the lock before
// persisting the cursor, because holding a mutex across an fsync would stall a
// concurrent Append behind the disk. The consequence is that two overlapping
// Release calls could let the older (segment, offset) land last, rewinding the
// persisted read position and replaying already-released entries after a
// restart. The shipper drives Peek and Release from its single select loop, so
// this does not happen today; it is stated because the alternative — a second
// caller appearing later and quietly reintroducing replay — would look like a
// spool bug rather than a violated contract.
func (s *Spool) Release() error {
	s.mu.Lock()

	_, size, ok, err := s.peekEntryLocked()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !ok {
		s.mu.Unlock()
		return nil
	}

	s.consumedEntries++
	s.consumedBytes += size

	boundary := s.consumedBytes >= s.segments[0].bytes
	if boundary {
		old := s.segments[0].path
		s.segments = s.segments[1:]
		s.consumedEntries = 0
		s.consumedBytes = 0
		// Best effort: if this fails, the file is orphaned but harmless — the
		// next NewSpool deletes any segment older than the persisted cursor on
		// recovery, which this one will be the moment the persist below lands.
		_ = os.Remove(old)
	}

	s.releasesSincePersist++
	durable := boundary || s.releasesSincePersist >= cursorDurableInterval
	if durable {
		s.releasesSincePersist = 0
	}
	segSeq, offset := s.pendingCursorLocked()
	cursorPath := s.cursorPath

	// Unlocked before the write: the only potentially slow step here is the
	// fsync inside a durable persist, and holding the mutex across it would
	// stall a concurrent Append behind however long the disk takes, the same
	// reasoning writeAtomic's caller in checkpoint.go already follows. Nothing
	// below reads s.* again, so releasing the lock here is safe.
	s.mu.Unlock()

	data, err := encodeCursor(segSeq, offset)
	if err != nil {
		return err
	}
	if durable {
		return writeAtomic(cursorPath, data)
	}
	return writeCursorCheap(cursorPath, data)
}

// pendingCursorLocked returns the (segment, offset) this Spool's state
// implies, whether or not any segment currently exists: with nothing left
// unconsumed, it names the segment that will be created next, which is what
// makes it safe for recovery to delete anything on disk numbered before it.
func (s *Spool) pendingCursorLocked() (uint64, int64) {
	if len(s.segments) == 0 {
		return s.nextSeq, 0
	}
	return s.segments[0].seq, s.consumedBytes
}

// Len reports entries currently spooled: appended but not yet released.
func (s *Spool) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	for _, seg := range s.segments {
		total += seg.entries
	}
	return total - s.consumedEntries
}

// Bytes reports bytes currently on disk across every live segment.
//
// This counts the whole file size of segments[0] even though part of it may
// already be released: that part still occupies disk space until the
// segment is fully consumed and deleted, and MaxBytes bounds actual disk
// usage, not an accounting fiction of what is logically still pending.
func (s *Spool) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalBytesLocked()
}

// Close persists the read cursor durably and releases the open tail handle.
// It is safe to call more than once.
func (s *Spool) Close() error {
	s.mu.Lock()
	segSeq, offset := s.pendingCursorLocked()
	var closeErr error
	if s.tail != nil {
		closeErr = s.tail.Close()
		s.tail = nil
	}
	cursorPath := s.cursorPath
	s.mu.Unlock()

	data, err := encodeCursor(segSeq, offset)
	if err != nil {
		return errors.Join(closeErr, err)
	}
	if err := writeAtomic(cursorPath, data); err != nil {
		return errors.Join(closeErr, fmt.Errorf("spool: persist cursor on close: %w", err))
	}
	return closeErr
}

// encodeEntry frames a payload for on-disk storage: length, checksum, then
// the payload itself, in that order (see entryHeaderSize).
func encodeEntry(payload []byte) []byte {
	buf := make([]byte, entryHeaderSize+len(payload))
	byteOrder.PutUint32(buf[0:4], uint32(len(payload)))
	byteOrder.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)
	return buf
}

// scanEntries validates entries in the file at path from the start, up to at
// most limit bytes (the whole file, if limit is negative or beyond the
// file's size), stopping at the first entry that fails to parse or fails its
// checksum instead of returning an error for it. A half-written entry means
// an ordinary crash mid-append here, not a fault worth surfacing to a
// caller — see the Spool doc comment on this package's stance on data a
// source can still replay. It returns how many whole valid entries it found
// and the byte offset immediately past the last one, which is always a
// legitimate entry boundary and never partway into one.
//
// A length prefix is rejected — and treated exactly like a torn entry,
// ending the scan there — before it is trusted for an allocation, whether
// because it exceeds maxEntryPayloadBytes or because it claims more bytes
// than remain in the file. The second check is what stops a corrupt length
// field from trying to allocate gigabytes even in a tiny test file: the
// claimed length is compared against what is actually left to read, not
// allocated first and checked after.
func scanEntries(path string, limit int64) (entries int, validBytes int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("stat %s: %w", path, err)
	}
	size := info.Size()
	if limit < 0 || limit > size {
		limit = size
	}

	header := make([]byte, entryHeaderSize)
	var offset int64
	for offset < limit {
		left := size - offset
		if left < entryHeaderSize {
			break // torn: not even a full header remains
		}
		if _, err := io.ReadFull(f, header); err != nil {
			break // torn: header could not be read in full
		}
		length := byteOrder.Uint32(header[:4])
		wantCRC := byteOrder.Uint32(header[4:])
		left -= entryHeaderSize

		if int64(length) > maxEntryPayloadBytes || int64(length) > left {
			break // absurd or torn length prefix: refuse to trust it
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break // torn: payload could not be read in full
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // corrupt: a bit flipped somewhere in this entry
		}

		entries++
		offset += entryHeaderSize + int64(length)
	}
	return entries, offset, nil
}

// segmentFileName formats a sequence number as a segment filename; see
// segmentDigits for why the width is fixed.
func segmentFileName(seq uint64) string {
	return fmt.Sprintf("%0*d%s", segmentDigits, seq, segmentSuffix)
}

// parseSegmentSeq reports whether name is a segment filename and, if so, its
// sequence number. Anything else — including cursorFileName, a stray dotfile,
// or a subdirectory a caller left behind — reports false so recovery can
// skip it rather than fail.
func parseSegmentSeq(name string) (uint64, bool) {
	if len(name) != segmentDigits+len(segmentSuffix) {
		return 0, false
	}
	if name[segmentDigits:] != segmentSuffix {
		return 0, false
	}
	digits := name[:segmentDigits]
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// spoolCursorFile is the on-disk shape of the persisted read position,
// mirroring checkpointFile's role in checkpoint.go.
type spoolCursorFile struct {
	Version int    `json:"version"`
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

// encodeCursor marshals a read position to its on-disk JSON form.
func encodeCursor(segment uint64, offset int64) ([]byte, error) {
	data, err := json.Marshal(spoolCursorFile{Version: cursorVersion, Segment: segment, Offset: offset})
	if err != nil {
		return nil, fmt.Errorf("spool: marshal cursor: %w", err)
	}
	return data, nil
}

// loadCursorFile reads the persisted read position. A missing file reports
// loaded=false with no error, the same as a first run having no checkpoint
// in checkpoint.go's Load.
//
// Unlike Load there, a cursor that fails to parse, carries an unrecognized
// version, or holds a negative offset is not treated as fatal: it also
// reports loaded=false rather than an error. The two disagree on purpose.
// CheckpointStore refuses to start on a corrupt file because guessing wrong
// about acked progress can either replay everything or skip everything, and
// skipping is a silent, undetectable loss of durable data. Guessing wrong
// here only ever replays a surviving segment in full — the same bounded,
// deduplicated-downstream tradeoff this package already accepts elsewhere —
// so refusing to start over it would trade a bounded, harmless duplicate for
// an agent that will not run.
func loadCursorFile(path string) (spoolCursorFile, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return spoolCursorFile{}, false, nil
		}
		return spoolCursorFile{}, false, fmt.Errorf("read cursor %s: %w", path, err)
	}

	var cf spoolCursorFile
	//nolint:nilerr // deliberate: see the doc comment above — a corrupt cursor
	// falls back to "not loaded" rather than failing NewSpool, unlike
	// CheckpointStore.Load's stricter refusal-to-guess for acked progress.
	if err := json.Unmarshal(data, &cf); err != nil {
		return spoolCursorFile{}, false, nil
	}
	if cf.Version != cursorVersion || cf.Offset < 0 {
		return spoolCursorFile{}, false, nil
	}
	return cf, true, nil
}

// writeCursorCheap persists data to path via the same temp-file-then-rename
// shape as writeAtomic in checkpoint.go, so a crash mid-write can only ever
// leave the previous complete cursor file or the new complete one, never a
// torn one — but it skips both of writeAtomic's fsyncs. That is the "cheap"
// persist Release uses on most calls: the temp file write and the rename are
// ordinary buffered operations, a small fraction of the cost of two fsyncs,
// at the cost of not knowing exactly when the update becomes durable. See
// cursorDurableInterval for what bounds the resulting replay window, and
// Release for when a durable persist is forced instead of this one.
func writeCursorCheap(path string, data []byte) error {
	dir := filepath.Dir(path)

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("spool: create temp cursor in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()

	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("spool: write temp cursor: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("spool: close temp cursor: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("spool: rename cursor into place: %w", err)
	}
	renamed = true

	return nil
}
