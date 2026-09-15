package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/backoff"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// Defaults applied by NewShipper for every zero-valued ShipperConfig field.
const (
	defaultMaxBatchRecords = 500
	// defaultMaxBatchBytes must stay comfortably under the collector's
	// default LOGAGG_INGEST_MAX_RECV_BYTES (1 MiB; see internal/config).
	// Half of that leaves headroom for the batch envelope (batch_id,
	// labels) on top of the records, and for the fact that MaxRecvMsgSize
	// is a hard gRPC-level refusal, not a soft limit worth skating close
	// to.
	defaultMaxBatchBytes = 512 << 10
	defaultMaxBatchDelay = time.Second
	defaultAckWindow     = 64
	defaultMinBackoff    = 250 * time.Millisecond
	defaultMaxBackoff    = 30 * time.Second

	// defaultCheckpointCommitInterval bounds how long acked progress can
	// sit uncommitted in memory. It is deliberately not part of
	// ShipperConfig: Commit is cheap insurance against a crash losing
	// otherwise-safe-to-skip replay, not a knob an operator needs to tune,
	// and tests shrink it directly (same package) rather than through the
	// public API.
	defaultCheckpointCommitInterval = 5 * time.Second
	// defaultShutdownAckWait bounds how long shutdown waits for batches
	// already in flight to be acknowledged before giving up and spooling
	// them instead. Bounded so a collector that hangs at exactly the wrong
	// moment cannot make shutdown hang too.
	defaultShutdownAckWait = 2 * time.Second
)

// ShipperConfig configures a Shipper.
type ShipperConfig struct {
	// Ingest is how to reach the collector. Addr is required; TLS material
	// and DialTimeout follow ingest.ClientConfig's own rules.
	Ingest ingest.ClientConfig
	// Spool buffers batches the collector has not yet durably accepted.
	// Required.
	Spool *Spool
	// Checkpoint persists acked progress per source. Required.
	Checkpoint *CheckpointStore
	// Extractor pulls structured fields out of a line's bytes. Nil means no
	// field extraction.
	Extractor *Extractor

	// Labels resolves a source name to the label set identifying its
	// stream. A source with no labels is a configuration error, not a
	// reason to drop data silently — report it. Required.
	Labels func(source string) (model.LabelSet, bool)

	// MaxBatchRecords bounds a batch by record count. Zero uses
	// defaultMaxBatchRecords.
	MaxBatchRecords int
	// MaxBatchBytes bounds a batch by its marshaled wire size. See
	// defaultMaxBatchBytes for why it must stay well under the collector's
	// max receive size. Zero uses defaultMaxBatchBytes.
	MaxBatchBytes int
	// MaxBatchDelay bounds how long a partially-filled batch waits before
	// shipping anyway. Zero uses defaultMaxBatchDelay.
	MaxBatchDelay time.Duration
	// AckWindow bounds how many batches may be outstanding — sent but not
	// yet acknowledged — before the shipper stops sending and spools
	// instead. This is the flow-control bound that keeps a stalled
	// collector from making the shipper buffer without limit in memory.
	// Zero uses defaultAckWindow.
	AckWindow int

	// MinBackoff and MaxBackoff bound the full-jitter backoff used both to
	// reopen a dropped stream and to pause after an OVERLOADED ack. Zero
	// uses defaultMinBackoff / defaultMaxBackoff.
	MinBackoff, MaxBackoff time.Duration

	// Metrics records batches, drops and reconnects. Nil builds an
	// unregistered set via NewMetrics(nil), which is what tests want.
	Metrics *Metrics
}

// accumulator is one batch being filled, for one label set. Shipper keeps
// one accumulator per active label set (see the accums field) rather than a
// single one for the whole shipper: labels are resolved per source, lines
// from sources with different label sets interleave on the one in channel,
// and a LogBatch carries exactly one Labels, so records for different label
// sets can never share a batch and must be allowed to accumulate
// independently and concurrently — see addLine.
type accumulator struct {
	labels model.LabelSet
	// overhead is the marshaled size of the batch envelope (batch_id and
	// labels) with no records, computed once so MaxBatchBytes checks do not
	// re-marshal the whole batch on every line.
	overhead int
	bytes    int
	records  []*logaggv1.LogRecord
	// sourceCursors is the highest Cursor per source seen by this
	// accumulator, valid or invalid record alike. See addLine for why an
	// invalid or oversized record still updates it.
	sourceCursors map[string]Cursor
	// deadline is when this accumulator flushes if nothing forces it out
	// sooner, set once at creation (see newAccumulator) so MaxBatchDelay is
	// measured from creation, not from whichever line happens to be added
	// last before a sweep notices it.
	//
	// There is deliberately no per-accumulator timer here. All accumulators
	// share one reused timer (Shipper.accumTimer), reset to the earliest
	// deadline across every accumulator — see rearmAccumTimer — following
	// the same pattern multiline.go's runJoin uses for held records: a timer
	// per accumulator would allocate per stream and still not answer "which
	// one, if any, is due right now".
	deadline time.Time
}

// outstanding is one batch sent but not yet acknowledged.
var tracer = otel.Tracer("github.com/jamespolk/go-log-aggregator/internal/agent")

type outstanding struct {
	batch         *logaggv1.LogBatch
	sourceCursors map[string]Cursor
	// span is the agent.ship span, open from Send until the ack that settles
	// the batch or the stream loss that requeues it.
	span trace.Span
	// fromSpool marks a batch that was Peek'd from the spool rather than
	// built fresh this run. Its bytes are still safely on disk,
	// unreleased, until an ack says otherwise — see teardownStream and
	// processAck for what that changes.
	fromSpool bool
}

// endSpan closes the ship span. Nil-safe because tests build outstanding
// entries by hand, and ending twice is a no-op in the SDK.
func (o outstanding) endSpan(code codes.Code, description string) {
	if o.span == nil {
		return
	}
	o.span.SetStatus(code, description)
	o.span.End()
}

// ackResult is what the receiver goroutine hands back to Run: exactly one of
// ack or err is set.
type ackResult struct {
	ack *logaggv1.Ack
	err error
}

// Shipper batches lines, ships them to a collector over a gRPC bidi stream,
// and spools whatever cannot be sent right now. See the package doc for the
// durability contract it exists to uphold: a line's progress is durable only
// once the collector has acknowledged it.
type Shipper struct {
	ingestCfg  ingest.ClientConfig
	spool      *Spool
	checkpoint *CheckpointStore
	extractor  *Extractor
	labels     func(string) (model.LabelSet, bool)

	maxBatchRecords int
	maxBatchBytes   int
	maxBatchDelay   time.Duration
	ackWindow       int
	minBackoff      time.Duration
	maxBackoff      time.Duration
	metrics         *Metrics

	checkpointCommitInterval time.Duration
	shutdownAckWait          time.Duration

	// Everything below is touched by exactly one goroutine — the one
	// running Run's select loop — except ackCh, which the receiver
	// goroutine spawned by tryConnect also writes to. That split is the
	// whole concurrency model: one sender, one receiver, communicating
	// only through a channel, which is what lets Send and Recv run at the
	// same time without a data race and without violating the "two senders
	// on a stream" rule ingest.Stream's doc warns about.
	client *ingest.Client
	stream *ingest.Stream
	ackCh  chan ackResult

	// accums holds one accumulator per active label set, keyed by
	// model.LabelSet.ID(). It is not bounded by anything in this package.
	// That is safe here because label sets come from the operator's
	// configured sources (see ShipperConfig.Labels), never from line
	// content, so the map's size is bounded by the config that started this
	// agent process, not by anything arriving on in — the same reasoning
	// multiline.go's heldRecord comment gives for its own per-source map.
	//
	// One source resolves to exactly one label set: Labels is called once
	// per line with that line's Source and returns the same answer every
	// time for the same source. So every line from a given source always
	// lands in the same accumulator, which is what keeps that source's
	// lines in arrival order within whatever batch eventually carries them.
	accums map[model.StreamID]*accumulator
	// accumTimer is the single reused timer that wakes Run to flush
	// whichever accumulators have reached their delay deadline. See
	// rearmAccumTimer.
	accumTimer   *time.Timer
	pendingAcks  []outstanding
	batchSeq     int64
	attempt      int
	sendPaused   bool
	pauseTimer   *time.Timer
	connectRetry *time.Timer

	// spoolMeta is the FIFO of per-source cursor maps for entries currently
	// sitting in the spool, aligned index-for-index with the spool's own
	// FIFO order. A nil entry means "unknown": either it predates this run
	// (loaded from disk at startup, with nothing in memory remembering
	// which lines produced it) or it was dropped as unparseable. See
	// tryDrainSpool and appendToSpool for the two ends of this queue.
	//
	// This alignment is not automatic: Spool.Append can itself evict whole
	// oldest segments to stay under MaxBytes, which silently removes
	// entries from the front of the spool's own FIFO without spoolMeta
	// knowing. appendToSpool is what keeps the two in step, by trimming the
	// same number of entries from spoolMeta's own front — see its doc
	// comment. Letting the two drift apart is exactly the bug this pairing
	// guards against: peekSpoolMeta would then hand back a cursor map
	// belonging to an entry that is no longer the one Peek returns, and a
	// later ACCEPTED would advance the wrong source's checkpoint.
	spoolMeta []map[string]Cursor
}

// NewShipper validates cfg and returns a Shipper ready to Run.
//
// cfg is taken by pointer per the package convention (see TailConfig):
// several fields here are themselves multi-field structs, which would make
// a by-value cfg exactly what gocritic's hugeParam check flags.
func NewShipper(cfg *ShipperConfig) (*Shipper, error) {
	if cfg == nil {
		return nil, errors.New("shipper: config is required")
	}

	var errs []error
	if cfg.Spool == nil {
		errs = append(errs, errors.New("shipper: spool is required"))
	}
	if cfg.Checkpoint == nil {
		errs = append(errs, errors.New("shipper: checkpoint store is required"))
	}
	if cfg.Labels == nil {
		errs = append(errs, errors.New("shipper: labels resolver is required"))
	}
	if cfg.Ingest.Addr == "" {
		errs = append(errs, errors.New("shipper: ingest address is required"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	s := &Shipper{
		ingestCfg:       cfg.Ingest,
		spool:           cfg.Spool,
		checkpoint:      cfg.Checkpoint,
		extractor:       cfg.Extractor,
		labels:          cfg.Labels,
		maxBatchRecords: cfg.MaxBatchRecords,
		maxBatchBytes:   cfg.MaxBatchBytes,
		maxBatchDelay:   cfg.MaxBatchDelay,
		ackWindow:       cfg.AckWindow,
		minBackoff:      cfg.MinBackoff,
		maxBackoff:      cfg.MaxBackoff,
		metrics:         cfg.Metrics,

		checkpointCommitInterval: defaultCheckpointCommitInterval,
		shutdownAckWait:          defaultShutdownAckWait,

		accums:     make(map[model.StreamID]*accumulator),
		accumTimer: time.NewTimer(idleWait),
	}

	if s.maxBatchRecords <= 0 {
		s.maxBatchRecords = defaultMaxBatchRecords
	}
	if s.maxBatchBytes <= 0 {
		s.maxBatchBytes = defaultMaxBatchBytes
	}
	if s.maxBatchDelay <= 0 {
		s.maxBatchDelay = defaultMaxBatchDelay
	}
	if s.ackWindow <= 0 {
		s.ackWindow = defaultAckWindow
	}
	if s.minBackoff <= 0 {
		s.minBackoff = defaultMinBackoff
	}
	if s.maxBackoff <= 0 {
		s.maxBackoff = defaultMaxBackoff
	}
	if s.minBackoff > s.maxBackoff {
		return nil, fmt.Errorf("shipper: min backoff %s exceeds max backoff %s", s.minBackoff, s.maxBackoff)
	}
	if s.metrics == nil {
		s.metrics = NewMetrics(nil)
	}

	return s, nil
}

// Run consumes lines, ships them, and returns when in closes or ctx is
// canceled.
//
// It never returns an error just because the collector is unreachable —
// that is the condition this whole type exists to survive, per
// ingest.DialLazy's own doc comment — only for a configuration problem
// (a malformed TLS combination, say) caught at dial time, or for a failure
// committing the checkpoint during the final flush.
//
//nolint:contextcheck // sendNow roots each agent.ship span in a background context on purpose; see its comment
func (s *Shipper) Run(ctx context.Context, in <-chan Line) error {
	client, err := ingest.DialLazy(s.ingestCfg)
	if err != nil {
		return fmt.Errorf("shipper: %w", err)
	}
	defer func() { _ = client.Close() }()
	s.client = client

	// Seed one "unknown" placeholder per entry the spool already held
	// before this run started. Those entries either survived a previous
	// crash of this process or were left behind by a drain that never
	// finished; either way nothing in memory remembers which lines
	// produced them, so their checkpoint cursor cannot be recovered. See
	// appendToSpool for the entries this run itself adds, which do carry
	// real cursors.
	for i := 0; i < s.spool.Len(); i++ {
		s.spoolMeta = append(s.spoolMeta, nil)
	}

	commitTicker := time.NewTicker(s.checkpointCommitInterval)
	defer commitTicker.Stop()
	defer s.accumTimer.Stop()

	s.tryConnect(ctx)

runLoop:
	for {
		var connectRetryC <-chan time.Time
		if s.connectRetry != nil {
			connectRetryC = s.connectRetry.C
		}
		var pauseC <-chan time.Time
		if s.pauseTimer != nil {
			pauseC = s.pauseTimer.C
		}
		var ackCh <-chan ackResult
		if s.stream != nil {
			ackCh = s.ackCh
		}

		select {
		case <-ctx.Done():
			break runLoop
		case line, ok := <-in:
			if !ok {
				break runLoop
			}
			s.addLine(&line)
		case <-s.accumTimer.C:
			// Flush every accumulator whose delay deadline has passed, then
			// re-arm for whichever deadline is now earliest. See
			// flushDueAccums and rearmAccumTimer.
			s.flushDueAccums(time.Now())
			s.rearmAccumTimer()
		case <-connectRetryC:
			s.connectRetry = nil
			s.tryConnect(ctx)
		case <-pauseC:
			s.pauseTimer = nil
			s.sendPaused = false
			s.tryDrainSpool()
		case res := <-ackCh:
			// Either a genuine ack, or the stream ending (cleanly or not),
			// which is handled identically to any other disconnect.
			if res.err != nil {
				s.teardownStream()
			} else {
				s.processAck(res.ack)
			}
		case <-commitTicker.C:
			_ = s.checkpoint.Commit()
		}
	}

	return s.shutdown()
}

// tryConnect makes one attempt to open a stream on the shipper's (already
// dialed, lazily-connecting) client. On failure it arms a backoff timer for
// the next attempt rather than retrying immediately, so a down collector
// does not turn this into a busy loop; on success it starts the receiver
// goroutine and immediately looks for spool backlog to drain.
//
//nolint:contextcheck // same root-span reason as Run
func (s *Shipper) tryConnect(ctx context.Context) {
	stream, err := s.client.Stream(ctx)
	if err != nil {
		s.armConnectRetry()
		return
	}
	s.stream = stream
	s.ackCh = make(chan ackResult, s.ackWindow+1)
	go receiveAcks(stream, s.ackCh)
	s.attempt = 0
	s.tryDrainSpool()
}

// armConnectRetry schedules the next connection attempt under full-jitter
// exponential backoff. See backoffDelay for the algorithm and why full
// jitter specifically.
func (s *Shipper) armConnectRetry() {
	s.connectRetry = time.NewTimer(s.backoffDelay())
	s.metrics.Reconnects.Inc()
}

// armPause schedules resuming sends after an OVERLOADED ack. Unlike
// armConnectRetry this does not tear down the stream: OVERLOADED is an
// application-level "not now", not a transport failure.
func (s *Shipper) armPause() {
	s.sendPaused = true
	s.pauseTimer = time.NewTimer(s.backoffDelay())
}

// backoffDelay returns the next backoff duration and advances the shared
// attempt counter. It is shared between connection retries and the pause
// after OVERLOADED because both describe the same thing — "wait, then try
// again" — and ShipperConfig exposes only one pair of bounds for it.
//
// Full jitter (a uniform random draw up to the backoff, not the backoff itself
// or the backoff times a fixed multiplier) is what a fleet of agents
// restarting together needs: without it, every agent computes the same
// deterministic backoff schedule and they all retry in lockstep, turning a
// recovering collector's first moments back up into a thundering herd.
func (s *Shipper) backoffDelay() time.Duration {
	d := backoff.Delay(s.attempt+1, s.minBackoff, s.maxBackoff)
	s.attempt++
	return d
}

// receiveAcks is the one-and-only reader of stream, run on its own
// goroutine so Recv can block waiting for an ack while the sender goroutine
// keeps sending. This is the concurrency the package doc and ingest.Stream's
// doc both insist on: without it, gRPC flow control blocks the server from
// writing an ack once this side stops reading, the server then stops
// reading batches, and a client that sends everything before it reads
// anything deadlocks the moment the ack window fills.
func receiveAcks(stream *ingest.Stream, out chan<- ackResult) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			out <- ackResult{err: err}
			return
		}
		out <- ackResult{ack: ack}
	}
}

// teardownStream abandons the current stream and its receiver goroutine
// (which will exit on its own once Recv finally errors, since nothing reads
// its channel again) and requeues every outstanding batch.
//
// A batch that came from the spool needs no requeuing: it was never
// released, so it is still sitting there exactly where the next
// tryDrainSpool call will find it again. A batch built fresh this run has
// no other copy anywhere, so it must be appended now or it is gone.
func (s *Shipper) teardownStream() {
	s.demoteOutstanding(s.pendingAcks...)
	s.pendingAcks = nil
	s.stream = nil
	s.ackCh = nil
	s.armConnectRetry()
}

// demoteOutstanding appends every batch in batches to the spool, in order,
// skipping any that is already there (fromSpool). It is the common step
// shared by teardownStream (the whole outstanding list), shutdown (whatever
// is still unacked at the deadline) and demoteRemaining (one acked batch plus
// whatever is left of the list) — see demoteRemaining for why the caller's
// ordering matters here.
func (s *Shipper) demoteOutstanding(batches ...outstanding) {
	for _, o := range batches {
		if !o.fromSpool {
			s.appendToSpool(o.batch, o.sourceCursors)
		}
		o.endSpan(codes.Error, "requeued to spool")
	}
}

// demoteRemaining moves o — the batch an OVERLOADED or INTERNAL/UNSPECIFIED
// ack just applied to — and every batch still outstanding behind it into the
// spool, in send order, then clears pendingAcks.
//
// This is what keeps advanceCheckpoint's no-younger-ack-outruns-an-older-
// spooled-batch assumption true: leaving newer batches outstanding while an
// older one goes to the spool would let a younger batch's later ACCEPTED
// advance a source's checkpoint past lines the older, now-spooled batch has
// not yet delivered — a gap, since Spool.Append never fsyncs and a crash in
// that window loses the older batch with the checkpoint already past it.
//
// Order is what makes this correct, not just "spool everything": a batch
// already fromSpool is already sitting in the spool, unreleased, at the
// front (see the outstanding field doc); appendToSpool only ever appends to
// the tail, so o — if it was built fresh this run — lands right behind
// whatever is already there, and every batch still in pendingAcks (sent
// after o, so also newer) lands behind that in the same order it was
// originally sent. Replay order therefore matches original send order.
//
// Any ack the collector later sends for a batch demoted here (it already
// received these batches; nothing stops it from acking them on this same,
// still-open stream) arrives to find pendingAcks empty and is ignored as a
// stray ack — see processAck. That is a deliberate, bounded duplicate, the
// same tradeoff replay from the spool already accepts elsewhere.
func (s *Shipper) demoteRemaining(o outstanding) {
	s.demoteOutstanding(o)
	s.demoteOutstanding(s.pendingAcks...)
	s.pendingAcks = nil
}

// addLine folds one line into the accumulator for its own label set,
// validating and extracting fields along the way, and flushes that
// accumulator — and only that accumulator — when a trigger fires.
//
// Selecting the accumulator by the line's own label set, and never touching
// any other entry in s.accums, is the whole point of keeping one
// accumulator per label set: labels are resolved per source and every
// configured source feeds the same in channel, so lines from different
// sources interleave here even though each source's own lines keep arrival
// order (see the accums field doc). Flushing on every label-set change, the
// way a single shared accumulator once did, would turn that interleaving
// into a flush per line the moment there is more than one source — which is
// the ordinary configuration, not an edge case.
//
// line is taken by pointer purely to keep gocritic's hugeParam check happy —
// Line itself (see source.go) stays a plain value everywhere else,
// including on the channel Run reads from.
func (s *Shipper) addLine(line *Line) {
	labels, ok := s.labels(line.Source)
	if !ok {
		s.countDropped(reasonNoLabels, 1)
		return
	}

	id := labels.ID()
	a, ok := s.accums[id]
	if !ok {
		a = s.newAccumulator(labels)
		s.accums[id] = a
	}

	rec := model.LogRecord{
		Time:    line.Time,
		Seq:     line.Cursor.Start,
		Message: string(line.Bytes),
	}
	if s.extractor != nil {
		rec.Fields = s.extractor.Fields(line.Bytes)
	}
	rec.Level = levelOf(rec.Fields)

	var pb *logaggv1.LogRecord
	var size int
	valid := rec.Validate(time.Now()) == nil
	if valid {
		pb = rec.Proto()
		size = proto.Size(pb)

		if len(a.records) > 0 && a.overhead+a.bytes+size > s.maxBatchBytes {
			// Does not fit beside what is already batched in this stream's
			// accumulator. Flush that batch now, before this line is
			// attributed to anything: attributing it to the batch being
			// flushed would let a crash between that batch's ack and this
			// line's own batch's ack skip re-reading a line whose record
			// was never actually shipped. This only ever flushes this one
			// label set's accumulator — every other entry in s.accums is
			// untouched.
			s.flushAccum(id)
			a = s.newAccumulator(labels)
			s.accums[id] = a
		}

		// Re-check against whichever accumulator the record will actually go
		// into — a is now either the original one (already empty, or with
		// room beside its existing records) or the brand-new replacement
		// just created above. Checking only once, here, after any flush has
		// already happened, is what catches a record too big to ever fit
		// even alone: without this re-check, a record that does not fit
		// beside an existing batch gets a fresh, empty accumulator via the
		// flush above but then skips this test entirely, letting a record
		// larger than MaxBatchBytes itself (a single record can legitimately
		// reach ~576 KiB; see model.MaxMessageLen and model.MaxFields) into a
		// batch the collector will refuse outright instead of being dropped
		// and counted here, where the offending source is visible.
		if a.overhead+size > s.maxBatchBytes {
			s.countDropped(reasonRecordTooLarge, 1)
			valid = false
		}
	} else {
		s.countDropped(reasonInvalidRecord, 1)
	}

	// From here a is definitely the accumulator this line belongs to —
	// whether or not its record made it in — so the cursor bookkeeping is
	// safe to attach now, not before. An invalid or oversized record still
	// advances it: that record is being permanently dropped, not retried
	// (see model.LogRecord.Validate's caller contract), so the checkpoint
	// may still move past it once this accumulator's survivors, if any,
	// are acknowledged.
	a.sourceCursors[line.Source] = line.Cursor
	if valid {
		a.records = append(a.records, pb)
		a.bytes += size
	}

	if len(a.records) >= s.maxBatchRecords {
		s.flushAccum(id)
	}

	// A flush above may have removed this accumulator, or creation above may
	// have added one with a later or earlier deadline than whatever was
	// already the soonest; either way the shared timer needs to reflect the
	// current earliest deadline across s.accums.
	s.rearmAccumTimer()
}

// newAccumulator starts a fresh batch for labels, recording its flush
// deadline immediately so MaxBatchDelay is measured from creation, not from
// the first successfully-added record. It does not arm anything itself —
// the caller (addLine) re-arms the shared accumTimer once, after all of an
// addLine call's accumulator bookkeeping is done.
func (s *Shipper) newAccumulator(labels model.LabelSet) *accumulator {
	// Cloned defensively: this accumulator holds labels across many addLine
	// calls until it flushes, and LabelSet.Clone's own doc comment is
	// exactly this situation — a caller keeping a LabelSet past the
	// lifetime of the call that produced it.
	labels = labels.Clone()
	// Sized with everything sendNow adds later, so a batch filled to
	// MaxBatchBytes still fits under the collector's receive ceiling: a
	// batch ID as long as the counter can make one, and a W3C traceparent.
	envelope := &logaggv1.LogBatch{
		BatchId:      fmt.Sprintf("agent-%d", uint64(math.MaxUint64)),
		Labels:       labels.Proto(),
		TraceContext: map[string]string{"traceparent": strings.Repeat("0", 55)},
	}
	return &accumulator{
		labels:        labels,
		overhead:      proto.Size(envelope),
		sourceCursors: make(map[string]Cursor),
		deadline:      time.Now().Add(s.maxBatchDelay),
	}
}

// flushAccum completes the accumulator for id, if one exists, and either
// dispatches it or — if it ended up with no shippable records at all —
// advances the checkpoint for it directly, since there is then no batch and
// no ack ever coming to advance it later. It touches only the accumulator
// named by id; every other entry in s.accums is left exactly as it was.
func (s *Shipper) flushAccum(id model.StreamID) {
	a, ok := s.accums[id]
	if !ok {
		return
	}
	delete(s.accums, id)

	if len(a.records) == 0 {
		for source, cur := range a.sourceCursors {
			s.advanceCheckpoint(source, cur)
		}
		return
	}

	s.batchSeq++
	batch := &logaggv1.LogBatch{
		BatchId: fmt.Sprintf("agent-%d", s.batchSeq),
		Labels:  a.labels.Proto(),
		Records: a.records,
	}
	s.dispatch(batch, a.sourceCursors)
}

// flushDueAccums flushes every accumulator whose deadline has passed as of
// now, in ascending StreamID order.
//
// Sorted because Go randomizes map iteration order, and more than one
// accumulator can be flushed in a single pass — here, or on shutdown
// (flushAllAccums) — which means more than one batch can be produced in a
// single pass too. Sorting the keys first makes the sequence of batches a
// collector sees in that case reproducible from one run to the next, rather
// than dependent on map iteration order, which would otherwise make tests
// asserting on batch order flaky under `go test -count=2`.
func (s *Shipper) flushDueAccums(now time.Time) {
	for _, id := range slices.Sorted(maps.Keys(s.accums)) {
		if s.accums[id].deadline.After(now) {
			continue
		}
		s.flushAccum(id)
	}
}

// flushAllAccums flushes every remaining accumulator, in ascending StreamID
// order (see flushDueAccums for why sorted).
func (s *Shipper) flushAllAccums() {
	for _, id := range slices.Sorted(maps.Keys(s.accums)) {
		s.flushAccum(id)
	}
}

// rearmAccumTimer resets the shared accumulator delay timer to fire when the
// earliest deadline across every accumulator in s.accums is next due, or
// after idleWait if nothing is accumulating. Called after every addLine
// (which may create, grow or flush an accumulator) and after every timer
// sweep, so the timer always reflects the current set.
//
// Resetting directly, without a Stop first, mirrors multiline.go's
// nextWait/runJoin pattern: this timer is only ever read and reset by the
// one goroutine running Run's select loop, so there is no concurrent
// Stop/Reset race for the documented Timer.Reset caveats to apply to.
func (s *Shipper) rearmAccumTimer() {
	s.accumTimer.Reset(nextWait(s.accums, func(a *accumulator) time.Time { return a.deadline }, time.Now()))
}

// dispatch sends a freshly-built batch directly if the shipper is connected,
// not paused, has ack-window room, and — the ordering guarantee decision —
// has nothing already waiting in the spool. Once the spool is non-empty for
// any reason, every fresh batch is appended behind whatever is already
// there instead of being sent directly, which is what keeps replayed and
// live batches from interleaving on the wire: interleaving them would let a
// live batch's ack advance a source's checkpoint past data an older,
// still-unacked replayed batch from the very same source has not yet
// delivered.
func (s *Shipper) dispatch(batch *logaggv1.LogBatch, cursors map[string]Cursor) {
	if s.stream == nil || s.sendPaused || s.spool.Len() > 0 || len(s.pendingAcks) >= s.ackWindow {
		s.appendToSpool(batch, cursors)
		return
	}
	s.sendNow(batch, cursors, false)
}

// sendNow puts batch on the wire and records it as outstanding regardless of
// whether Send reported an error: per ingest.Stream.Send's doc, a send error
// is usually stale (the real reason arrives on Recv), so the batch is kept
// exactly as if it had gone out cleanly and teardownStream — triggered
// either here or later by the receiver goroutine — is what decides its fate.
func (s *Shipper) sendNow(batch *logaggv1.LogBatch, cursors map[string]Cursor, fromSpool bool) {
	// The trace starts here, not at the line read: a batch is the unit the
	// rest of the pipeline sees. A spooled batch gets a fresh span per attempt
	// with the map rebuilt, so a stale tracestate from a previous run cannot
	// ride along. A root span has no parent by definition, so the background
	// context is the right one; the run context's cancellation must not end
	// a span that an in-flight ack will still settle.
	ctx, span := tracer.Start(context.Background(), "agent.ship", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("batch.id", batch.GetBatchId()),
			attribute.Int("batch.records", len(batch.GetRecords())),
			attribute.Bool("batch.from_spool", fromSpool),
		))
	batch.TraceContext = make(map[string]string, 2)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(batch.TraceContext))

	err := s.stream.Send(batch)
	s.pendingAcks = append(s.pendingAcks, outstanding{batch: batch, sourceCursors: cursors, fromSpool: fromSpool, span: span})
	if err != nil {
		s.teardownStream()
	}
}

// tryDrainSpool sends the oldest spooled batch, if the shipper is connected,
// not paused, and not already waiting on something. It sends at most one
// spool entry at a time by construction: Spool.Peek always returns the same
// oldest unreleased entry until Release is called, and Release only happens
// once that entry's ack arrives, so pipelining several spooled batches at
// once is not something the spool's own API allows — nor is it needed, since
// decision 2 already serializes replay behind exactly this one-at-a-time
// drain.
func (s *Shipper) tryDrainSpool() {
	if s.stream == nil || s.sendPaused || len(s.pendingAcks) > 0 {
		return
	}
	for s.spool.Len() > 0 {
		payload, ok, err := s.spool.Peek()
		if err != nil || !ok {
			return
		}

		var batch logaggv1.LogBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			// Corruption the spool's own checksum did not catch (see the
			// Spool doc comment on Peek). Retrying it would only fail this
			// same way forever, so it is dropped and counted instead of
			// blocking the rest of the spool behind it.
			s.countDropped(reasonCorruptSpoolEntry, 1)
			_ = s.spool.Release()
			s.popSpoolMeta()
			continue
		}

		s.sendNow(&batch, s.peekSpoolMeta(), true)
		return
	}
}

// processAck applies one ack to the oldest outstanding batch — acks on one
// stream arrive in the order their batches were sent, per the ingest
// handler's own strictly-in-order contract, which is what makes a plain FIFO
// pop correct here without matching on batch ID.
func (s *Shipper) processAck(ack *logaggv1.Ack) {
	if len(s.pendingAcks) == 0 {
		// A stray ack with nothing outstanding would be a collector
		// protocol violation; there is no batch left to attribute it to,
		// so there is nothing safe to do but ignore it.
		return
	}
	o := s.pendingAcks[0]
	s.pendingAcks = s.pendingAcks[1:]
	s.metrics.Acks.WithLabelValues(ackLabel(ack.GetCode())).Inc()
	// Ended here, before the demote paths below get a chance to end it with
	// their generic text: the collector's reason is the useful one.
	if ack.GetCode() == logaggv1.AckCode_ACK_CODE_ACCEPTED {
		o.endSpan(codes.Ok, "")
	} else {
		o.endSpan(codes.Error, ackLabel(ack.GetCode())+": "+ack.GetDetail())
	}

	switch ack.GetCode() {
	case logaggv1.AckCode_ACK_CODE_ACCEPTED:
		for source, cur := range o.sourceCursors {
			s.advanceCheckpoint(source, cur)
		}
		if o.fromSpool {
			_ = s.spool.Release()
			s.popSpoolMeta()
		}
		s.attempt = 0
	case logaggv1.AckCode_ACK_CODE_OVERLOADED:
		// Demote o and every batch still outstanding behind it together —
		// see demoteRemaining for why leaving newer batches outstanding
		// here would reopen this same finding.
		s.demoteRemaining(o)
		s.armPause()
	case logaggv1.AckCode_ACK_CODE_INVALID:
		// Resending identical bytes fails identically (see the AckCode doc
		// in the proto), so this batch is done: drop it, count every
		// record in it, and never advance the checkpoint for it — the data
		// is gone, not merely delayed.
		s.countDropped(reasonAckInvalid, len(o.batch.GetRecords()))
		if o.fromSpool {
			_ = s.spool.Release()
			s.popSpoolMeta()
		}
	default: // ACK_CODE_INTERNAL and ACK_CODE_UNSPECIFIED
		// Same demotion as OVERLOADED, for the same reason — see
		// demoteRemaining.
		s.demoteRemaining(o)
		// No backoff: a collector-side fault is retried at the same rate,
		// not backed off, so one transient fault does not throttle the
		// agent the way sustained overload should.
	}
	s.tryDrainSpool()
}

// appendToSpool marshals batch and appends it to the spool, recording its
// per-source cursors in spoolMeta so a later successful drain can still
// advance the checkpoint correctly.
//
// spoolMeta and the spool must be trimmed together: Spool.Append reports how
// many entries its own MaxBytes eviction dropped from the front of the
// spool's FIFO, and that many entries are dropped from the front of spoolMeta
// here too, after this call's own cursors are appended to the back. Eviction
// always removes the oldest entries, and spoolMeta[0] is always the oldest
// entry's metadata, so trimming from the front realigns the two exactly —
// leaving them misaligned is what would let a later ACCEPTED advance the
// wrong source's checkpoint (see the spoolMeta field doc).
func (s *Shipper) appendToSpool(batch *logaggv1.LogBatch, cursors map[string]Cursor) {
	data, err := proto.Marshal(batch)
	if err != nil {
		// Marshaling a message this package built itself should not fail;
		// if it somehow does, the batch cannot be held onto any other way.
		s.countDropped(reasonEncodeFailed, len(batch.GetRecords()))
		return
	}
	evicted, err := s.spool.Append(data)
	if err != nil {
		// Disk exhaustion or similar: the batch is already off the network
		// path, so there is nowhere else for it to go.
		s.countDropped(reasonSpoolAppendFailed, len(batch.GetRecords()))
		return
	}
	s.spoolMeta = append(s.spoolMeta, cursors)
	// Defensive bound, not an expected case: as long as spoolMeta stays
	// aligned with the spool (which is this whole function's job), eviction
	// can never remove more entries than spoolMeta already tracks — nil
	// placeholders for pre-existing entries (see the spoolMeta field doc and
	// Run) count too, so it is never short. Capping evicted here means a
	// future bug in that invariant becomes a lost cursor map, not a slice
	// bound panic that takes the whole agent down with it.
	if evicted > len(s.spoolMeta) {
		evicted = len(s.spoolMeta)
	}
	s.spoolMeta = s.spoolMeta[evicted:]
}

// peekSpoolMeta returns the cursor map for the oldest spool entry without
// removing it, mirroring Spool.Peek's own "look but do not advance"
// contract.
func (s *Shipper) peekSpoolMeta() map[string]Cursor {
	if len(s.spoolMeta) == 0 {
		return nil
	}
	return s.spoolMeta[0]
}

// popSpoolMeta discards the oldest spool entry's cursor map, mirroring a
// completed Spool.Release for that same entry.
func (s *Shipper) popSpoolMeta() {
	if len(s.spoolMeta) == 0 {
		return
	}
	s.spoolMeta = s.spoolMeta[1:]
}

// advanceCheckpoint sets source's cursor unless doing so would move it backward
// within the same file generation. In-order acks on one stream should make that
// impossible, but the cost of checking is one comparison against the cost of
// silently resuming a source too early — see CheckpointStore's doc comment on
// the ack-only rule this guards.
//
// The comparison is scoped to a matching FileID, and that scoping is not
// optional. A file source's Offset restarts near zero when the file rotates (see
// TailSource.rotate), so the first cursor of a new generation legitimately has a
// smaller Offset than the last cursor of the old one. Comparing Offset alone
// would discard it and freeze the checkpoint on the old generation permanently:
// every restart would then re-read the whole current file and report a missed
// generation that never happened, turning the one metric that means real data
// loss into a false alarm. Across a change of generation, "backward" is not a
// thing that can be judged by offset at all.
//
// A stale cursor from the old generation arriving after a new one would pass
// this check, but cannot happen: acks on one stream arrive in send order, and
// spool replay is serialized ahead of live batches for exactly that reason —
// dispatch never sends a fresh batch directly while the spool is non-empty,
// and demoteRemaining is what keeps the spool non-empty for every batch
// still outstanding once an older one from the same stream is demoted to it,
// so a younger batch's ack can never outrun an older, still-unacked spooled
// one from the same source.
func (s *Shipper) advanceCheckpoint(source string, cur Cursor) {
	current, ok := s.checkpoint.Get(source)
	if ok && cur.File == current.File && cur.Offset < current.Offset {
		return
	}
	s.checkpoint.Set(source, cur)
}

// levelOf reads the record's level from an extracted "level" field, accepting
// every spelling model.ParseLevel does. No field, or one it does not
// recognize, is LevelUnspecified: the model package's own answer for "no
// level known", not a guess. Inference from the message text is deliberately
// not attempted.
func levelOf(fields map[string]string) model.Level {
	if lvl, err := model.ParseLevel(fields["level"]); err == nil {
		return lvl
	}
	return model.LevelUnspecified
}

// countDropped records n records dropped for reason under this package's
// shared, agent-component slice of observability.RecordsDropped.
func (s *Shipper) countDropped(reason string, n int) {
	if n <= 0 {
		return
	}
	s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reason).Add(float64(n))
}

// shutdown flushes the active accumulator, gives outstanding batches a
// bounded window to be acknowledged, spools whatever is not, and commits the
// checkpoint once more. It runs whether Run is stopping because in closed or
// because ctx was canceled — both cases want exactly this: nothing in flight
// may simply be discarded, but nothing here may block indefinitely either.
func (s *Shipper) shutdown() error {
	s.flushAllAccums()

	if s.stream != nil {
		deadline := time.NewTimer(s.shutdownAckWait)
	waitAcks:
		for len(s.pendingAcks) > 0 {
			select {
			case res := <-s.ackCh:
				if res.err != nil {
					break waitAcks
				}
				s.processAck(res.ack)
			case <-deadline.C:
				break waitAcks
			}
		}
		deadline.Stop()
		_ = s.stream.CloseSend()
	}

	s.demoteOutstanding(s.pendingAcks...)
	s.pendingAcks = nil

	return s.checkpoint.Commit()
}

// ackLabel is the short metric label for an ack code, mirroring
// internal/ingest's own ackCodeLabel so the two packages' dashboards read
// the same way.
func ackLabel(code logaggv1.AckCode) string {
	switch code {
	case logaggv1.AckCode_ACK_CODE_ACCEPTED:
		return "accepted"
	case logaggv1.AckCode_ACK_CODE_OVERLOADED:
		return "overloaded"
	case logaggv1.AckCode_ACK_CODE_INVALID:
		return "invalid"
	default:
		return "internal"
	}
}
