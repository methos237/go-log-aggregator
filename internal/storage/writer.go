package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// Writer errors.
var (
	// ErrWriterClosed is returned by Submit after Close has begun. Callers treat it
	// as retryable against a different node, not as data loss: nothing was
	// acknowledged.
	ErrWriterClosed = errors.New("writer is closed")
	// ErrWriterNotStarted guards against submitting to a writer whose workers were
	// never launched, which would otherwise block forever.
	ErrWriterNotStarted = errors.New("writer has not been started")
)

// logColumns is the column order used by both the COPY into staging and the
// INSERT out of it. One list, used twice, so the two cannot drift apart.
var logColumns = []string{"time", "stream_id", "seq", "level", "message", "trace_id", "span_id", "fields"}

// createStagingSQL prepares the per-connection staging table.
//
// Temporary rather than unlogged-permanent: temp tables are invisible to other
// sessions and not WAL-logged, so two writer goroutines on two pooled connections
// cannot collide, and the staging write costs no durability.
//
// IF NOT EXISTS plus ON COMMIT DELETE ROWS means the table is created once per
// pooled connection and merely emptied on every subsequent commit, instead of
// churning the catalog on every batch.
const createStagingSQL = `
CREATE TEMP TABLE IF NOT EXISTS logs_staging (LIKE logs) ON COMMIT DELETE ROWS`

// insertFromStagingSQL moves the staged rows into the hypertable.
//
// This two-step exists because COPY has no ON CONFLICT clause. Copying straight
// into logs would abort the entire batch on the first redelivered record, and
// at-least-once delivery guarantees redeliveries happen. ON CONFLICT DO NOTHING
// against the logs_dedup index turns a replay into a no-op instead.
//
// No conflict target is named: any unique index on logs should suppress a
// duplicate, and naming one would silently stop covering a later index.
//
// ORDER BY matches the dedup index. It buys two things: a single global lock order
// across writers, so two nodes inserting overlapping keys cannot deadlock, and
// better index locality, since consecutive rows land on the same index pages instead
// of walking the whole B-tree per row.
const insertFromStagingSQL = `
INSERT INTO logs (time, stream_id, seq, level, message, trace_id, span_id, fields)
SELECT time, stream_id, seq, level, message, trace_id, span_id, fields
FROM logs_staging
ORDER BY stream_id, seq, time
ON CONFLICT DO NOTHING`

// Shipment is the unit the writer accepts: one stream's labels plus records for it.
//
// Records are grouped by stream because that is how they arrive — a gRPC batch and
// a JetStream message each carry one label set — and because the writer has to
// upsert the stream row before the foreign key on logs will accept the records.
type Shipment struct {
	// Stream is the label set these records belong to. Its ID overwrites each
	// record's StreamID, so a caller cannot accidentally ship records tagged with a
	// stream it did not send.
	Stream model.Stream
	// Records is taken over by Submit, which stamps the stream ID onto each entry
	// and compacts out any that fail validation. Callers must not read or reuse the
	// slice afterwards.
	Records []model.LogRecord

	// Ack is called exactly once, with nil when every accepted record is durably
	// in the hypertable and with an error when the writer gave up.
	//
	// This is what makes end-to-end at-least-once work: the ingest path passes a
	// closure that acknowledges the JetStream message, so a crash before the write
	// lands leads to redelivery rather than to loss. Ack is invoked only when
	// Submit reported at least one accepted record.
	Ack func(error)

	// enqueued is stamped by Submit so the queue-wait histogram measures real
	// waiting rather than the caller's own latency.
	enqueued time.Time
}

// Writer is a bounded pool of goroutines that batch records into the hypertable.
//
// The concurrency shape, which is the interesting part:
//
//   - One bounded channel of shipments feeds every worker. Bounded is not
//     negotiable: an unbounded channel would turn a slow database into an OOM,
//     which is the failure mode this design exists to avoid.
//   - Submit blocks when that channel is full rather than shedding. Backpressure
//     belongs upstream — the queue is the buffer, and JetStream depth is the signal
//     an operator alerts on. Load shedding happens at ingest (phase 2), where there
//     is still an unacknowledged client to say "slow down" to.
//   - Each worker owns its own batch, so there is no shared mutable batch and no
//     lock on the hot path. The only shared state is the stream cache, which is
//     shared precisely because a per-worker cache would multiply cache misses by
//     the worker count.
type Writer struct {
	pool    *pgxpool.Pool
	cfg     config.Writer
	log     *slog.Logger
	metrics *Metrics
	streams *streamCache

	in chan Shipment
	// depth tracks queued records, not queued shipments, because that is the number
	// that says how much memory the queue is holding.
	depth atomic.Int64

	started atomic.Bool
	closing atomic.Bool
	// quit is closed by Close to start a graceful drain. It is never used to
	// interrupt a database call; see Start.
	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// now is injectable so the flush timing and last_seen refresh logic are
	// testable without sleeping.
	now func() time.Time
}

// NewWriter builds a writer. Call Start before Submit.
//
// metrics may be nil, in which case nothing is recorded; that keeps tests that
// only care about rows from having to build a registry.
//
//nolint:gocritic // hugeParam: config is copied once per process; by value keeps it immutable
func NewWriter(pool *pgxpool.Pool, cfg config.Writer, metrics *Metrics, log *slog.Logger) (*Writer, error) {
	if pool == nil {
		return nil, errors.New("writer needs a connection pool")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("writer config: %w", err)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Writer{
		pool:    pool,
		cfg:     cfg,
		log:     log.With(slog.String("component", "writer")),
		metrics: metrics,
		streams: newStreamCache(cfg.StreamCacheSize, cfg.StreamRefreshInterval, metrics),
		in:      make(chan Shipment, cfg.QueueDepth),
		quit:    make(chan struct{}),
		now:     time.Now,
	}, nil
}

// Start launches the worker goroutines. It returns immediately.
//
// Canceling ctx starts the same graceful drain that Close does. Note what ctx is
// *not* used for: the database calls run under context.WithoutCancel(ctx) plus a
// per-attempt timeout, because the point of a graceful shutdown is to flush the
// records already in hand, and handing the flush an already-canceled context
// would abort exactly the work being drained.
func (w *Writer) Start(ctx context.Context) {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	dbCtx := context.WithoutCancel(ctx)

	for i := 0; i < w.cfg.Workers; i++ {
		w.wg.Add(1)
		go w.work(ctx, dbCtx, i)
	}
	w.log.Info("writer started",
		slog.Int("workers", w.cfg.Workers),
		slog.Int("batch_size", w.cfg.BatchSize),
		slog.Duration("flush_interval", w.cfg.FlushInterval),
		slog.Int("queue_depth", w.cfg.QueueDepth),
	)
}

// Submit validates a shipment and queues its records, blocking while the queue is
// full. It reports how many records were accepted.
//
// Records that fail validation are dropped here rather than at flush time. One
// oversized message would otherwise fail the COPY for the whole batch and then
// fail every retry of it, so a single bad record would block a stream indefinitely
// — the classic poison-message failure.
//
// The returned count is what phase 2 puts in Ack.accepted. Ack is called later, and
// only if that count is above zero.
// a pointer would put every shipment on the heap to travel through the channel.
//
//nolint:gocritic // hugeParam: one copy per batch of up to BatchSize records, and passing
func (w *Writer) Submit(ctx context.Context, sh Shipment) (int, error) {
	if !w.started.Load() {
		return 0, ErrWriterNotStarted
	}
	if w.closing.Load() {
		return 0, ErrWriterClosed
	}
	if err := sh.Stream.Labels.Validate(); err != nil {
		// The whole shipment goes: without a valid label set there is no stream row
		// to hang the records off.
		w.drop(reasonInvalid, len(sh.Records))
		return 0, fmt.Errorf("invalid stream %s: %w", sh.Stream.Labels, err)
	}

	now := w.now()
	sh.Stream.ID = sh.Stream.Labels.ID()
	sh.Records = w.acceptable(sh.Stream.ID, sh.Records, now)
	if len(sh.Records) == 0 {
		return 0, nil
	}
	sh.enqueued = now

	accepted := len(sh.Records)
	w.addDepth(accepted)

	select {
	case w.in <- sh:
		return accepted, nil
	case <-w.quit:
		w.addDepth(-accepted)
		w.drop(reasonShutdown, accepted)
		return 0, ErrWriterClosed
	case <-ctx.Done():
		w.addDepth(-accepted)
		// Counted as queue_full because that is why the caller waited long enough
		// for its context to expire.
		w.drop(reasonQueueFull, accepted)
		return 0, fmt.Errorf("submit to writer: %w", ctx.Err())
	}
}

// acceptable stamps the stream ID onto each record and compacts the invalid ones
// out, returning the survivors.
//
// Filtering happens in place. Submit therefore takes ownership of sh.Records —
// documented on Shipment — which keeps the common case (nothing invalid) free of
// allocation on the hottest path in the write pipeline.
func (w *Writer) acceptable(id model.StreamID, records []model.LogRecord, now time.Time) []model.LogRecord {
	kept := records[:0]
	dropped := 0
	for i := range records {
		records[i].StreamID = id
		if err := records[i].Validate(now); err != nil {
			w.log.Debug("dropping invalid record",
				slog.Int64("stream_id", int64(id)),
				slog.Int64("seq", records[i].Seq),
				slog.Any("error", err),
			)
			dropped++
			continue
		}
		kept = append(kept, records[i])
	}
	w.drop(reasonInvalid, dropped)
	return kept
}

// Close stops accepting work, drains the queue and waits for the workers.
//
// ctx bounds the wait. If it expires, Close returns an error and the process should
// treat the remaining records as lost — they were never acknowledged upstream, so
// the queue will redeliver them, which is the whole point of acking after the write.
func (w *Writer) Close(ctx context.Context) error {
	if !w.started.Load() {
		return nil
	}

	w.closeOnce.Do(func() {
		w.closing.Store(true)
		close(w.quit)
	})

	drained := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-ctx.Done():
		return fmt.Errorf("writer did not drain in time: %w", ctx.Err())
	}

	// A Submit that was mid-select when quit closed may have won the send race and
	// left a shipment behind. Failing it explicitly is the difference between a
	// caller that retries and a caller that waits forever for an Ack.
	w.drainAbandoned()

	w.log.Info("writer stopped")
	return nil
}

func (w *Writer) drainAbandoned() {
	for {
		select {
		case sh := <-w.in:
			w.addDepth(-len(sh.Records))
			w.drop(reasonShutdown, len(sh.Records))
			w.ack(&sh, ErrWriterClosed)
		default:
			return
		}
	}
}

// work is one worker: accumulate records until the batch is full or the flush
// interval expires, then write.
//
// Size and time triggers are both needed. Size alone would leave the last partial
// batch of a quiet stream unwritten indefinitely; time alone would cap throughput
// at one batch per interval.
func (w *Writer) work(ctx, dbCtx context.Context, id int) {
	defer w.wg.Done()

	log := w.log.With(slog.Int("worker", id))
	batch := newWriteBatch(w.cfg.BatchSize)

	// The timer only runs while a batch is open. An always-armed ticker would wake
	// every worker FlushInterval times a second while the system is idle.
	timer := time.NewTimer(w.cfg.FlushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	var flushAt <-chan time.Time

	stopTimer := func() {
		if flushAt == nil {
			return
		}
		if !timer.Stop() {
			// Already fired: drain it, or the next Reset would flush immediately.
			select {
			case <-timer.C:
			default:
			}
		}
		flushAt = nil
	}

	for {
		select {
		case sh := <-w.in:
			w.addDepth(-len(sh.Records))
			w.observeQueueWait(&sh)
			batch.add(&sh, w.streams, w.now())

			if flushAt == nil {
				timer.Reset(w.cfg.FlushInterval)
				flushAt = timer.C
			}
			if batch.full(w.cfg.BatchSize) {
				stopTimer()
				w.flush(dbCtx, log, batch)
			}

		case <-flushAt:
			flushAt = nil
			w.flush(dbCtx, log, batch)

		case <-w.quit:
			stopTimer()
			w.drainInto(dbCtx, log, batch)
			return

		case <-ctx.Done():
			stopTimer()
			w.drainInto(dbCtx, log, batch)
			return
		}
	}
}

// drainInto empties the queue into the current batch, flushing as it fills, and
// then writes whatever is left. Called once per worker on shutdown.
//
// Draining without blocking is what makes this terminate: every worker is running
// the same loop, so anything still queued is claimed by whichever worker reaches it
// first, and each worker stops as soon as the channel is momentarily empty.
func (w *Writer) drainInto(dbCtx context.Context, log *slog.Logger, batch *writeBatch) {
	for {
		select {
		case sh := <-w.in:
			w.addDepth(-len(sh.Records))
			w.observeQueueWait(&sh)
			batch.add(&sh, w.streams, w.now())
			if batch.full(w.cfg.BatchSize) {
				w.flush(dbCtx, log, batch)
			}
		default:
			w.flush(dbCtx, log, batch)
			return
		}
	}
}

// flush writes the batch, retrying transient failures, and resets it.
func (w *Writer) flush(dbCtx context.Context, log *slog.Logger, batch *writeBatch) {
	if batch.empty() {
		return
	}
	defer batch.reset()

	started := w.now()
	if w.metrics != nil {
		w.metrics.BatchSize.Observe(float64(len(batch.records)))
	}

	var err error
	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		var inserted int64
		inserted, err = w.writeOnce(dbCtx, batch)
		if err == nil {
			w.recordSuccess(batch, inserted, w.now().Sub(started))
			batch.ackAll(nil)
			return
		}

		retryable := isRetryable(err)
		reason := reasonNonRetryable
		if retryable {
			reason = reasonRetryable
		}
		if w.metrics != nil {
			w.metrics.WriteRetries.WithLabelValues(reason).Inc()
		}

		if !retryable || attempt == w.cfg.MaxAttempts {
			log.Error("batch write failed",
				slog.Int("attempt", attempt),
				slog.Int("records", len(batch.records)),
				slog.Bool("retryable", retryable),
				slog.Any("error", err),
			)
			break
		}

		delay := backoff(attempt, w.cfg.RetryBaseDelay, w.cfg.RetryMaxDelay)
		log.Warn("batch write failed, retrying",
			slog.Int("attempt", attempt),
			slog.Int("records", len(batch.records)),
			slog.Duration("delay", delay),
			slog.Any("error", err),
		)
		if !sleep(dbCtx, delay) {
			break
		}
	}

	if w.metrics != nil {
		w.metrics.WriteDuration.WithLabelValues(outcomeFailure).Observe(w.now().Sub(started).Seconds())
	}
	w.drop(reasonWriteFailed, len(batch.records))
	batch.ackAll(fmt.Errorf("write batch of %d records: %w", len(batch.records), err))
}

func (w *Writer) recordSuccess(batch *writeBatch, inserted int64, took time.Duration) {
	copied := int64(len(batch.records))
	if w.metrics == nil {
		return
	}
	w.metrics.WriteDuration.WithLabelValues(outcomeSuccess).Observe(took.Seconds())
	w.metrics.RowsCopied.Add(float64(copied))
	w.metrics.RowsInserted.Add(float64(inserted))
	if deduped := copied - inserted; deduped > 0 {
		w.metrics.RowsDeduped.Add(float64(deduped))
	}
}

// writeOnce performs one attempt: make sure the streams exist, then write the records.
// It returns the number of rows that reached the hypertable.
func (w *Writer) writeOnce(ctx context.Context, batch *writeBatch) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.WriteTimeout)
	defer cancel()

	if err := w.commitStreams(ctx, batch); err != nil {
		return 0, err
	}
	return w.commitRecords(ctx, batch)
}

// commitStreams upserts the batch's streams in their own short transaction.
//
// Deliberately *not* in the same transaction as the records, and this is the single
// least obvious decision in the package. Sharing a transaction deadlocks under
// concurrency, reproducibly:
//
//   - ON CONFLICT DO UPDATE takes an exclusive row lock on the stream row and holds it
//     until commit, which with a shared transaction means for the whole duration of a
//     5000-row COPY.
//   - The insert into logs creates hypertable chunks, and Timescale serializes chunk
//     creation on its own catalog lock.
//
// Two writers therefore acquire {stream row, chunk catalog} in whichever order they
// happen to reach them, and PostgreSQL reports the cycle as SQLSTATE 40P01. It showed
// up as roughly one lost batch in four on a 20k-record run, and only under real
// concurrency -- a single-writer test never sees it.
//
// Splitting them means the exclusive row lock is held for microseconds and released
// before any chunk lock is taken. The records transaction then only needs KEY SHARE on
// the stream row for the foreign key check, which is a shared lock that concurrent
// writers can all hold at once.
//
// The cost is that a crash between the two commits leaves a stream row with no
// records. That is harmless: streams is a dimension table, and a row there means "this
// label set was seen", which was true. The reverse ordering -- records first -- would
// violate the foreign key and lose the batch, which is why it is this way round.
func (w *Writer) commitStreams(ctx context.Context, batch *writeBatch) error {
	streams := batch.streamSlice()
	if len(streams) == 0 {
		return nil
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin stream upsert: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := upsertStreams(ctx, tx, streams); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %d streams: %w", len(streams), err)
	}

	// Cached only now, after the commit. Caching earlier would let a rolled-back
	// upsert leave the cache claiming a stream exists, and the next batch's records
	// would fail their foreign key.
	at := w.now()
	for _, s := range streams {
		w.streams.markUpserted(s.ID, at)
	}
	if w.metrics != nil {
		w.metrics.StreamUpserts.Add(float64(len(streams)))
	}

	// Cleared so a retry of the records transaction does not redo work that is already
	// durable.
	batch.streamsCommitted()
	return nil
}

// commitRecords stages the batch with COPY and moves it into the hypertable.
func (w *Writer) commitRecords(ctx context.Context, batch *writeBatch) (int64, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	// Rollback after a successful Commit is a documented no-op in pgx, so this
	// unconditional deferred rollback is the safe way to guarantee the transaction
	// is never left open on an early return.
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err = tx.Exec(ctx, createStagingSQL); err != nil {
		return 0, fmt.Errorf("create staging table: %w", err)
	}

	records := batch.records
	copied, err := tx.CopyFrom(ctx,
		pgx.Identifier{"logs_staging"},
		logColumns,
		pgx.CopyFromSlice(len(records), func(i int) ([]any, error) {
			return copyRow(&records[i])
		}),
	)
	if err != nil {
		return 0, fmt.Errorf("copy %d records into staging: %w", len(records), err)
	}
	if copied != int64(len(records)) {
		return 0, fmt.Errorf("copied %d of %d records into staging", copied, len(records))
	}

	tag, err := tx.Exec(ctx, insertFromStagingSQL)
	if err != nil {
		return 0, fmt.Errorf("insert %d records from staging: %w", copied, err)
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit %d records: %w", copied, err)
	}
	return tag.RowsAffected(), nil
}

// copyRow renders one record in logColumns order.
//
// Nil rather than an empty slice or "{}" for absent optional columns: NULL costs a
// bit in the row header, while an empty JSONB value costs bytes per row and makes
// "has fields" a value comparison instead of a null check.
func copyRow(r *model.LogRecord) ([]any, error) {
	var fields []byte
	if len(r.Fields) > 0 {
		encoded, err := marshalFields(r.Fields)
		if err != nil {
			return nil, err
		}
		fields = encoded
	}
	return []any{
		r.Time,
		int64(r.StreamID),
		r.Seq,
		int16(r.Level),
		r.Message,
		nilIfEmpty(r.TraceID),
		nilIfEmpty(r.SpanID),
		fields,
	}, nil
}

func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func (w *Writer) observeQueueWait(sh *Shipment) {
	if w.metrics == nil || sh.enqueued.IsZero() {
		return
	}
	w.metrics.QueueWait.WithLabelValues(observability.QueueWriter).Observe(w.now().Sub(sh.enqueued).Seconds())
}

func (w *Writer) addDepth(delta int) {
	depth := w.depth.Add(int64(delta))
	if w.metrics != nil {
		w.metrics.QueueDepth.WithLabelValues(observability.QueueWriter).Set(float64(depth))
	}
}

func (w *Writer) drop(reason string, n int) {
	if n <= 0 || w.metrics == nil {
		return
	}
	w.metrics.RecordsDropped.WithLabelValues(observability.ComponentWriter, reason).Add(float64(n))
}

func (w *Writer) ack(sh *Shipment, err error) {
	if sh.Ack != nil {
		sh.Ack(err)
	}
}

// writeBatch accumulates records from several shipments plus the streams and acks
// they imply. Owned by exactly one worker, so none of it is synchronized.
type writeBatch struct {
	records []model.LogRecord
	// streams holds only the streams that actually need writing, keyed by ID so a
	// batch full of one stream produces one upsert — and so the upsert statement
	// never sees a duplicate key, which ON CONFLICT DO UPDATE would reject.
	streams map[model.StreamID]model.Stream
	acks    []func(error)
}

func newWriteBatch(capacity int) *writeBatch {
	return &writeBatch{
		records: make([]model.LogRecord, 0, capacity),
		streams: make(map[model.StreamID]model.Stream),
	}
}

func (b *writeBatch) add(sh *Shipment, cache *streamCache, now time.Time) {
	b.records = append(b.records, sh.Records...)
	if sh.Ack != nil {
		b.acks = append(b.acks, sh.Ack)
	}

	id := sh.Stream.ID
	if _, pending := b.streams[id]; pending {
		return
	}
	if !cache.needsUpsert(id, now) {
		return
	}
	stream := sh.Stream
	// Labels are cloned because the caller may reuse its map, and this batch
	// outlives Submit.
	stream.Labels = stream.Labels.Clone()
	if stream.FirstSeen.IsZero() {
		stream.FirstSeen = now
	}
	// last_seen means "most recently written to", so it is stamped at batch time
	// rather than taken from the caller, which may have been holding the shipment.
	stream.LastSeen = now
	b.streams[id] = stream
}

// streamSlice returns the streams to upsert, sorted by ID.
//
// The sort is not cosmetic, it is deadlock avoidance. ON CONFLICT DO UPDATE takes a
// row lock per conflicting stream, in the order the rows arrive. Two writers sharing
// some streams and iterating a Go map -- whose order is deliberately randomized --
// would take those locks in opposite orders and deadlock. Sorting gives every writer
// in the cluster one global lock order, which makes that impossible rather than
// merely unlikely.
func (b *writeBatch) streamSlice() []model.Stream {
	if len(b.streams) == 0 {
		return nil
	}
	out := make([]model.Stream, 0, len(b.streams))
	for _, s := range b.streams {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b model.Stream) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// streamsCommitted marks the batch's streams as durable, so a retry of the records
// transaction does not upsert them again.
func (b *writeBatch) streamsCommitted() { clear(b.streams) }

func (b *writeBatch) empty() bool             { return len(b.records) == 0 }
func (b *writeBatch) full(threshold int) bool { return len(b.records) >= threshold }

func (b *writeBatch) ackAll(err error) {
	for _, ack := range b.acks {
		ack(err)
	}
}

// reset keeps the record slice's capacity so a steady-state writer stops allocating
// after its first few batches.
func (b *writeBatch) reset() {
	b.records = b.records[:0]
	b.acks = b.acks[:0]
	clear(b.streams)
}

// isRetryable decides whether another attempt could plausibly succeed.
//
// Retrying is always *safe*: the transaction is rebuilt from records still held in
// memory and the insert is idempotent through ON CONFLICT DO NOTHING. So this is
// purely about not wasting attempts, and the default for an unrecognized error is
// to retry, because the errors this cannot classify are network and pool errors.
func isRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if len(pgErr.Code) < 2 {
			return false
		}
		switch pgErr.Code[:2] {
		case "08", // connection exception
			"40", // transaction rollback: serialization failure, deadlock
			"53", // insufficient resources: out of memory, too many connections
			"57", // operator intervention: admin shutdown, query canceled
			"58": // system error: I/O failure
			return true
		default:
			// 22 data exception, 23 integrity constraint, 42 syntax or access rule.
			// All of these mean the batch or the code is wrong, and will fail the
			// same way forever.
			return false
		}
	}
	return true
}

// backoff returns the delay before the given attempt, exponential with full jitter.
//
// Full jitter rather than a fixed multiple: when the database briefly refuses
// connections, every writer in the cluster fails at once, and without jitter they
// all retry at the same instant and refuse it again.
func backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base << min(attempt-1, 16)
	if delay <= 0 || delay > maxDelay {
		delay = maxDelay
	}
	return time.Duration(rand.Int64N(int64(delay)) + 1) //nolint:gosec // jitter, not a secret
}

// sleep waits for d, reporting false if ctx was canceled first.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
