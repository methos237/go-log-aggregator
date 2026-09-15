//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/storage"
)

// defaultThroughputRecords is the phase 1 exit criterion. Overridable so a laptop
// inner loop can run the same test at 50k.
const defaultThroughputRecords = 1_000_000

func throughputRecords(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("LOGAGG_TEST_RECORDS")
	if raw == "" {
		return defaultThroughputRecords
	}
	n, err := strconv.Atoi(raw)
	require.NoError(t, err, "LOGAGG_TEST_RECORDS must be an integer")
	require.Positive(t, n, "LOGAGG_TEST_RECORDS must be positive")
	return n
}

func writerConfig() config.Writer {
	return config.Writer{
		Workers:               4,
		BatchSize:             5000,
		FlushInterval:         100 * time.Millisecond,
		QueueDepth:            64,
		WriteTimeout:          2 * time.Minute,
		MaxAttempts:           3,
		RetryBaseDelay:        20 * time.Millisecond,
		RetryMaxDelay:         time.Second,
		StreamCacheSize:       4096,
		StreamRefreshInterval: time.Minute,
		CopyMode:              config.CopyModeDirect,
	}
}

// newWriter builds a started writer wired to a private registry, and closes it when
// the test ends.
func newWriter(t *testing.T, pool *pgxpool.Pool, cfg config.Writer) (*storage.Writer, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewRegistry()
	w, err := storage.NewWriter(pool, cfg, storage.NewMetrics(reg), testLogger(t))
	require.NoError(t, err)

	w.Start(context.Background())
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		require.NoError(t, w.Close(closeCtx))
	})
	return w, reg
}

// TestWriterInsertsOneMillionRecords is the phase 1 exit criterion: a million
// synthetic records through the real writer, then a row count.
func TestWriterInsertsOneMillionRecords(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	total := throughputRecords(t)
	w, reg := newWriter(t, pool, writerConfig())

	// Twenty concurrent streams rather than one: it exercises the stream cache, makes
	// batches span several streams (so a batch's upsert set has more than one entry),
	// and is closer to the shape of real ingest than a single-writer loop.
	const (
		streams         = 20
		recordsPerBatch = 500
	)

	perStream := total / streams
	require.Positive(t, perStream, "need at least one record per stream")

	base := time.Now().Add(-2 * time.Hour).Truncate(time.Second)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
		acked    int
		ackErrs  []error
	)

	start := time.Now()
	for s := 0; s < streams; s++ {
		wg.Add(1)
		go func(streamIndex int) {
			defer wg.Done()

			labels := model.LabelSet{
				Service: fmt.Sprintf("service-%02d", streamIndex%5),
				Host:    fmt.Sprintf("node-%02d", streamIndex),
				Env:     "prod",
				Extra:   map[string]string{"region": "eu-west-1", "shard": strconv.Itoa(streamIndex)},
			}
			stream := model.NewStream(labels, base)

			for offset := 0; offset < perStream; offset += recordsPerBatch {
				size := min(recordsPerBatch, perStream-offset)
				records := synthesize(stream.ID, base, offset, size)

				n, err := w.Submit(ctx, storage.Shipment{
					Stream:  stream,
					Records: records,
					Ack: func(err error) {
						mu.Lock()
						defer mu.Unlock()
						acked += size
						if err != nil {
							ackErrs = append(ackErrs, err)
						}
					},
				})
				require.NoError(t, err)

				mu.Lock()
				accepted += n
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()

	// Flush everything still batched before counting rows. Closing is the only way to
	// know the writer has no work left; the test's own Cleanup would run too late.
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	require.NoError(t, w.Close(closeCtx))
	elapsed := time.Since(start)

	require.Empty(t, ackErrs, "some batches failed to write")
	require.Equal(t, streams*perStream, accepted, "writer rejected records it should have taken")
	require.Equal(t, accepted, acked, "not every accepted record was acknowledged")

	require.Equal(t, int64(accepted), countLogs(ctx, t, pool), "row count does not match what was accepted")
	require.Equal(t, int64(streams), countStreams(ctx, t, pool), "one stream row per distinct label set")

	require.Equal(t, float64(accepted), counterValue(t, reg, "logagg_storage_rows_copied_total"))
	require.Equal(t, float64(accepted), counterValue(t, reg, "logagg_storage_rows_inserted_total"))
	require.Zero(t, counterValue(t, reg, "logagg_storage_rows_deduplicated_total"),
		"a first write should produce no duplicates")

	// Not an assertion, just a number in the log: phase 8 turns this into a published
	// benchmark with a stated hardware baseline.
	t.Logf("wrote %d records in %s (%.0f records/sec)",
		accepted, elapsed.Round(time.Millisecond), float64(accepted)/elapsed.Seconds())
}

// copyModes are the two write paths; every dedup guarantee has to hold on both,
// since direct mode is only safe because it falls back to staging on a replay.
var copyModes = []string{config.CopyModeStaging, config.CopyModeDirect}

// TestWriterDeduplicatesOnReplay is the at-least-once guarantee. A redelivered batch
// must leave the row count unchanged instead of erroring or duplicating.
func TestWriterDeduplicatesOnReplay(t *testing.T) {
	t.Parallel()
	for _, mode := range copyModes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testWriterDeduplicatesOnReplay(t, mode)
		})
	}
}

func testWriterDeduplicatesOnReplay(t *testing.T, mode string) {
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	cfg := writerConfig()
	cfg.BatchSize = 200
	cfg.CopyMode = mode
	w, reg := newWriter(t, pool, cfg)

	labels := model.LabelSet{Service: "api", Host: "node-1", Env: "prod"}
	stream := model.NewStream(labels, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	const size = 1000
	// The writer is a parameter, not a captured variable: the replay below uses a
	// second writer, and capturing the first one would silently submit to a closed
	// writer instead of replaying.
	submit := func(w *storage.Writer) {
		for offset := 0; offset < size; offset += 200 {
			n, err := w.Submit(ctx, storage.Shipment{
				Stream:  stream,
				Records: synthesize(stream.ID, base, offset, 200),
			})
			require.NoError(t, err)
			require.Equal(t, 200, n)
		}
	}

	submit(w)
	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))
	require.Equal(t, int64(size), countLogs(ctx, t, pool))
	require.Zero(t, counterValue(t, reg, "logagg_storage_direct_copy_fallbacks_total"),
		"first delivery must not trip the dedup index")

	// Same records, same timestamps and sequence numbers: exactly what JetStream
	// redelivery looks like after a crash between the write and the ack.
	replay, reg2 := newWriter(t, pool, cfg)
	submit(replay)
	require.NoError(t, replay.Close(mustDeadline(t, time.Minute)))

	require.Equal(t, int64(size), countLogs(ctx, t, pool), "replay duplicated rows")
	require.Equal(t, float64(size), counterValue(t, reg2, "logagg_storage_rows_copied_total"))
	require.Zero(t, counterValue(t, reg2, "logagg_storage_rows_inserted_total"),
		"a full replay should insert nothing")
	require.Equal(t, float64(size), counterValue(t, reg2, "logagg_storage_rows_deduplicated_total"),
		"the dedup counter should account for every replayed row")
	if mode == config.CopyModeDirect {
		require.Equal(t, float64(size/200), counterValue(t, reg2, "logagg_storage_direct_copy_fallbacks_total"),
			"every replayed batch should have fallen back to staging")
	}

	// And the first writer's counters should not have been touched by the replay.
	require.Equal(t, float64(size), counterValue(t, reg, "logagg_storage_rows_inserted_total"))

	// A replay that arrives after the chunk was compressed must still be a no-op:
	// the dedup index has to be enforced on COPY into a compressed chunk too.
	compressAllChunks(ctx, t, pool)
	late, reg3 := newWriter(t, pool, cfg)
	submit(late)
	require.NoError(t, late.Close(mustDeadline(t, time.Minute)))
	require.Equal(t, int64(size), countLogs(ctx, t, pool), "replay into a compressed chunk duplicated rows")
	require.Equal(t, float64(size), counterValue(t, reg3, "logagg_storage_rows_deduplicated_total"))
	if mode == config.CopyModeDirect {
		require.Equal(t, float64(size/200), counterValue(t, reg3, "logagg_storage_direct_copy_fallbacks_total"))
	}
}

// TestWriterHandlesPartialReplay covers the realistic redelivery case: an overlapping
// batch where some records are new.
func TestWriterHandlesPartialReplay(t *testing.T) {
	t.Parallel()
	for _, mode := range copyModes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testWriterHandlesPartialReplay(t, mode)
		})
	}
}

func testWriterHandlesPartialReplay(t *testing.T, mode string) {
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	cfg := writerConfig()
	cfg.CopyMode = mode
	w, _ := newWriter(t, pool, cfg)
	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	_, err := w.Submit(ctx, storage.Shipment{Stream: stream, Records: synthesize(stream.ID, base, 0, 100)})
	require.NoError(t, err)
	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))
	require.Equal(t, int64(100), countLogs(ctx, t, pool))

	// Records 50..149: half already stored, half new.
	w2, reg := newWriter(t, pool, cfg)
	_, err = w2.Submit(ctx, storage.Shipment{Stream: stream, Records: synthesize(stream.ID, base, 50, 100)})
	require.NoError(t, err)
	require.NoError(t, w2.Close(mustDeadline(t, time.Minute)))

	require.Equal(t, int64(150), countLogs(ctx, t, pool), "the new half should have landed")
	require.Equal(t, float64(50), counterValue(t, reg, "logagg_storage_rows_inserted_total"))
	require.Equal(t, float64(50), counterValue(t, reg, "logagg_storage_rows_deduplicated_total"))
}

// TestWriterUpsertsStreamsOncePerCache is the reason the LRU exists: a stream row
// must be written the first time it is seen and then left alone.
func TestWriterUpsertsStreamsOncePerCache(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	cfg := writerConfig()
	cfg.BatchSize = 10
	// Single worker so batch boundaries -- and therefore upsert counts -- are
	// deterministic.
	cfg.Workers = 1
	w, reg := newWriter(t, pool, cfg)

	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	for i := 0; i < 20; i++ {
		_, err := w.Submit(ctx, storage.Shipment{Stream: stream, Records: synthesize(stream.ID, base, i*10, 10)})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))

	require.Equal(t, int64(1), countStreams(ctx, t, pool))
	require.Equal(t, float64(1), counterValue(t, reg, "logagg_storage_stream_upserts_total"),
		"the stream should have been upserted exactly once")
	require.Positive(t, counterValue(t, reg, "logagg_storage_stream_cache_hits_total"),
		"later batches should have hit the cache")

	// first_seen must not drift forward on later batches; last_seen must.
	var firstSeen, lastSeen time.Time
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT first_seen, last_seen FROM streams WHERE stream_id = $1", int64(stream.ID)).
		Scan(&firstSeen, &lastSeen))
	require.False(t, lastSeen.Before(firstSeen), "last_seen went backwards")
}

// TestWriterRefreshesLastSeenAfterInterval exercises the other half of the cache
// policy: a long-lived stream's last_seen must not go stale forever.
func TestWriterRefreshesLastSeenAfterInterval(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	cfg := writerConfig()
	cfg.BatchSize = 10
	cfg.Workers = 1
	// Every batch is "stale", so every batch re-upserts.
	cfg.StreamRefreshInterval = time.Nanosecond
	w, reg := newWriter(t, pool, cfg)

	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	for i := 0; i < 5; i++ {
		_, err := w.Submit(ctx, storage.Shipment{Stream: stream, Records: synthesize(stream.ID, base, i*10, 10)})
		require.NoError(t, err)
		// One batch per flush, so each Submit produces its own upsert.
		time.Sleep(2 * cfg.FlushInterval)
	}
	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))

	require.Equal(t, int64(1), countStreams(ctx, t, pool), "re-upserting must not create duplicate rows")
	require.Greater(t, counterValue(t, reg, "logagg_storage_stream_upserts_total"), float64(1),
		"a stale stream should be re-upserted")
}

// TestWriterRejectsInvalidRecordsWithoutPoisoningTheBatch is the poison-message
// guard: one bad record must not stop the good ones in the same shipment.
func TestWriterRejectsInvalidRecordsWithoutPoisoningTheBatch(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	w, reg := newWriter(t, pool, writerConfig())
	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	records := synthesize(stream.ID, base, 0, 10)
	records[3].Time = time.Time{}     // no timestamp
	records[5].Level = 99             // unknown level
	records[7].TraceID = []byte{0x01} // wrong length
	records[9].Message = strings.Repeat("x", model.MaxMessageLen+1)

	accepted, err := w.Submit(ctx, storage.Shipment{Stream: stream, Records: records})
	require.NoError(t, err, "a shipment with some valid records should not be rejected outright")
	require.Equal(t, 6, accepted)
	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))

	require.Equal(t, int64(6), countLogs(ctx, t, pool))
	require.Equal(t, float64(4), counterValueWithLabel(t, reg, "logagg_records_dropped_total", "reason", "invalid_record"))
}

// TestWriterRejectsInvalidStream checks the other direction: without a valid label set
// there is no stream row to hang records off, so the whole shipment goes.
func TestWriterRejectsInvalidStream(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	w, reg := newWriter(t, pool, writerConfig())

	// No service: a stream row would violate NOT NULL.
	bad := model.NewStream(model.LabelSet{Host: "h", Env: "prod"}, time.Now())
	accepted, err := w.Submit(ctx, storage.Shipment{
		Stream:  bad,
		Records: synthesize(bad.ID, time.Now().Add(-time.Hour), 0, 10),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, model.ErrMissingLabel)
	require.Zero(t, accepted)

	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))
	require.Zero(t, countLogs(ctx, t, pool))
	require.Equal(t, float64(10), counterValueWithLabel(t, reg, "logagg_records_dropped_total", "reason", "invalid_record"))
}

// TestWriterSubmitAfterCloseFails documents that a shutting-down writer refuses work
// rather than accepting records it will never write. Nothing was acked, so the queue
// redelivers -- which is the entire reason acks come after the write.
func TestWriterSubmitAfterCloseFails(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	w, _ := newWriter(t, pool, writerConfig())
	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))

	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, time.Now())
	accepted, err := w.Submit(ctx, storage.Shipment{
		Stream:  stream,
		Records: synthesize(stream.ID, time.Now().Add(-time.Hour), 0, 5),
	})
	require.ErrorIs(t, err, storage.ErrWriterClosed)
	require.Zero(t, accepted)
	require.Zero(t, countLogs(ctx, t, pool))
}

// TestWriterCloseDrainsQueuedRecords is the graceful-shutdown guarantee: whatever was
// accepted before Close must be written and acked, not dropped.
func TestWriterCloseDrainsQueuedRecords(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	cfg := writerConfig()
	// A flush interval longer than the test guarantees the records are still sitting
	// in an open batch when Close is called, so this really does test the drain path.
	cfg.FlushInterval = time.Hour
	cfg.BatchSize = 100_000
	w, _ := newWriter(t, pool, cfg)

	stream := model.NewStream(model.LabelSet{Service: "api", Host: "h", Env: "prod"}, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	var (
		mu      sync.Mutex
		ackErrs []error
		acks    int
	)
	const shipments = 10
	for i := 0; i < shipments; i++ {
		n, err := w.Submit(ctx, storage.Shipment{
			Stream:  stream,
			Records: synthesize(stream.ID, base, i*50, 50),
			Ack: func(err error) {
				mu.Lock()
				defer mu.Unlock()
				acks++
				if err != nil {
					ackErrs = append(ackErrs, err)
				}
			},
		})
		require.NoError(t, err)
		require.Equal(t, 50, n)
	}

	require.NoError(t, w.Close(mustDeadline(t, time.Minute)))

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, ackErrs)
	require.Equal(t, shipments, acks, "every shipment should have been acknowledged")
	require.Equal(t, int64(shipments*50), countLogs(ctx, t, pool))
}

// TestWriterCompressionAndAggregatesRun closes the last part of the exit criterion:
// the compression policy and the continuous aggregates do real work on real rows,
// not just exist in a catalog.
func TestWriterCompressionAndAggregatesRun(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	w, _ := newWriter(t, pool, writerConfig())

	// Old enough for the 2 hour compression policy to consider the chunks eligible,
	// and spread across several 1 hour chunks and several 1 minute buckets.
	base := time.Now().Add(-12 * time.Hour).Truncate(time.Minute)
	stream := model.NewStream(model.LabelSet{Service: "api", Host: "node-1", Env: "prod"}, base)

	const total = 20_000
	for offset := 0; offset < total; offset += 1000 {
		_, err := w.Submit(ctx, storage.Shipment{
			Stream:  stream,
			Records: synthesizeSpread(stream.ID, base, offset, 1000),
		})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close(mustDeadline(t, 2*time.Minute)))
	require.Equal(t, int64(total), countLogs(ctx, t, pool))

	var chunks int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'logs'`).Scan(&chunks))
	require.Greater(t, chunks, 1, "records should span several chunks")

	compressAllChunks(ctx, t, pool)

	var compressed int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.chunks
		WHERE hypertable_name = 'logs' AND is_compressed`).Scan(&compressed))
	require.Positive(t, compressed, "the compression policy did not compress anything")

	// Compression must be transparent to readers.
	require.Equal(t, int64(total), countLogs(ctx, t, pool), "rows disappeared after compression")

	var before, after int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT coalesce(sum(before_compression_total_bytes), 0),
		       coalesce(sum(after_compression_total_bytes), 0)
		FROM chunk_compression_stats('logs')`).Scan(&before, &after))
	require.Positive(t, before)
	require.Positive(t, after)
	require.Less(t, after, before, "compression made the data larger")
	t.Logf("compression ratio %.1fx (%d -> %d bytes)", float64(before)/float64(after), before, after)

	// Continuous aggregates: refresh, then check the counts agree with the raw table.
	end := time.Now()
	refreshAggregate(ctx, t, pool, "logs_rate_1m", base.Add(-time.Hour), end)
	refreshAggregate(ctx, t, pool, "logs_rate_1h", base.Add(-time.Hour), end)

	var aggregated int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT coalesce(sum(n), 0) FROM logs_rate_1m").Scan(&aggregated))
	require.Equal(t, int64(total), aggregated, "logs_rate_1m does not account for every row")

	require.NoError(t, pool.QueryRow(ctx, "SELECT coalesce(sum(n), 0) FROM logs_rate_1h").Scan(&aggregated))
	require.Equal(t, int64(total), aggregated, "logs_rate_1h does not account for every row")

	var buckets int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM logs_rate_1m").Scan(&buckets))
	require.Greater(t, buckets, 1, "records should span several 1 minute buckets")
}

// TestConcurrentWritersDoNotDeadlockOnStreams is the multi-node case in miniature:
// several writers upserting the same stream row concurrently. With a per-row lock and
// a poorly ordered transaction this deadlocks; the test is here so a regression shows
// up as a failure rather than as a stuck production node.
func TestConcurrentWritersDoNotDeadlockOnStreams(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	labels := model.LabelSet{Service: "api", Host: "shared", Env: "prod"}
	stream := model.NewStream(labels, time.Now())
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)

	const writers = 4
	const perWriter = 2000

	// Detached from the test's context so canceling it cannot abort a flush midway,
	// which is what the writer's own Start does with the process context.
	runCtx := context.WithoutCancel(ctx)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			cfg := writerConfig()
			cfg.Workers = 2
			cfg.BatchSize = 250
			w, err := storage.NewWriter(pool, cfg, storage.NewMetrics(nil), testLogger(t))
			require.NoError(t, err)
			w.Start(runCtx)

			for offset := 0; offset < perWriter; offset += 250 {
				// Disjoint sequence ranges per writer, so every record is distinct and
				// the row count is exact.
				seqBase := index*perWriter + offset
				_, serr := w.Submit(ctx, storage.Shipment{
					Stream:  stream,
					Records: synthesize(stream.ID, base, seqBase, 250),
				})
				require.NoError(t, serr)
			}

			closeCtx, cancel := context.WithTimeout(runCtx, 2*time.Minute)
			defer cancel()
			require.NoError(t, w.Close(closeCtx))
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(writers*perWriter), countLogs(ctx, t, pool))
	require.Equal(t, int64(1), countStreams(ctx, t, pool))
}

// TestConcurrentWritersSpanningChunksDoNotDeadlock is the regression test for the
// deadlock that made the stream upsert its own transaction (see commitStreams).
//
// It maximizes the interleaving that caused it: several writers, one shared stream, a
// refresh interval short enough that every batch upserts that stream, and records
// spread over hours so nearly every batch also creates hypertable chunks. Sharing one
// transaction between the upsert and the insert makes PostgreSQL report SQLSTATE 40P01
// here, and retries do not save it -- a batch is lost, which shows up as a row count
// short by a multiple of the batch size.
func TestConcurrentWritersSpanningChunksDoNotDeadlock(t *testing.T) {
	t.Parallel()

	pool, _ := migratedDB(t)
	ctx := testContext(t)

	stream := model.NewStream(model.LabelSet{Service: "api", Host: "shared", Env: "prod"}, time.Now())
	// Far enough back that every record stays inside the backfill window while still
	// spanning many 1 hour chunks.
	base := time.Now().Add(-72 * time.Hour).Truncate(time.Hour)

	const (
		writers   = 6
		perWriter = 6000
		batchSize = 1000
	)

	runCtx := context.WithoutCancel(ctx)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ackErrs []error
	)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			cfg := writerConfig()
			cfg.Workers = 2
			cfg.BatchSize = batchSize
			// Every batch sees the stream as stale, so every batch upserts it. That is
			// what puts the exclusive row lock and the chunk lock in the same window.
			cfg.StreamRefreshInterval = time.Nanosecond
			w, err := storage.NewWriter(pool, cfg, storage.NewMetrics(nil), testLogger(t))
			require.NoError(t, err)
			w.Start(runCtx)

			for offset := 0; offset < perWriter; offset += batchSize {
				seqBase := index*perWriter + offset
				_, serr := w.Submit(ctx, storage.Shipment{
					Stream: stream,
					// One record per minute, so a 1000-record batch covers ~17 chunks.
					Records: synthesizeMinutely(stream.ID, base, seqBase, batchSize),
					Ack: func(err error) {
						if err == nil {
							return
						}
						mu.Lock()
						defer mu.Unlock()
						ackErrs = append(ackErrs, err)
					},
				})
				require.NoError(t, serr)
			}

			closeCtx, cancel := context.WithTimeout(runCtx, 2*time.Minute)
			defer cancel()
			require.NoError(t, w.Close(closeCtx))
		}(i)
	}
	wg.Wait()

	mu.Lock()
	errs := ackErrs
	mu.Unlock()
	require.Empty(t, errs, "batches were lost to write failures")

	require.Equal(t, int64(writers*perWriter), countLogs(ctx, t, pool))
	require.Equal(t, int64(1), countStreams(ctx, t, pool))
}

// synthesize builds size records with sequence numbers and timestamps derived from
// offset, so the same (offset, size) always produces byte-identical records. That is
// what makes the replay tests meaningful.
func synthesize(id model.StreamID, base time.Time, offset, size int) []model.LogRecord {
	levels := []model.Level{model.LevelDebug, model.LevelInfo, model.LevelWarn, model.LevelError}
	records := make([]model.LogRecord, size)
	for i := range records {
		seq := offset + i
		records[i] = model.LogRecord{
			StreamID: id,
			Time:     base.Add(time.Duration(seq) * time.Millisecond),
			Seq:      int64(seq),
			Level:    levels[seq%len(levels)],
			Message:  fmt.Sprintf("synthetic record %d for stream %d", seq, id),
		}
		// Roughly a quarter of records carry structured extras and trace context, which
		// is what makes the JSONB and BYTEA columns part of the measurement rather
		// than always null.
		if seq%4 == 0 {
			records[i].Fields = map[string]string{"seq": strconv.Itoa(seq), "kind": "synthetic"}
			records[i].TraceID = make([]byte, model.TraceIDLen)
			records[i].SpanID = make([]byte, model.SpanIDLen)
			records[i].TraceID[0] = byte(seq)
			records[i].SpanID[0] = byte(seq)
		}
	}
	return records
}

// chunkSpanMinutes is how far synthesizeMinutely spreads records before wrapping:
// 48 hours, so records land in about 48 one-hour chunks.
const chunkSpanMinutes = 48 * 60

// synthesizeMinutely spreads records one minute apart so a batch spans many chunks and
// forces chunk creation to contend between writers.
//
// Timestamps wrap after chunkSpanMinutes to keep every record inside the model's
// backfill window; seq keeps increasing, so the (stream_id, seq, time) dedup key stays
// unique across the wrap and the row count is still exact.
func synthesizeMinutely(id model.StreamID, base time.Time, offset, size int) []model.LogRecord {
	records := synthesize(id, base, offset, size)
	for i := range records {
		records[i].Time = base.Add(time.Duration(records[i].Seq%chunkSpanMinutes) * time.Minute)
	}
	return records
}

// synthesizeSpread is synthesize with a one-second step instead of a millisecond one,
// so records land in many chunks and many aggregate buckets.
func synthesizeSpread(id model.StreamID, base time.Time, offset, size int) []model.LogRecord {
	records := synthesize(id, base, offset, size)
	for i := range records {
		records[i].Time = base.Add(time.Duration(records[i].Seq) * time.Second)
	}
	return records
}

func mustDeadline(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// counterValue reads a single unlabeled counter out of a registry.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	return metricValue(t, reg, name, "", "")
}

func counterValueWithLabel(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	return metricValue(t, reg, name, label, value)
}

func metricValue(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if label != "" && !hasLabel(m, label, value) {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatalf("metric %s{%s=%q} not found", name, label, value)
	return 0
}

func hasLabel(m *dto.Metric, name, value string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}
