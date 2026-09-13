//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/storage"
)

// collector is the same set of components cmd/collector wires, assembled in process.
//
// In process rather than as a subprocess: the point of these tests is the boundary
// between the pieces — ack ordering, redelivery, dedup — and a subprocess would hide
// exactly that behind a log file. What it costs is the binary's flag parsing, which
// the compose smoke test in CI covers instead.
type collector struct {
	queue    *queue.Conn
	writer   *storage.Writer
	consumer *ingest.Consumer
	server   *ingest.Server
	pool     *pgxpool.Pool
	addr     string
	// opt is the configuration actually used, defaults applied.
	opt collectorOptions

	cancel context.CancelFunc
}

// collectorOptions are the knobs these tests need to vary.
type collectorOptions struct {
	dsn           string
	stream        string
	durable       string
	subjectPrefix string
	// tailPrefix is the fan-out subject prefix. Derived from subjectPrefix so
	// parallel tests on the shared broker never hear each other's tails.
	tailPrefix string
	// ackWait is short in tests so a kill-and-recover case does not wait out the
	// production five-minute default.
	ackWait time.Duration
	// bufferSize and publishWorkers shrink the ingest pipeline when a test wants to
	// provoke shedding.
	bufferSize     int
	publishWorkers int
	// writerBatchSize and writerFlush keep the writer flushing promptly, since these
	// tests assert on rows rather than on throughput.
	writerBatchSize int
	writerFlush     time.Duration
}

func (o *collectorOptions) withDefaults() {
	if o.tailPrefix == "" {
		o.tailPrefix = o.subjectPrefix + "_tail"
	}
	if o.ackWait <= 0 {
		o.ackWait = 3 * time.Second
	}
	if o.bufferSize <= 0 {
		o.bufferSize = 256
	}
	if o.publishWorkers <= 0 {
		o.publishWorkers = 4
	}
	if o.writerBatchSize <= 0 {
		o.writerBatchSize = 500
	}
	if o.writerFlush <= 0 {
		o.writerFlush = 100 * time.Millisecond
	}
}

// startCollector assembles and starts a collector. It is not registered for cleanup:
// each test decides whether to stop it gracefully or kill it.
func startCollector(t *testing.T, opt collectorOptions, reg prometheus.Registerer) *collector {
	t.Helper()
	opt.withDefaults()

	ctx, cancel := context.WithCancel(context.Background())

	pool, err := storage.Open(ctx, testDBConfig(opt.dsn), "logagg-integration-collector")
	require.NoError(t, err, "open pool")

	writer, err := storage.NewWriter(pool, collectorWriterConfig(&opt), storage.NewMetrics(reg), testLogger(t))
	require.NoError(t, err, "create writer")
	writer.Start(ctx)

	q, err := queue.Connect(ctx, queueConfig(&opt), queue.NewMetrics(reg), testLogger(t))
	require.NoError(t, err, "connect queue")

	ingestMetrics := ingest.NewMetrics(reg)
	consumer, err := ingest.NewConsumer(q, writer, ingestMetrics, testLogger(t))
	require.NoError(t, err, "create consumer")
	require.NoError(t, consumer.Start(ctx), "start consumer")

	srv, err := ingest.New(ctx, ingestConfig(&opt), q, ingestMetrics, testLogger(t))
	require.NoError(t, err, "create ingest server")

	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	c := &collector{
		queue:    q,
		writer:   writer,
		consumer: consumer,
		server:   srv,
		pool:     pool,
		addr:     srv.Addr(),
		opt:      opt,
		cancel:   cancel,
	}

	// The pool outlives the components so a graceful stop can still flush; closed
	// here, after whichever stop the test chose.
	t.Cleanup(func() {
		<-served
		pool.Close()
		cancel()
	})
	return c
}

// stop shuts the collector down in the same order cmd/collector does.
func (c *collector) stop(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, c.server.Shutdown(ctx), "ingest shutdown")
	require.NoError(t, c.consumer.Stop(ctx), "consumer stop")
	require.NoError(t, c.writer.Close(ctx), "writer close")
	require.NoError(t, c.queue.Close(ctx), "queue close")
}

// kill drops the collector the way a SIGKILL would: nothing drains, nothing flushes,
// no ack reaches the broker.
//
// Getting this faithful takes care, because every component here is built to drain
// gracefully. Calling writer.Close would make it flush — the whole point of that
// code — so the pool is closed underneath it instead, which is what a dead process
// looks like from the database's side: in-flight writes fail and the records the
// writer was holding never land. The queue connection goes first for the same
// reason: an ack that cannot be sent leaves its message unacknowledged, so the
// broker redelivers it, which is the property under test.
func (c *collector) kill(t *testing.T) {
	t.Helper()

	dead, cancel := context.WithCancel(context.Background())
	cancel()

	// Errors are expected and ignored: a crash reports nothing.
	_ = c.queue.Close(dead)
	c.pool.Close()
	_ = c.server.Shutdown(dead)
	c.consumer.Stop(dead) //nolint:errcheck // a crash reports nothing
	c.cancel()
}

func queueConfig(opt *collectorOptions) config.Queue {
	return config.Queue{
		URL:               natsURL,
		StreamName:        opt.stream,
		SubjectPrefix:     opt.subjectPrefix,
		Durable:           opt.durable,
		ConnectTimeout:    15 * time.Second,
		PublishTimeout:    10 * time.Second,
		MaxAckPending:     1024,
		AckWait:           opt.ackWait,
		StreamMaxBytes:    256 << 20,
		StreamMaxAge:      time.Hour,
		TailSubjectPrefix: opt.tailPrefix,
	}
}

func ingestConfig(opt *collectorOptions) config.Ingest {
	return config.Ingest{
		// Port 0: the OS picks, so parallel tests and a running dev stack cannot
		// collide.
		Addr:            "127.0.0.1:0",
		MaxRecvMsgBytes: 8 << 20,
		BufferSize:      opt.bufferSize,
		PublishWorkers:  opt.publishWorkers,
	}
}

func collectorWriterConfig(opt *collectorOptions) config.Writer {
	return config.Writer{
		Workers:               4,
		BatchSize:             opt.writerBatchSize,
		FlushInterval:         opt.writerFlush,
		QueueDepth:            256,
		WriteTimeout:          20 * time.Second,
		MaxAttempts:           3,
		RetryBaseDelay:        20 * time.Millisecond,
		RetryMaxDelay:         time.Second,
		StreamCacheSize:       1024,
		StreamRefreshInterval: time.Minute,
	}
}

// migratedDBWithStream is the setup every ingest test shares: a fresh database plus
// names nothing else is using.
func migratedDBWithStream(t *testing.T) (*pgxpool.Pool, collectorOptions) {
	t.Helper()

	pool, dsn := migratedDB(t)
	stream, durable, prefix := uniqueStream(t)
	return pool, collectorOptions{dsn: dsn, stream: stream, durable: durable, subjectPrefix: prefix}
}

// countRows returns how many log rows exist for a service.
func countRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, service string) int {
	t.Helper()

	var n int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM logs l JOIN streams s USING (stream_id) WHERE s.service = $1`,
		service).Scan(&n)
	require.NoError(t, err, "count rows for %s", service)
	return n
}

// seqsPresent reports which of the given sequence numbers exist for a service.
func seqsPresent(ctx context.Context, t *testing.T, pool *pgxpool.Pool, service string) map[int64]bool {
	t.Helper()

	rows, err := pool.Query(ctx,
		`SELECT l.seq FROM logs l JOIN streams s USING (stream_id) WHERE s.service = $1`,
		service)
	require.NoError(t, err, "read seqs for %s", service)
	defer rows.Close()

	present := make(map[int64]bool)
	for rows.Next() {
		var seq int64
		require.NoError(t, rows.Scan(&seq))
		present[seq] = true
	}
	require.NoError(t, rows.Err())
	return present
}

// defaultConfig loads the shipped defaults, with the environment cleared of the
// overrides a developer's shell or the test harness may have set.
func defaultConfig() (*config.Config, error) {
	return config.Load()
}
