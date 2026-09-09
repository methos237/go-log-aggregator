//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/agent"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// Tuning shared by every test in this file. Kept in one place so a change to
// one -- say, because CI runs slower than a laptop -- does not have to be
// hunted down in four different tests.
const (
	// testPollInterval is how often the tailed file is checked for new data.
	// Short so a rotation or a truncation is noticed quickly relative to the
	// test's own deadline, without the poll loop meaningfully competing with
	// this test's own writes for disk I/O.
	testPollInterval = 20 * time.Millisecond
	// testMaxBatchDelay bounds how long a partially filled batch waits before
	// shipping anyway, short so "wait for delivery" polls resolve quickly
	// instead of the test's own deadline doing the waiting for it.
	testMaxBatchDelay = 50 * time.Millisecond
	// testMaxBatchRecords and testAckWindow are deliberately small: see
	// restartMaxDuplicates below, which is derived directly from them.
	testMaxBatchRecords = 25
	testAckWindow       = 4
	// testMinBackoff and testMaxBackoff bound the shipper's reconnect backoff.
	// Short so the spool-outage test's recovery time is dominated by the test
	// bringing the collector back, not by exponential backoff.
	testMinBackoff = 20 * time.Millisecond
	testMaxBackoff = 200 * time.Millisecond

	testSpoolMaxBytes     = 8 << 20
	testSpoolSegmentBytes = 1 << 20

	// restartMaxDuplicates bounds duplicate rows the restart-resume test may
	// see. It is the only one of the four tests where a genuine duplicate ROW
	// (as opposed to a redelivery collapsed by storage's own dedup index) can
	// occur: a batch the collector already accepted and wrote, but whose ack
	// did not reach the shipper before an ungraceful stop, is both (a)
	// already durably written and (b) never checkpointed -- so it is both
	// replayed from the leftover spool segment (carrying its original
	// timestamp) and re-read fresh from the file by the restarted TailSource
	// (carrying a new one) once the process comes back. Because the two
	// copies carry different timestamps, storage's (stream_id, seq, time)
	// dedup index (migrations/0001, index logs_dedup) does not collapse them
	// -- unlike the other three tests below, where nothing ever produces a
	// second, differently-timed copy of the same line. How many records can
	// be caught in that exposed window at once is bounded by how many
	// batches can be outstanding at all (testAckWindow), plus the one
	// accumulator shutdown flushes fresh on the way out, each capped at
	// testMaxBatchRecords.
	restartMaxDuplicates = testMaxBatchRecords * (testAckWindow + 1)
)

// numberedLine formats the payload every test in this file writes. Its number
// is monotonic across the WHOLE test, including across a rotation or a
// truncation -- unlike Seq, a file byte offset that legitimately restarts
// near zero in both cases (see TailSource's package doc) and so cannot be
// used to check for gaps. See assertNoGapsBoundedDuplicates.
func numberedLine(n int) string {
	return fmt.Sprintf("line %06d", n)
}

var lineNumberPattern = regexp.MustCompile(`^line (\d{6})$`)

// parseLineNumber recovers the number numberedLine encoded, reporting false
// for anything else so a caller can tell a genuinely unparseable row apart
// from a normal miss.
func parseLineNumber(msg string) (int, bool) {
	m := lineNumberPattern.FindStringSubmatch(msg)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}

// appendLines opens path for append -- creating it if it does not exist yet
// -- and writes numberedLine(n) for every n in [from, to], one per line.
// Opening and closing around each call, rather than holding one descriptor
// for the whole test, mirrors how an external rotator (logrotate's
// copytruncate, or a sidecar that re-execs) touches a log file: it never
// assumes it is the process that has it open.
func appendLines(t *testing.T, path string, from, to int) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err, "open %s for append", path)
	for n := from; n <= to; n++ {
		_, err := fmt.Fprintln(f, numberedLine(n))
		require.NoError(t, err, "write line %d to %s", n, path)
	}
	require.NoError(t, f.Close(), "close %s", path)
}

// lineNumberCounts returns, for every row recorded under service, how many
// times each numbered payload's line number appears in the message column.
// Every query in this file is scoped to one service so that nothing else in
// the test's own (freshly migrated, single-test) database can affect the
// result.
func lineNumberCounts(ctx context.Context, t *testing.T, pool *pgxpool.Pool, service string) map[int]int {
	t.Helper()

	rows, err := pool.Query(ctx,
		`SELECT l.message FROM logs l JOIN streams s USING (stream_id) WHERE s.service = $1`, service)
	require.NoError(t, err, "query messages for %s", service)
	defer rows.Close()

	counts := make(map[int]int)
	for rows.Next() {
		var msg string
		require.NoError(t, rows.Scan(&msg))
		n, ok := parseLineNumber(msg)
		require.True(t, ok, "message %q did not carry a parseable numbered payload", msg)
		counts[n]++
	}
	require.NoError(t, rows.Err())
	return counts
}

// assertNoGapsBoundedDuplicates asserts that every number in [1, n] appears
// at least once among service's rows -- no gaps -- and that the total excess
// of rows over distinct numbers in that range -- rows minus distinct, i.e.
// duplicate deliveries -- is at most maxDuplicates. On a gap it reports the
// specific missing numbers, which is what actually points at a rotation or
// truncation boundary; a bare count is not actionable.
func assertNoGapsBoundedDuplicates(ctx context.Context, t *testing.T, pool *pgxpool.Pool, service string, n, maxDuplicates int) {
	t.Helper()

	counts := lineNumberCounts(ctx, t, pool, service)

	var missing []int
	total := 0
	for i := 1; i <= n; i++ {
		c := counts[i]
		total += c
		if c == 0 {
			missing = append(missing, i)
		}
	}
	require.Empty(t, missing, "missing line numbers (gap): %v", missing)

	duplicates := total - n
	require.LessOrEqual(t, duplicates, maxDuplicates,
		"delivered %d rows for the %d distinct numbers 1..%d: %d duplicates exceeds the bound of %d",
		total, n, n, duplicates, maxDuplicates)
}

// pipelineConfig configures one in-process agent pipeline: a single
// TailSource feeding a pass-through Joiner feeding a Shipper, which is the
// shape cmd/agent assembles per configured file (see buildSources, buildJoiner
// and runPipeline in cmd/agent/main.go). Built directly from internal/agent's
// public API rather than by shelling out to the agent binary, mirroring how
// startCollector in collector_test.go assembles a collector directly instead
// of running cmd/collector.
type pipelineConfig struct {
	path           string
	checkpointPath string
	spoolDir       string
	ingestAddr     string
	service        string
}

// pipeline is one running instance of the wiring pipelineConfig describes.
//
// Unlike cmd/agent's runPipeline, stopping this needs no separate drainCtx to
// bound a graceful drain on top of an immediate ctx-cancel exit: Shipper.Run's
// own runLoop always falls through to its unconditional shutdown() call --
// flush accumulators, wait shutdownAckWait for outstanding acks, spool
// whatever is not acked in time, commit the checkpoint -- whichever of its
// two select cases (in closing, or ctx.Done firing) is what broke the loop.
// So one shared ctx is enough here: canceling it stops every stage and still
// leaves the checkpoint and spool in a state a fresh pipeline over the same
// paths can correctly resume from, which is exactly the property the
// restart-resume and spool-outage tests below depend on.
type pipeline struct {
	spool  *agent.Spool
	source *agent.TailSource

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// startPipeline builds and starts a pipeline. Every tuning knob (poll
// interval, batch delay, batch and ack-window sizing, backoff bounds) comes
// from this file's shared test constants rather than from cfg, so every test
// here exercises the same, deliberately small configuration -- see
// restartMaxDuplicates for why that size matters to the assertions, not just
// to how fast the tests run.
func startPipeline(t *testing.T, cfg *pipelineConfig) *pipeline {
	t.Helper()

	checkpoint := agent.NewCheckpointStore(cfg.checkpointPath)
	require.NoError(t, checkpoint.Load(), "load checkpoint %s", cfg.checkpointPath)

	metrics := agent.NewMetrics(nil)

	spool, err := agent.NewSpool(agent.SpoolConfig{
		Dir:          cfg.spoolDir,
		MaxBytes:     testSpoolMaxBytes,
		SegmentBytes: testSpoolSegmentBytes,
		Metrics:      metrics,
	})
	require.NoError(t, err, "open spool %s", cfg.spoolDir)

	resume, _ := checkpoint.Get(cfg.path)
	source, err := agent.NewTailSource(&agent.TailConfig{
		Path:         cfg.path,
		PollInterval: testPollInterval,
		Resume:       resume,
		Metrics:      metrics,
	})
	require.NoError(t, err, "new tail source for %s", cfg.path)

	joiner, err := agent.NewJoiner(agent.MultilineConfig{Metrics: metrics})
	require.NoError(t, err, "new joiner")

	labels := model.LabelSet{Service: cfg.service, Host: "agent-test-host", Env: "test"}
	shipper, err := agent.NewShipper(&agent.ShipperConfig{
		Ingest:     ingest.ClientConfig{Addr: cfg.ingestAddr},
		Spool:      spool,
		Checkpoint: checkpoint,
		Labels: func(source string) (model.LabelSet, bool) {
			if source != cfg.path {
				return model.LabelSet{}, false
			}
			return labels, true
		},
		DefaultLevel:    model.LevelUnspecified,
		MaxBatchRecords: testMaxBatchRecords,
		MaxBatchDelay:   testMaxBatchDelay,
		AckWindow:       testAckWindow,
		MinBackoff:      testMinBackoff,
		MaxBackoff:      testMaxBackoff,
		Metrics:         metrics,
	})
	require.NoError(t, err, "new shipper")

	ctx, cancel := context.WithCancel(context.Background())
	lines := make(chan agent.Line, 64)
	joined := make(chan agent.Line, 64)

	p := &pipeline{spool: spool, source: source, cancel: cancel}
	p.wg.Add(3)
	go func() {
		defer p.wg.Done()
		if runErr := source.Run(ctx, lines); runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Logf("tail source %s: %v", cfg.path, runErr)
		}
	}()
	go func() {
		defer p.wg.Done()
		if runErr := joiner.Run(ctx, lines, joined); runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Logf("joiner: %v", runErr)
		}
	}()
	go func() {
		defer p.wg.Done()
		if runErr := shipper.Run(ctx, joined); runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Logf("shipper: %v", runErr)
		}
	}()

	return p
}

// stop cancels the pipeline's ctx and waits, bounded by timeout, for every
// stage to return -- which for the shipper includes flushing accumulators,
// waiting shutdownAckWait for outstanding acks, spooling anything not acked
// in time, and committing the checkpoint (see the pipeline doc comment). Only
// once that has happened is it safe to close the tail source's descriptor and
// persist the spool's own final read cursor.
func (p *pipeline) stop(t *testing.T, timeout time.Duration) {
	t.Helper()

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		require.FailNowf(t, "timed out", "pipeline stages did not stop within %s", timeout)
	}

	require.NoError(t, p.source.Close(), "close tail source")
	require.NoError(t, p.spool.Close(), "close spool")
}

// outageGate is a TCP relay the agent's shipper dials at a fixed address, so
// the spool-outage test can simulate a collector that vanishes and comes
// back without needing to control the real collector's own OS-assigned
// listening port (startCollector always binds "127.0.0.1:0"; see
// ingestConfig in collector_test.go). The gate's own listening socket is
// opened exactly once, in newOutageGate, and never closed or re-bound for the
// life of the test -- that is what "the same address" means from the
// shipper's point of view here. Only whether it forwards to a live backend,
// or refuses outright, changes.
type outageGate struct {
	ln   net.Listener
	addr string

	mu      sync.Mutex
	up      bool
	backend string
	conns   map[net.Conn]struct{}
}

// newOutageGate opens the gate's fixed listening address and starts accepting.
// It begins down: nothing forwards until setUp names a backend, which
// matches every caller's need to control exactly when the agent can first
// reach a collector.
func newOutageGate(t *testing.T) *outageGate {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "open outage gate listener")

	g := &outageGate{ln: ln, addr: ln.Addr().String(), conns: make(map[net.Conn]struct{})}
	go g.acceptLoop()
	t.Cleanup(func() { _ = g.ln.Close() })
	return g
}

func (g *outageGate) acceptLoop() {
	for {
		conn, err := g.ln.Accept()
		if err != nil {
			return // g.ln was closed by the test's Cleanup.
		}
		go g.handle(conn)
	}
}

// handle relays one accepted connection to whatever backend is current at
// the moment it arrives, or refuses it outright while the gate is down --
// accept-then-close, which a gRPC client sees as a connection reset rather
// than one it can keep waiting on, so the shipper notices the outage
// promptly instead of only via a keepalive timeout.
func (g *outageGate) handle(conn net.Conn) {
	g.mu.Lock()
	up, backend := g.up, g.backend
	if up {
		g.conns[conn] = struct{}{}
	}
	g.mu.Unlock()

	if !up {
		_ = conn.Close()
		return
	}

	upstream, err := net.Dial("tcp", backend)
	if err != nil {
		g.mu.Lock()
		delete(g.conns, conn)
		g.mu.Unlock()
		_ = conn.Close()
		return
	}

	defer func() {
		g.mu.Lock()
		delete(g.conns, conn)
		g.mu.Unlock()
		_ = conn.Close()
		_ = upstream.Close()
	}()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, conn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, upstream); done <- struct{}{} }()
	<-done
}

// setUp starts forwarding new and future connections to backend.
func (g *outageGate) setUp(backend string) {
	g.mu.Lock()
	g.up = true
	g.backend = backend
	g.mu.Unlock()
}

// setDown stops forwarding and forcibly closes every connection currently
// relayed, simulating a collector that has gone away outright rather than
// one merely refusing new connections -- otherwise the shipper's existing
// HTTP/2 connection could sit open for a while before either side noticed.
func (g *outageGate) setDown() {
	g.mu.Lock()
	g.up = false
	conns := make([]net.Conn, 0, len(g.conns))
	for c := range g.conns {
		conns = append(conns, c)
	}
	g.conns = make(map[net.Conn]struct{})
	g.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

// TestAgentSurvivesFileRotation is the phase's headline claim: a file tailed
// across a rename-and-recreate rotation, with writes continuing on both
// sides of it, delivers every line with no gap.
func TestAgentSurvivesFileRotation(t *testing.T) {
	t.Parallel()

	const service = "agent-rotation"
	pool, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	const (
		beforeRotation = 150
		afterRotation  = 150
		total          = beforeRotation + afterRotation
	)

	appendLines(t, path, 1, beforeRotation)

	p := startPipeline(t, &pipelineConfig{
		path:           path,
		checkpointPath: filepath.Join(dir, "checkpoint.json"),
		spoolDir:       filepath.Join(dir, "spool"),
		ingestAddr:     c.addr,
		service:        service,
	})
	defer p.stop(t, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait for the pre-rotation lines to land before rotating. Without this,
	// startPipeline's TailSource goroutine has not necessarily even opened
	// path yet -- Run() has not been scheduled, awaitFile has not raced ahead
	// of the rename below -- and the rotation would then race the source's
	// very first open instead of happening to a file it is already tailing,
	// which is a materially different (and, under load, flaky) scenario from
	// the one this test exists to prove.
	waitFor(t, 30*time.Second, "the pre-rotation lines to reach the hypertable", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= beforeRotation
	})

	// Rename-and-recreate while writing continues: the old inode is renamed
	// out from under the tail source's open fd, and a fresh file appears at
	// the same path (appendLines' O_CREATE) immediately after.
	require.NoError(t, os.Rename(path, path+".1"), "rotate %s", path)
	appendLines(t, path, beforeRotation+1, total)

	waitFor(t, 60*time.Second, "every rotated line to reach the hypertable", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= total
	})

	// No restart occurs in this test, so the one TailSource that ran for its
	// whole duration reads each byte exactly once: the old fd is drained to
	// true EOF before the switch (see TailSource.rotate), so zero is the
	// correct duplicate bound here, not a loose one.
	assertNoGapsBoundedDuplicates(ctx, t, pool, service, total, 0)
}

// TestAgentSurvivesTruncation exercises copytruncate: the file's content is
// copied elsewhere and the original is truncated to zero while the same
// inode keeps being written to, which TailSource can only tell apart from a
// rotation by watching that same fd's own size and head fingerprint (see
// resolveState's "same inode" branch in tailfile.go).
func TestAgentSurvivesTruncation(t *testing.T) {
	t.Parallel()

	const service = "agent-truncate"
	pool, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	const (
		beforeTruncate = 150
		afterTruncate  = 150
		total          = beforeTruncate + afterTruncate
	)

	appendLines(t, path, 1, beforeTruncate)

	p := startPipeline(t, &pipelineConfig{
		path:           path,
		checkpointPath: filepath.Join(dir, "checkpoint.json"),
		spoolDir:       filepath.Join(dir, "spool"),
		ingestAddr:     c.addr,
		service:        service,
	})
	defer p.stop(t, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait for the pre-truncate lines to land before truncating, for the same
	// reason the rotation test waits before rotating: the source must already
	// be tailing this file when the disruption happens, not still racing its
	// own first open against it.
	waitFor(t, 30*time.Second, "the pre-truncate lines to reach the hypertable", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= beforeTruncate
	})

	// "Copy the content away" -- what a copytruncate rotator does before
	// truncating, so the pre-truncate lines are not simply discarded by the
	// rotator itself. The copy is never read back by anything; only the
	// truncate below is what TailSource has to detect.
	content, err := os.ReadFile(path)
	require.NoError(t, err, "read %s before truncation", path)
	require.NoError(t, os.WriteFile(path+".1", content, 0o600), "write copytruncate backup")

	require.NoError(t, os.Truncate(path, 0), "truncate %s", path)
	appendLines(t, path, beforeTruncate+1, total)

	waitFor(t, 60*time.Second, "every post-truncation line to reach the hypertable", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= total
	})

	// Same reasoning as the rotation test: one continuous TailSource run
	// reads each byte exactly once, so zero duplicates is the correct bound.
	assertNoGapsBoundedDuplicates(ctx, t, pool, service, total, 0)
}

// TestAgentResumesAfterRestart proves the checkpoint, not just the source
// file, is what makes a restart safe: an ungracefully stopped pipeline is
// rebuilt from scratch over the same checkpoint and spool paths, and the
// prefix that was already acknowledged before the stop must not be re-shipped
// from the beginning.
func TestAgentResumesAfterRestart(t *testing.T) {
	t.Parallel()

	const service = "agent-restart"
	pool, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)

	dir := t.TempDir()
	cfg := pipelineConfig{
		path:           filepath.Join(dir, "app.log"),
		checkpointPath: filepath.Join(dir, "checkpoint.json"),
		spoolDir:       filepath.Join(dir, "spool"),
		ingestAddr:     c.addr,
		service:        service,
	}

	const (
		acked    = 300 // written and waited-for before the ungraceful stop
		inFlight = 100 // written just before the stop, with no wait at all
		total    = acked + inFlight
	)

	first := startPipeline(t, &cfg)

	appendLines(t, cfg.path, 1, acked)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	waitFor(t, 60*time.Second, "the pre-restart prefix to be fully acknowledged", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= acked
	})

	// Written with no wait at all: some of these may already be accepted and
	// durably written by the time the stop below runs, some may not have
	// left this process yet. Either is fine -- see restartMaxDuplicates.
	appendLines(t, cfg.path, acked+1, total)

	// An ungraceful stop, on purpose: stop still runs the shipper's shutdown
	// sequence (see the pipeline doc comment), but nothing here waits for the
	// in-flight tail to be acknowledged first, which is exactly the state a
	// real crash or a redeploy leaves behind.
	first.stop(t, 10*time.Second)

	// Rebuilt from scratch over the same paths: a new CheckpointStore that
	// Loads from checkpointPath, a new Spool that recovers spoolDir, and a
	// new TailSource resumed from whatever the checkpoint says -- exactly
	// what a restarted process does, not an in-memory continuation of the
	// same objects.
	second := startPipeline(t, &cfg)
	defer second.stop(t, 10*time.Second)

	waitFor(t, 60*time.Second, "every line to reach the hypertable after resume", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= total
	})

	assertNoGapsBoundedDuplicates(ctx, t, pool, service, total, restartMaxDuplicates)

	// The property that actually distinguishes a working checkpoint from a
	// no-op one: the prefix fully acknowledged before the stop must appear
	// exactly once. If the checkpoint were not honored on resume, the
	// restarted TailSource would reread the whole file from byte zero and
	// this count would be 2*acked instead of acked.
	counts := lineNumberCounts(ctx, t, pool, service)
	prefixRows := 0
	for i := 1; i <= acked; i++ {
		prefixRows += counts[i]
	}
	require.Equal(t, acked, prefixRows,
		"the acknowledged prefix was re-shipped from the beginning instead of resuming from the checkpoint")
}

// TestAgentReplaysSpoolAfterOutage proves the spool: writes made while the
// collector is unreachable are buffered to disk and delivered once it comes
// back, with the agent itself never restarting and the collector's address
// never changing from the agent's point of view (see outageGate).
func TestAgentReplaysSpoolAfterOutage(t *testing.T) {
	t.Parallel()

	const service = "agent-spool-outage"
	pool, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)

	gate := newOutageGate(t)
	gate.setUp(c.addr)

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	const (
		beforeOutage = 100
		duringOutage = 150
		afterOutage  = 50
		total        = beforeOutage + duringOutage + afterOutage
	)

	appendLines(t, path, 1, beforeOutage)

	p := startPipeline(t, &pipelineConfig{
		path:           path,
		checkpointPath: filepath.Join(dir, "checkpoint.json"),
		spoolDir:       filepath.Join(dir, "spool"),
		ingestAddr:     gate.addr,
		service:        service,
	})
	defer p.stop(t, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	waitFor(t, 30*time.Second, "the pre-outage lines to reach the hypertable", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= beforeOutage
	})

	gate.setDown()

	appendLines(t, path, beforeOutage+1, beforeOutage+duringOutage)

	// The one thing this test exists to exercise: without this, the test
	// could pass even if the spool never actually buffered anything.
	waitFor(t, 30*time.Second, "the spool to hold data while the collector is unreachable", func() bool {
		return p.spool.Len() > 0
	})

	gate.setUp(c.addr)

	appendLines(t, path, beforeOutage+duringOutage+1, total)

	waitFor(t, 60*time.Second, "every line, including the spooled backlog, to reach the hypertable", func() bool {
		return len(lineNumberCounts(ctx, t, pool, service)) >= total
	})

	// No restart occurs here, so every line is still read exactly once by
	// the one TailSource that ran the whole test, and any batch resent after
	// a lost ack -- rather than a lost connection before any ack -- carries
	// the exact bytes it did the first time: same stream_id, seq and time.
	// Storage's (stream_id, seq, time) dedup index (migrations/0001,
	// logs_dedup) collapses that before it is ever visible as a row. Zero is
	// therefore the correct bound, not a loose one.
	assertNoGapsBoundedDuplicates(ctx, t, pool, service, total, 0)
}
