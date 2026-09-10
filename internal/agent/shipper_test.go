package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/ingest"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// These tests deliberately do not mock ingest.Client: they stand up a real
// in-process gRPC server implementing LogService, with a stub whose ack
// behavior each test controls. That is what exercises the real flow-control
// behavior the send/receive concurrency invariant depends on — a mock
// client cannot deadlock the way a real one can.

// defaultTestLabels is the label set every test uses unless it overrides
// ShipperConfig.Labels for a specific case (no-labels, multi-stream, etc.).
var defaultTestLabels = model.LabelSet{Service: "svc", Host: "host", Env: "test"}

// labelsFunc returns a Labels resolver that maps every source to ls.
func labelsFunc(ls model.LabelSet) func(string) (model.LabelSet, bool) {
	return func(string) (model.LabelSet, bool) { return ls, true }
}

// testLine builds a Line whose Cursor is derived from seq and the message
// length, the same way a real source would: Start is the byte offset of the
// line, Offset is the byte offset immediately past it.
func testLine(source, msg string, seq int64) Line {
	return Line{
		Source: source,
		Time:   time.Now(),
		Bytes:  []byte(msg),
		Cursor: Cursor{Start: seq, Offset: seq + int64(len(msg)) + 1},
	}
}

func newTestSpool(t *testing.T) *Spool {
	t.Helper()
	sp, err := NewSpool(SpoolConfig{Dir: t.TempDir(), MaxBytes: 8 << 20, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("new spool: %v", err)
	}
	t.Cleanup(func() { _ = sp.Close() })
	return sp
}

func newTestCheckpoint(t *testing.T) *CheckpointStore {
	t.Helper()
	return NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json"))
}

// newTestShipperConfig returns a config pointed at addr with short,
// test-friendly timings. Individual tests override whichever field their
// scenario needs.
func newTestShipperConfig(t *testing.T, addr string) *ShipperConfig {
	t.Helper()
	return &ShipperConfig{
		Ingest:          ingest.ClientConfig{Addr: addr},
		Spool:           newTestSpool(t),
		Checkpoint:      newTestCheckpoint(t),
		Labels:          labelsFunc(defaultTestLabels),
		MaxBatchRecords: 1000,
		MaxBatchBytes:   1 << 20,
		MaxBatchDelay:   200 * time.Millisecond,
		AckWindow:       8,
		MinBackoff:      5 * time.Millisecond,
		MaxBackoff:      50 * time.Millisecond,
	}
}

// newTestShipper builds a Shipper and shrinks the two internal tunables
// that are not part of the public config (see their doc comments in
// shipper.go for why) so the whole suite stays fast.
func newTestShipper(t *testing.T, cfg *ShipperConfig) *Shipper {
	t.Helper()
	s, err := NewShipper(cfg)
	if err != nil {
		t.Fatalf("new shipper: %v", err)
	}
	s.checkpointCommitInterval = 30 * time.Millisecond
	s.shutdownAckWait = 500 * time.Millisecond
	return s
}

// waitForRun blocks until Run returns, failing the test if it takes longer
// than timeout — the hard timeout every test needs to turn a deadlock into a
// failure instead of a hang.
func waitForRun(t *testing.T, done <-chan error, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("Run did not return in time")
	}
}

// waitForBatches polls until the stub has received at least n batches,
// failing the test if timeout elapses first.
func waitForBatches(t *testing.T, stub *stubServer, n int, timeout time.Duration) []*logaggv1.LogBatch {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := stub.receivedBatches()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d batches, got %d", n, len(got))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// pendingSend is one ack a stubServer has decided on but not yet sent,
// because the test asked it to hold batches back.
type pendingSend struct {
	ack    *logaggv1.Ack
	stream logaggv1.LogService_StreamServer
}

// stubServer is a minimal, test-controlled implementation of LogService. It
// is intentionally simple: each test tells it what to decide for a received
// batch (accept, reject with a code, or hold the ack back) rather than the
// stub encoding any policy of its own.
type stubServer struct {
	logaggv1.UnimplementedLogServiceServer

	mu       sync.Mutex
	received []*logaggv1.LogBatch
	held     []pendingSend
	// decide is called for each received batch and returns the ack to send
	// and whether to hold it back instead of sending it now. A nil decide
	// accepts every batch immediately, which is what most tests want.
	decide func(*logaggv1.LogBatch) (ack *logaggv1.Ack, hold bool)
}

func acceptAck(batch *logaggv1.LogBatch) *logaggv1.Ack {
	return &logaggv1.Ack{
		BatchId:  batch.GetBatchId(),
		Code:     logaggv1.AckCode_ACK_CODE_ACCEPTED,
		Accepted: uint32(len(batch.GetRecords())), //nolint:gosec // test data, bounded by test input
	}
}

func (s *stubServer) Stream(stream logaggv1.LogService_StreamServer) error {
	for {
		batch, err := stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}

		s.mu.Lock()
		s.received = append(s.received, batch)
		decide := s.decide
		s.mu.Unlock()

		ack, hold := acceptAck(batch), false
		if decide != nil {
			ack, hold = decide(batch)
		}
		if hold {
			s.mu.Lock()
			s.held = append(s.held, pendingSend{ack: ack, stream: stream})
			s.mu.Unlock()
			continue
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (s *stubServer) receivedBatches() []*logaggv1.LogBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*logaggv1.LogBatch, len(s.received))
	copy(out, s.received)
	return out
}

// releaseHeld sends every ack this stub has held back so far, in the order
// it received them.
func (s *stubServer) releaseHeld(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	held := s.held
	s.held = nil
	s.mu.Unlock()
	for _, h := range held {
		if err := h.stream.Send(h.ack); err != nil {
			t.Fatalf("send held ack: %v", err)
		}
	}
}

// startStubServer binds addr (use "127.0.0.1:0" for an ephemeral port) and
// serves srv on it, returning the address actually bound and a stop
// function safe to call more than once (also registered as cleanup, so
// tests that need to stop the server early — the mid-run disconnect test —
// can do so without double-stopping at teardown).
func startStubServer(t *testing.T, srv *stubServer, addr string) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}
	gs := grpc.NewServer()
	logaggv1.RegisterLogServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	var stopOnce sync.Once
	stop := func() { stopOnce.Do(gs.Stop) }
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

func TestShipper_HappyPath(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	s := newTestShipper(t, cfg)

	in := make(chan Line, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "hello", 0)
	in <- testLine("app.log", "world", 10)
	close(in)

	waitForRun(t, done, 4*time.Second)

	got := stub.receivedBatches()
	if len(got) != 1 || len(got[0].GetRecords()) != 2 {
		t.Fatalf("got %d batches, want 1 batch with 2 records: %+v", len(got), got)
	}

	cur, ok := s.checkpoint.Get("app.log")
	if !ok {
		t.Fatal("checkpoint was not set for app.log")
	}
	if cur.Start != 10 {
		t.Fatalf("checkpoint start = %d, want 10 (the last acked cursor)", cur.Start)
	}
	if s.spool.Len() != 0 {
		t.Fatalf("spool len = %d, want 0", s.spool.Len())
	}
}

func TestShipper_SizeTriggeredBatching(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 3
	cfg.MaxBatchDelay = 5 * time.Second // must not be what triggers this batch
	s := newTestShipper(t, cfg)

	in := make(chan Line, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	for i := 0; i < 3; i++ {
		in <- testLine("app.log", fmt.Sprintf("line-%d", i), int64(i*10))
	}

	got := waitForBatches(t, stub, 1, 1*time.Second)
	if len(got[0].GetRecords()) != 3 {
		t.Fatalf("batch has %d records, want 3", len(got[0].GetRecords()))
	}

	close(in)
	waitForRun(t, done, 2*time.Second)
}

func TestShipper_TimeTriggeredBatching(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 1000 // must not be what triggers this batch
	cfg.MaxBatchDelay = 80 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "a", 0)
	in <- testLine("app.log", "b", 10)

	got := waitForBatches(t, stub, 1, 1*time.Second)
	if len(got[0].GetRecords()) != 2 {
		t.Fatalf("batch has %d records, want 2", len(got[0].GetRecords()))
	}

	close(in)
	waitForRun(t, done, 2*time.Second)
}

func TestShipper_BatchNeverExceedsByteBound(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 1000
	cfg.MaxBatchDelay = 2 * time.Second
	cfg.MaxBatchBytes = 400 // small enough that ~90-byte messages must split
	s := newTestShipper(t, cfg)

	in := make(chan Line, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	msg := strings.Repeat("x", 80)
	for i := 0; i < 10; i++ {
		in <- testLine("app.log", fmt.Sprintf("%s-%d", msg, i), int64(i*100))
	}
	close(in)

	waitForRun(t, done, 3*time.Second)

	got := stub.receivedBatches()
	if len(got) < 2 {
		t.Fatalf("got %d batches, want more than 1 given the byte bound", len(got))
	}
	total := 0
	for _, b := range got {
		if sz := proto.Size(b); sz > cfg.MaxBatchBytes {
			t.Fatalf("batch marshaled to %d bytes, want <= %d", sz, cfg.MaxBatchBytes)
		}
		total += len(b.GetRecords())
	}
	if total != 10 {
		t.Fatalf("got %d total records across all batches, want 10", total)
	}
}

func TestShipper_AckWindowRespectedWithoutDeadlock(t *testing.T) {
	stub := &stubServer{decide: func(_ *logaggv1.LogBatch) (*logaggv1.Ack, bool) {
		return nil, true // hold every ack back forever
	}}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 1
	cfg.AckWindow = 3
	s := newTestShipper(t, cfg)

	in := make(chan Line, 20)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	const lines = 10
	for i := 0; i < lines; i++ {
		in <- testLine("app.log", fmt.Sprintf("line-%d", i), int64(i*10))
	}

	// Give the shipper time to send up to the window and, if the
	// concurrency invariant were broken, to deadlock instead of stopping.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if len(stub.receivedBatches()) >= cfg.AckWindow {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let any over-eager send happen if the invariant is broken

	if got := len(stub.receivedBatches()); got != cfg.AckWindow {
		t.Fatalf("server received %d batches, want exactly the ack window (%d)", got, cfg.AckWindow)
	}
	if s.spool.Len() == 0 {
		t.Fatal("batches past the ack window should have been spooled, not sent")
	}

	close(in)
	waitForRun(t, done, 2*time.Second)
}

func TestShipper_Overloaded(t *testing.T) {
	stub := &stubServer{}
	var mu sync.Mutex
	calls := 0
	stub.decide = func(b *logaggv1.LogBatch) (*logaggv1.Ack, bool) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			return &logaggv1.Ack{
				BatchId:  b.GetBatchId(),
				Code:     logaggv1.AckCode_ACK_CODE_OVERLOADED,
				Rejected: uint32(len(b.GetRecords())), //nolint:gosec // test data
			}, false
		}
		return acceptAck(b), false
	}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MinBackoff = 20 * time.Millisecond
	cfg.MaxBackoff = 50 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "x", 0)

	waitForBatches(t, stub, 1, 1*time.Second)
	// The backoff itself — that armPause schedules a full-jitter delay
	// rather than retrying immediately — is TestShipper_BackoffHasJitter's
	// job. Full jitter's draw can legitimately land anywhere in [0, d], so
	// there is no per-call lower bound this test could assert on elapsed
	// time without being flaky; what matters here is that the batch is
	// spooled and retried at all, which the second waitForBatches proves.
	waitForBatches(t, stub, 2, 2*time.Second)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.checkpoint.Get("app.log"); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cur, ok := s.checkpoint.Get("app.log")
	if !ok {
		t.Fatal("checkpoint never advanced after the retried batch was accepted")
	}
	if cur.Start != 0 {
		t.Fatalf("checkpoint start = %d, want 0", cur.Start)
	}

	close(in)
	waitForRun(t, done, 2*time.Second)

	if s.spool.Len() != 0 {
		t.Fatalf("spool len = %d, want 0 once the retried batch was accepted", s.spool.Len())
	}
}

func TestShipper_Invalid(t *testing.T) {
	stub := &stubServer{decide: func(b *logaggv1.LogBatch) (*logaggv1.Ack, bool) {
		return &logaggv1.Ack{
			BatchId:  b.GetBatchId(),
			Code:     logaggv1.AckCode_ACK_CODE_INVALID,
			Rejected: uint32(len(b.GetRecords())), //nolint:gosec // test data
		}, false
	}}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	s := newTestShipper(t, cfg)

	in := make(chan Line, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "rejected forever", 0)
	close(in)

	waitForRun(t, done, 2*time.Second)

	if got := len(stub.receivedBatches()); got != 1 {
		t.Fatalf("server received %d batches, want exactly 1 (INVALID must not be retried)", got)
	}
	if _, ok := s.checkpoint.Get("app.log"); ok {
		t.Fatal("checkpoint should not advance for a batch the collector rejected as invalid")
	}
	if s.spool.Len() != 0 {
		t.Fatalf("spool len = %d, want 0 — an INVALID batch must be dropped, not spooled", s.spool.Len())
	}
	if got, want := counterValue(t, s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonAckInvalid)), float64(1); got != want {
		t.Fatalf("ack_invalid drop count = %v, want %v", got, want)
	}
}

func TestShipper_InternalRetriesAtSameRate(t *testing.T) {
	stub := &stubServer{}
	var mu sync.Mutex
	calls := 0
	stub.decide = func(b *logaggv1.LogBatch) (*logaggv1.Ack, bool) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			return &logaggv1.Ack{
				BatchId:  b.GetBatchId(),
				Code:     logaggv1.AckCode_ACK_CODE_INTERNAL,
				Rejected: uint32(len(b.GetRecords())), //nolint:gosec // test data
			}, false
		}
		return acceptAck(b), false
	}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	// Deliberately huge: if INTERNAL backed off the way OVERLOADED does,
	// the second batch would not arrive within this test's wait bound.
	cfg.MinBackoff = 5 * time.Second
	cfg.MaxBackoff = 5 * time.Second
	s := newTestShipper(t, cfg)

	in := make(chan Line, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "x", 0)

	waitForBatches(t, stub, 2, 1*time.Second)

	close(in)
	waitForRun(t, done, 2*time.Second)

	if _, ok := s.checkpoint.Get("app.log"); !ok {
		t.Fatal("checkpoint should advance once the retried batch is accepted")
	}
}

func TestShipper_CollectorUnreachableAtStartup(t *testing.T) {
	// Reserve a port, then release it, so addr names a real address
	// nothing is listening on yet.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}

	cfg := newTestShipperConfig(t, addr)
	cfg.MinBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 30 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "queued while down", 0)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && s.spool.Len() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if s.spool.Len() == 0 {
		t.Fatal("line was not spooled while the collector was unreachable")
	}

	stub := &stubServer{}
	startStubServer(t, stub, addr)

	waitForBatches(t, stub, 1, 3*time.Second)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, ok := s.checkpoint.Get("app.log")
		if ok && s.spool.Len() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s.spool.Len() != 0 {
		t.Fatalf("spool len = %d, want 0 once the collector appeared", s.spool.Len())
	}
	if _, ok := s.checkpoint.Get("app.log"); !ok {
		t.Fatal("checkpoint did not advance once the spool drained")
	}

	close(in)
	waitForRun(t, done, 2*time.Second)
}

func TestShipper_MidRunDisconnect(t *testing.T) {
	stub1 := &stubServer{}
	addr, stop1 := startStubServer(t, stub1, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 1
	cfg.MinBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 30 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "before-disconnect", 0)
	waitForBatches(t, stub1, 1, 2*time.Second)

	// Give the ack a moment to land before pulling the rug out, so this
	// test is about losing the connection, not about racing the first ack.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.checkpoint.Get("app.log"); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	stop1()

	in <- testLine("app.log", "during-outage", 20)

	stub2 := &stubServer{}
	startStubServer(t, stub2, addr)

	waitForBatches(t, stub2, 1, 4*time.Second)

	close(in)
	waitForRun(t, done, 3*time.Second)

	cur, ok := s.checkpoint.Get("app.log")
	if !ok || cur.Start < 20 {
		t.Fatalf("checkpoint = %+v ok=%v, want it advanced past the during-outage line", cur, ok)
	}

	// Bounded duplicates: the during-outage line may have been resent once
	// (if it was in flight at the moment of disconnect), but not an
	// unbounded number of times.
	total := len(stub1.receivedBatches()) + len(stub2.receivedBatches())
	if total > 4 {
		t.Fatalf("saw %d batches total across both servers, want a small bounded number", total)
	}
}

func TestShipper_SpoolDrainsBeforeFresh(t *testing.T) {
	stub := &stubServer{}
	var mu sync.Mutex
	released := false
	stub.decide = func(b *logaggv1.LogBatch) (*logaggv1.Ack, bool) {
		mu.Lock()
		r := released
		mu.Unlock()
		return acceptAck(b), !r
	}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 1
	cfg.AckWindow = 1 // forces every batch after the first outstanding one to spool
	s := newTestShipper(t, cfg)

	in := make(chan Line, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "first", 0)
	waitForBatches(t, stub, 1, 1*time.Second)

	in <- testLine("app.log", "second", 10)
	in <- testLine("app.log", "third", 20)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && s.spool.Len() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.spool.Len(); got != 2 {
		t.Fatalf("spool len = %d, want 2 (second and third queued behind the outstanding first)", got)
	}

	mu.Lock()
	released = true
	mu.Unlock()
	stub.releaseHeld(t)

	got := waitForBatches(t, stub, 3, 3*time.Second)

	close(in)
	waitForRun(t, done, 2*time.Second)

	want := []string{"first", "second", "third"}
	for i, b := range got {
		if len(b.GetRecords()) != 1 || b.GetRecords()[0].GetMessage() != want[i] {
			t.Fatalf("batch %d = %+v, want the single record %q — the spool must drain in order before anything else", i, b, want[i])
		}
	}
}

func TestShipper_InvalidRecordDroppedRestOfBatchShips(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchDelay = 50 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	bad := testLine("app.log", "bad", 10)
	bad.Time = time.Time{} // zero time fails model.LogRecord.Validate

	in <- testLine("app.log", "good-1", 0)
	in <- bad
	in <- testLine("app.log", "good-2", 20)
	close(in)

	waitForRun(t, done, 2*time.Second)

	got := stub.receivedBatches()
	if len(got) != 1 || len(got[0].GetRecords()) != 2 {
		t.Fatalf("got %d batches, want 1 batch with the 2 valid records: %+v", len(got), got)
	}
	if got[0].GetRecords()[0].GetMessage() != "good-1" || got[0].GetRecords()[1].GetMessage() != "good-2" {
		t.Fatalf("unexpected records shipped: %+v", got[0].GetRecords())
	}
	if n := counterValue(t, s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonInvalidRecord)); n != 1 {
		t.Fatalf("invalid_record drop count = %v, want 1", n)
	}

	cur, ok := s.checkpoint.Get("app.log")
	if !ok || cur.Start != 20 {
		t.Fatalf("checkpoint = %+v ok=%v, want it advanced past the invalid record too (start=20)", cur, ok)
	}
}

func TestShipper_NoLabelsDropped(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = func(source string) (model.LabelSet, bool) {
		if source == "unlabeled" {
			return model.LabelSet{}, false
		}
		return defaultTestLabels, true
	}
	s := newTestShipper(t, cfg)

	in := make(chan Line, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("unlabeled", "nowhere to go", 0)
	close(in)

	waitForRun(t, done, 2*time.Second)

	if got := len(stub.receivedBatches()); got != 0 {
		t.Fatalf("server received %d batches, want 0", got)
	}
	if n := counterValue(t, s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonNoLabels)); n != 1 {
		t.Fatalf("no_labels drop count = %v, want 1", n)
	}
}

func TestShipper_CheckpointNeverMovesBackward(t *testing.T) {
	cfg := &ShipperConfig{
		Ingest:     ingest.ClientConfig{Addr: "127.0.0.1:1"}, // never dialed by this unit test
		Spool:      newTestSpool(t),
		Checkpoint: newTestCheckpoint(t),
		Labels:     labelsFunc(defaultTestLabels),
	}
	s, err := NewShipper(cfg)
	if err != nil {
		t.Fatalf("new shipper: %v", err)
	}

	s.advanceCheckpoint("app.log", Cursor{Start: 0, Offset: 100})
	s.advanceCheckpoint("app.log", Cursor{Start: 0, Offset: 40}) // must be ignored

	cur, ok := s.checkpoint.Get("app.log")
	if !ok || cur.Offset != 100 {
		t.Fatalf("checkpoint = %+v ok=%v, want Offset=100 (the backward Set must have been ignored)", cur, ok)
	}
}

func TestShipper_ShutdownFlushes(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	ckptPath := filepath.Join(t.TempDir(), "checkpoint.json")
	cfg := newTestShipperConfig(t, addr)
	cfg.Checkpoint = NewCheckpointStore(ckptPath)
	cfg.MaxBatchDelay = 5 * time.Second // only the shutdown flush should force this batch out
	s := newTestShipper(t, cfg)

	in := make(chan Line, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("app.log", "flush-me", 0)
	close(in)

	waitForRun(t, done, 2*time.Second)

	if got := len(stub.receivedBatches()); got != 1 {
		t.Fatalf("server received %d batches, want the pending batch flushed at shutdown", got)
	}

	reloaded := NewCheckpointStore(ckptPath)
	if err := reloaded.Load(); err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	cur, ok := reloaded.Get("app.log")
	if !ok || cur.Start != 0 {
		t.Fatalf("persisted checkpoint = %+v ok=%v, want it committed at shutdown", cur, ok)
	}
}

func TestShipper_BackoffHasJitter(t *testing.T) {
	cfg := &ShipperConfig{
		Ingest:     ingest.ClientConfig{Addr: "127.0.0.1:1"},
		Spool:      newTestSpool(t),
		Checkpoint: newTestCheckpoint(t),
		Labels:     labelsFunc(defaultTestLabels),
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond, // fixed base: any variance seen is jitter, not growth
	}
	s, err := NewShipper(cfg)
	if err != nil {
		t.Fatalf("new shipper: %v", err)
	}

	seen := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		s.attempt = 0 // hold the exponential term fixed
		seen[s.backoffDelay()] = true
	}
	if len(seen) < 2 {
		t.Fatalf("got %d distinct delays across 20 draws at a fixed backoff, want jitter to produce more than one", len(seen))
	}
}

func TestNewShipper_Validation(t *testing.T) {
	validConfig := func(t *testing.T) *ShipperConfig {
		t.Helper()
		return &ShipperConfig{
			Ingest:     ingest.ClientConfig{Addr: "127.0.0.1:1"},
			Spool:      newTestSpool(t),
			Checkpoint: newTestCheckpoint(t),
			Labels:     labelsFunc(defaultTestLabels),
		}
	}

	t.Run("nil config", func(t *testing.T) {
		if _, err := NewShipper(nil); err == nil {
			t.Fatal("want an error for a nil config")
		}
	})
	t.Run("missing spool", func(t *testing.T) {
		cfg := validConfig(t)
		cfg.Spool = nil
		if _, err := NewShipper(cfg); err == nil {
			t.Fatal("want an error for a missing spool")
		}
	})
	t.Run("missing checkpoint", func(t *testing.T) {
		cfg := validConfig(t)
		cfg.Checkpoint = nil
		if _, err := NewShipper(cfg); err == nil {
			t.Fatal("want an error for a missing checkpoint store")
		}
	})
	t.Run("missing labels", func(t *testing.T) {
		cfg := validConfig(t)
		cfg.Labels = nil
		if _, err := NewShipper(cfg); err == nil {
			t.Fatal("want an error for a missing labels resolver")
		}
	})
	t.Run("missing addr", func(t *testing.T) {
		cfg := validConfig(t)
		cfg.Ingest.Addr = ""
		if _, err := NewShipper(cfg); err == nil {
			t.Fatal("want an error for a missing ingest address")
		}
	})
	t.Run("min backoff exceeds max", func(t *testing.T) {
		cfg := validConfig(t)
		cfg.MinBackoff = time.Minute
		cfg.MaxBackoff = time.Second
		if _, err := NewShipper(cfg); err == nil {
			t.Fatal("want an error when min backoff exceeds max backoff")
		}
	})
	t.Run("defaults applied", func(t *testing.T) {
		s, err := NewShipper(validConfig(t))
		if err != nil {
			t.Fatalf("new shipper: %v", err)
		}
		if s.maxBatchRecords != defaultMaxBatchRecords {
			t.Errorf("maxBatchRecords = %d, want %d", s.maxBatchRecords, defaultMaxBatchRecords)
		}
		if s.maxBatchBytes != defaultMaxBatchBytes {
			t.Errorf("maxBatchBytes = %d, want %d", s.maxBatchBytes, defaultMaxBatchBytes)
		}
		if s.maxBatchDelay != defaultMaxBatchDelay {
			t.Errorf("maxBatchDelay = %s, want %s", s.maxBatchDelay, defaultMaxBatchDelay)
		}
		if s.ackWindow != defaultAckWindow {
			t.Errorf("ackWindow = %d, want %d", s.ackWindow, defaultAckWindow)
		}
		if s.minBackoff != defaultMinBackoff || s.maxBackoff != defaultMaxBackoff {
			t.Errorf("backoff bounds = [%s, %s], want [%s, %s]", s.minBackoff, s.maxBackoff, defaultMinBackoff, defaultMaxBackoff)
		}
		if s.metrics == nil {
			t.Error("metrics should default to an unregistered set, not stay nil")
		}
	})
}

// TestShipper_CheckpointAdvancesAcrossRotation pins that a rotation is not
// mistaken for a backward cursor.
//
// A file source's Offset resets to zero when the file rotates (see
// TailSource.rotate), so the first cursor from a new generation legitimately has
// a *smaller* Offset than the last cursor from the old one. A monotonicity guard
// that compares Offset alone silently discards it, and the checkpoint then
// freezes at the old generation forever: every restart re-reads the whole current
// file and reports a missed generation that never happened, which turns the one
// metric that is supposed to mean real data loss into a false alarm.
//
// The guard is about out-of-order acks *within* one generation, which is why it
// has to be scoped to a matching FileID.
func TestShipper_CheckpointAdvancesAcrossRotation(t *testing.T) {
	cfg := &ShipperConfig{
		Ingest:     ingest.ClientConfig{Addr: "127.0.0.1:1"}, // never dialed by this unit test
		Spool:      newTestSpool(t),
		Checkpoint: newTestCheckpoint(t),
		Labels:     labelsFunc(defaultTestLabels),
	}
	s, err := NewShipper(cfg)
	if err != nil {
		t.Fatalf("new shipper: %v", err)
	}

	oldGen := FileID{Dev: 1, Ino: 10}
	newGen := FileID{Dev: 1, Ino: 11}

	s.advanceCheckpoint("app.log", Cursor{Start: 4900, Offset: 5000, File: oldGen})
	// Rotation: a new inode, and the offset restarts near the beginning.
	s.advanceCheckpoint("app.log", Cursor{Start: 200, Offset: 300, File: newGen})

	cur, ok := s.checkpoint.Get("app.log")
	if !ok {
		t.Fatal("checkpoint has no cursor for the source")
	}
	if cur.File != newGen {
		t.Errorf("checkpoint File = %+v, want the new generation %+v; a rotation was treated as a backward cursor", cur.File, newGen)
	}
	if cur.Offset != 300 {
		t.Errorf("checkpoint Offset = %d, want 300", cur.Offset)
	}

	// Within the new generation the guard must still hold.
	s.advanceCheckpoint("app.log", Cursor{Start: 100, Offset: 150, File: newGen})
	cur, _ = s.checkpoint.Get("app.log")
	if cur.Offset != 300 {
		t.Errorf("checkpoint Offset = %d after a backward same-generation Set, want 300", cur.Offset)
	}
}

// twoSourceLabels returns a Labels resolver mapping "source-a" to lsA and
// "source-b" to lsB, and reporting no labels for anything else.
func twoSourceLabels(lsA, lsB model.LabelSet) func(string) (model.LabelSet, bool) {
	return func(source string) (model.LabelSet, bool) {
		switch source {
		case "source-a":
			return lsA, true
		case "source-b":
			return lsB, true
		default:
			return model.LabelSet{}, false
		}
	}
}

// TestShipper_InterleavedSourcesBatchIndependently is the regression test for
// the single-shared-accumulator bug: addLine used to flush whenever the
// incoming line's label set differed from whatever was currently
// accumulating, which is exactly what happens on every line once lines from
// more than one label set interleave on the one in channel — the ordinary
// configuration, not an edge case. That turned size- and time-triggered
// batching into a no-op the moment there was more than one source: every
// line flushed a one-record batch of its own instead of joining the batch
// for its own label set.
//
// Against the single-accumulator code this produces 8 batches of 1 record
// each instead of the 2 batches of 4 asserted below.
func TestShipper_InterleavedSourcesBatchIndependently(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	lsA := model.LabelSet{Service: "svc-a", Host: "host", Env: "test"}
	lsB := model.LabelSet{Service: "svc-b", Host: "host", Env: "test"}
	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = twoSourceLabels(lsA, lsB)
	cfg.MaxBatchRecords = 4
	cfg.MaxBatchDelay = 5 * time.Second // must not be what triggers this batch
	s := newTestShipper(t, cfg)

	in := make(chan Line, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	// Strictly alternating labels sets: a, b, a, b, ...
	for i := 0; i < 4; i++ {
		in <- testLine("source-a", fmt.Sprintf("a-%d", i), int64(i*10))
		in <- testLine("source-b", fmt.Sprintf("b-%d", i), int64(i*10))
	}
	close(in)

	waitForRun(t, done, 2*time.Second)

	got := stub.receivedBatches()
	if len(got) != 2 {
		t.Fatalf("got %d batches, want exactly 2 (one per label set) — interleaved sources must batch independently, not flush on every line: %+v", len(got), got)
	}
	for _, b := range got {
		if len(b.GetRecords()) != 4 {
			t.Fatalf("batch has %d records, want 4: %+v", len(b.GetRecords()), got)
		}
	}
}

// TestShipper_SharedLabelSetAcrossSources confirms that two sources whose
// Labels resolves to the *same* label set share one accumulator: they batch
// together, and both sources' cursors are tracked and checkpointed once that
// shared batch is acked.
func TestShipper_SharedLabelSetAcrossSources(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = labelsFunc(defaultTestLabels) // both sources resolve to the same label set
	cfg.MaxBatchRecords = 1000
	cfg.MaxBatchDelay = 5 * time.Second
	s := newTestShipper(t, cfg)

	in := make(chan Line, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("source-a", "a-1", 0)
	in <- testLine("source-b", "b-1", 0)
	in <- testLine("source-a", "a-2", 10)
	in <- testLine("source-b", "b-2", 10)
	close(in)

	waitForRun(t, done, 2*time.Second)

	got := stub.receivedBatches()
	if len(got) != 1 || len(got[0].GetRecords()) != 4 {
		t.Fatalf("got %d batches, want exactly 1 batch with all 4 records: %+v", len(got), got)
	}

	curA, okA := s.checkpoint.Get("source-a")
	if !okA || curA.Start != 10 {
		t.Fatalf("checkpoint for source-a = %+v ok=%v, want Start=10", curA, okA)
	}
	curB, okB := s.checkpoint.Get("source-b")
	if !okB || curB.Start != 10 {
		t.Fatalf("checkpoint for source-b = %+v ok=%v, want Start=10", curB, okB)
	}
}

// TestShipper_IndependentDelayTimers confirms that two accumulators created
// at different times each flush on their own MaxBatchDelay deadline: the
// earlier one flushing must not also flush the later one, and the later one
// must not hold the earlier one back.
func TestShipper_IndependentDelayTimers(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	lsA := model.LabelSet{Service: "svc-a", Host: "host", Env: "test"}
	lsB := model.LabelSet{Service: "svc-b", Host: "host", Env: "test"}
	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = twoSourceLabels(lsA, lsB)
	cfg.MaxBatchRecords = 1000 // only the delay should trigger these batches
	cfg.MaxBatchDelay = 150 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("source-a", "a-line", 0)
	time.Sleep(100 * time.Millisecond)
	in <- testLine("source-b", "b-line", 0)

	// Source A's accumulator was created ~150ms before source B's, so it
	// must flush first, on its own deadline, without waiting for B's line to
	// arrive or for B's own deadline.
	got := waitForBatches(t, stub, 1, 500*time.Millisecond)
	if len(got) != 1 || len(got[0].GetRecords()) != 1 || got[0].GetRecords()[0].GetMessage() != "a-line" {
		t.Fatalf("first batch = %+v, want exactly source A's one record — B's later deadline must not hold A back, nor fire early with it", got)
	}

	got = waitForBatches(t, stub, 2, 500*time.Millisecond)
	if len(got[1].GetRecords()) != 1 || got[1].GetRecords()[0].GetMessage() != "b-line" {
		t.Fatalf("second batch = %+v, want exactly source B's one record on its own, later deadline", got[1])
	}

	close(in)
	waitForRun(t, done, 2*time.Second)
}

// TestShipper_TimerSweepFlushesEveryDueAccumulator confirms the delay trigger
// eventually ships every stream's partial batch, with none lost or merged.
//
// Ordering is deliberately not asserted here. Driving it through the timer
// looked like a stronger test and was a flakier one: the accumulators are
// created as addLine processes each line, so their deadlines differ by however
// long that takes, and under load a sweep can fire after the first deadline but
// before the last. The batches then come out in deadline order across two
// sweeps, which is correct behavior that an ordering assertion cannot
// distinguish from a bug. The sort itself is slices.Sorted(maps.Keys(...)),
// which needs no test of its own.
func TestShipper_TimerSweepFlushesEveryDueAccumulator(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	labelSets := []model.LabelSet{
		{Service: "svc-1", Host: "host", Env: "test"},
		{Service: "svc-2", Host: "host", Env: "test"},
		{Service: "svc-3", Host: "host", Env: "test"},
	}
	bySource := map[string]model.LabelSet{
		"source-1": labelSets[0],
		"source-2": labelSets[1],
		"source-3": labelSets[2],
	}

	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = func(source string) (model.LabelSet, bool) {
		ls, ok := bySource[source]
		return ls, ok
	}
	cfg.MaxBatchRecords = 1000 // only the delay should trigger these batches, all at once
	cfg.MaxBatchDelay = 100 * time.Millisecond
	s := newTestShipper(t, cfg)

	in := make(chan Line, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	for source := range bySource {
		in <- testLine(source, "x", 0)
	}

	got := waitForBatches(t, stub, 3, 2*time.Second)

	close(in)
	waitForRun(t, done, 2*time.Second)

	if len(got) != 3 {
		t.Fatalf("got %d batches, want 3", len(got))
	}

	// Every configured stream must appear exactly once: one batch per accumulator,
	// none dropped and none folded into another stream's batch.
	seen := make(map[model.StreamID]int, len(labelSets))
	for _, b := range got {
		seen[model.LabelSetFromProto(b.GetLabels()).ID()]++
	}
	for _, ls := range labelSets {
		if n := seen[ls.ID()]; n != 1 {
			t.Errorf("stream %+v produced %d batches, want exactly 1", ls, n)
		}
	}
}

// TestShipper_IndependentSizeTriggers confirms that one stream reaching
// MaxBatchBytes flushes only its own accumulator, leaving another stream's
// partial, not-yet-due accumulator untouched.
func TestShipper_IndependentSizeTriggers(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	lsA := model.LabelSet{Service: "svc-a", Host: "host", Env: "test"}
	lsB := model.LabelSet{Service: "svc-b", Host: "host", Env: "test"}
	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = twoSourceLabels(lsA, lsB)
	cfg.MaxBatchRecords = 1000
	cfg.MaxBatchDelay = 5 * time.Second // only the byte bound should trigger A's flush
	cfg.MaxBatchBytes = 400             // small enough that ~80-byte messages must split for A
	s := newTestShipper(t, cfg)

	in := make(chan Line, 12)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	// B gets one small record that would fit comfortably in a batch of its
	// own, and is not due to flush for 5 seconds.
	in <- testLine("source-b", "b-partial", 0)

	// A gets pushed well past MaxBatchBytes so its accumulator flushes on
	// its own size trigger, long before either MaxBatchDelay.
	msg := strings.Repeat("x", 80)
	for i := 0; i < 10; i++ {
		in <- testLine("source-a", fmt.Sprintf("%s-%d", msg, i), int64(i*100))
	}

	got := waitForBatches(t, stub, 1, 2*time.Second)
	for _, b := range got {
		for _, r := range b.GetRecords() {
			if r.GetMessage() == "b-partial" {
				t.Fatalf("source B's partial record was flushed by source A's size trigger: %+v", got)
			}
		}
	}

	close(in)
	waitForRun(t, done, 3*time.Second)

	found := false
	for _, b := range stub.receivedBatches() {
		for _, r := range b.GetRecords() {
			if r.GetMessage() == "b-partial" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("source B's record never shipped (it should have, at shutdown)")
	}
}

// TestShipper_ShutdownFlushesAllAccumulators confirms that shutdown flushes
// every accumulator, not just one, when more than one label set has an
// active accumulator at the time in closes.
func TestShipper_ShutdownFlushesAllAccumulators(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	lsA := model.LabelSet{Service: "svc-a", Host: "host", Env: "test"}
	lsB := model.LabelSet{Service: "svc-b", Host: "host", Env: "test"}
	cfg := newTestShipperConfig(t, addr)
	cfg.Labels = twoSourceLabels(lsA, lsB)
	cfg.MaxBatchRecords = 1000
	cfg.MaxBatchDelay = 5 * time.Second // only shutdown should force these out
	s := newTestShipper(t, cfg)

	in := make(chan Line, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	in <- testLine("source-a", "a-only", 0)
	in <- testLine("source-b", "b-only", 0)
	close(in)

	waitForRun(t, done, 2*time.Second)

	got := stub.receivedBatches()
	if len(got) != 2 {
		t.Fatalf("got %d batches at shutdown, want 2 (one per still-active accumulator): %+v", len(got), got)
	}
	total := 0
	for _, b := range got {
		total += len(b.GetRecords())
	}
	if total != 2 {
		t.Fatalf("got %d total records across shutdown batches, want 2", total)
	}
}

// TestShipper_SpoolEvictionKeepsMetaAligned is the regression test for the
// spoolMeta/spool desync finding: Spool.Append's own MaxBytes eviction can
// silently drop entries from the front of the spool without spoolMeta
// knowing, and appendToSpool must trim spoolMeta by the same amount to stay
// aligned with what Peek actually returns next.
//
// The spool here is sized to hold exactly two entries: appending a third
// batch (from source-a again) forces a roll and then an eviction of the
// first batch's whole segment. Against the unfixed code, spoolMeta is never
// trimmed, so its front entry (source-a's first cursor) stays associated
// with the payload Peek now returns (source-b's batch) — a straight
// misattribution that would let a later ACCEPTED move source-a's checkpoint
// to a cursor whose batch was evicted and never delivered, while source-b's
// real cursor is never seen at all.
func TestShipper_SpoolEvictionKeepsMetaAligned(t *testing.T) {
	sample := &logaggv1.LogBatch{
		BatchId: "sample",
		Labels:  defaultTestLabels.Proto(),
		Records: []*logaggv1.LogRecord{{Message: "aaaa"}},
	}
	data, err := proto.Marshal(sample)
	if err != nil {
		t.Fatalf("marshal sample: %v", err)
	}
	entrySize := int64(len(encodeEntry(data)))

	sp, err := NewSpool(SpoolConfig{Dir: t.TempDir(), MaxBytes: entrySize * 2, SegmentBytes: entrySize})
	if err != nil {
		t.Fatalf("new spool: %v", err)
	}
	t.Cleanup(func() { _ = sp.Close() })

	cfg := &ShipperConfig{
		Ingest:     ingest.ClientConfig{Addr: "127.0.0.1:1"}, // never dialed by this unit test
		Spool:      sp,
		Checkpoint: newTestCheckpoint(t),
		Labels:     twoSourceLabels(defaultTestLabels, defaultTestLabels),
	}
	s, err := NewShipper(cfg)
	if err != nil {
		t.Fatalf("new shipper: %v", err)
	}

	batch := func(msg string) *logaggv1.LogBatch {
		return &logaggv1.LogBatch{BatchId: msg, Labels: defaultTestLabels.Proto(), Records: []*logaggv1.LogRecord{{Message: msg}}}
	}
	curA1 := Cursor{Start: 0, Offset: 10}
	curB1 := Cursor{Start: 0, Offset: 10}
	curA2 := Cursor{Start: 10, Offset: 20}

	// batch1 (source-a) and batch2 (source-b) each land in their own
	// segment; batch3 (source-a again) forces the roll that then evicts
	// batch1's segment to stay under MaxBytes.
	s.appendToSpool(batch("aaaa"), map[string]Cursor{"source-a": curA1})
	s.appendToSpool(batch("bbbb"), map[string]Cursor{"source-b": curB1})
	s.appendToSpool(batch("cccc"), map[string]Cursor{"source-a": curA2})

	if got, want := s.spool.Len(), 2; got != want {
		t.Fatalf("spool.Len() = %d, want %d (the oldest entry must have been evicted)", got, want)
	}
	if got, want := len(s.spoolMeta), 2; got != want {
		t.Fatalf("len(spoolMeta) = %d, want %d — it must be trimmed in step with the spool's own eviction", got, want)
	}

	// The oldest surviving spool entry must be batch2 (bbbb, source-b), and
	// its spoolMeta entry must name only source-b — not source-a's stale,
	// evicted-batch cursor.
	peekAndUnmarshal := func() *logaggv1.LogBatch {
		t.Helper()
		payload, ok, err := s.spool.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek: ok=%v err=%v", ok, err)
		}
		got := new(logaggv1.LogBatch)
		if err := proto.Unmarshal(payload, got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return got
	}

	oldest := peekAndUnmarshal()
	if len(oldest.GetRecords()) != 1 || oldest.GetRecords()[0].GetMessage() != "bbbb" {
		t.Fatalf("oldest surviving entry message = %q, want %q", oldest.GetRecords()[0].GetMessage(), "bbbb")
	}
	meta := s.peekSpoolMeta()
	if _, bad := meta["source-a"]; bad {
		t.Fatalf("spoolMeta for the surviving bbbb batch names source-a; it belongs only to source-b: %+v", meta)
	}
	if metaCur, metaOK := meta["source-b"]; !metaOK || metaCur != curB1 {
		t.Fatalf("spoolMeta for bbbb = %+v, want source-b -> %+v", meta, curB1)
	}

	// Simulate exactly what processAck's ACCEPTED branch does for this
	// entry: advance only the source(s) actually named by its own metadata.
	for source, cur := range meta {
		s.advanceCheckpoint(source, cur)
	}
	if _, sawA := s.checkpoint.Get("source-a"); sawA {
		t.Fatal("source-a's checkpoint must not advance from the bbbb batch's ack — it was never in that batch")
	}
	if curB, sawB := s.checkpoint.Get("source-b"); !sawB || curB != curB1 {
		t.Fatalf("source-b checkpoint = %+v ok=%v, want %+v", curB, sawB, curB1)
	}
	if err := s.spool.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	s.popSpoolMeta()

	// The last surviving entry must be batch3 (cccc, source-a's second
	// cursor) — never misattributed to the evicted first one.
	last := peekAndUnmarshal()
	if len(last.GetRecords()) != 1 || last.GetRecords()[0].GetMessage() != "cccc" {
		t.Fatalf("last surviving entry message = %q, want %q", last.GetRecords()[0].GetMessage(), "cccc")
	}
	meta = s.peekSpoolMeta()
	if metaCur, metaOK := meta["source-a"]; !metaOK || metaCur != curA2 {
		t.Fatalf("spoolMeta for cccc = %+v, want source-a -> %+v (never the evicted curA1)", meta, curA2)
	}
	for source, cur := range meta {
		s.advanceCheckpoint(source, cur)
	}
	if curA, sawA := s.checkpoint.Get("source-a"); !sawA || curA != curA2 {
		t.Fatalf("source-a checkpoint = %+v ok=%v, want %+v (curA1 must never have been used)", curA, sawA, curA2)
	}
}

// TestShipper_OverloadedDemotesNewerOutstanding is the regression test for
// demoting only the acked batch on OVERLOADED while leaving younger,
// still-outstanding batches in flight. With three batches outstanding from
// one source, an OVERLOADED on the first must demote all three together —
// not just the first — so a later ACCEPTED for the second can never advance
// the checkpoint past lines the first (now-spooled, unacked) batch has not
// delivered.
func TestShipper_OverloadedDemotesNewerOutstanding(t *testing.T) {
	cfg := &ShipperConfig{
		Ingest:     ingest.ClientConfig{Addr: "127.0.0.1:1"}, // never dialed by this unit test
		Spool:      newTestSpool(t),
		Checkpoint: newTestCheckpoint(t),
		Labels:     labelsFunc(defaultTestLabels),
	}
	s, err := NewShipper(cfg)
	if err != nil {
		t.Fatalf("new shipper: %v", err)
	}
	t.Cleanup(func() {
		if s.pauseTimer != nil {
			s.pauseTimer.Stop()
		}
	})

	makeBatch := func(id, msg string) *logaggv1.LogBatch {
		return &logaggv1.LogBatch{BatchId: id, Labels: defaultTestLabels.Proto(), Records: []*logaggv1.LogRecord{{Message: msg}}}
	}
	cur1 := Cursor{Start: 0, Offset: 10}
	cur2 := Cursor{Start: 10, Offset: 20}
	cur3 := Cursor{Start: 20, Offset: 30}

	b1 := makeBatch("b1", "one")
	b2 := makeBatch("b2", "two")
	b3 := makeBatch("b3", "three")
	s.pendingAcks = []outstanding{
		{batch: b1, sourceCursors: map[string]Cursor{"app.log": cur1}},
		{batch: b2, sourceCursors: map[string]Cursor{"app.log": cur2}},
		{batch: b3, sourceCursors: map[string]Cursor{"app.log": cur3}},
	}

	// OVERLOADED on the first outstanding batch.
	s.processAck(&logaggv1.Ack{BatchId: "b1", Code: logaggv1.AckCode_ACK_CODE_OVERLOADED})

	if len(s.pendingAcks) != 0 {
		t.Fatalf("pendingAcks = %d, want 0 — every outstanding batch must be demoted together", len(s.pendingAcks))
	}
	if !s.sendPaused {
		t.Fatal("OVERLOADED must pause sending, not tear down the stream")
	}

	// A late ACCEPTED for the second batch — the collector already received
	// it before it decided to reject the first — must find nothing left in
	// pendingAcks to attribute it to and be ignored as a stray ack.
	s.processAck(&logaggv1.Ack{BatchId: "b2", Code: logaggv1.AckCode_ACK_CODE_ACCEPTED})

	if _, ok := s.checkpoint.Get("app.log"); ok {
		t.Fatal("checkpoint must not advance past the first (spooled, unacked) batch's lines")
	}

	if got, want := s.spool.Len(), 3; got != want {
		t.Fatalf("spool.Len() = %d, want %d — all three outstanding batches must have been spooled", got, want)
	}
	for i, want := range []string{"one", "two", "three"} {
		payload, ok, err := s.spool.Peek()
		if err != nil || !ok {
			t.Fatalf("Peek(%d): ok=%v err=%v", i, ok, err)
		}
		got := new(logaggv1.LogBatch)
		if err := proto.Unmarshal(payload, got); err != nil {
			t.Fatalf("unmarshal spooled batch %d: %v", i, err)
		}
		if len(got.GetRecords()) != 1 || got.GetRecords()[0].GetMessage() != want {
			t.Fatalf("spooled batch %d message = %q, want %q — original send order must be preserved", i, got.GetRecords()[0].GetMessage(), want)
		}
		if err := s.spool.Release(); err != nil {
			t.Fatalf("Release(%d): %v", i, err)
		}
	}
}

// TestShipper_OversizedRecordDroppedWithNonEmptyAccumulator is the
// regression test for skipping the "cannot fit even alone" guard when a
// record forces a flush of a non-empty accumulator. A small record
// accumulates first; a huge one that cannot fit even in a brand-new, empty
// accumulator arrives next, forcing that flush — and must then be dropped
// and counted as record_too_large rather than riding along into the fresh
// accumulator unchecked.
func TestShipper_OversizedRecordDroppedWithNonEmptyAccumulator(t *testing.T) {
	stub := &stubServer{}
	addr, _ := startStubServer(t, stub, "127.0.0.1:0")

	cfg := newTestShipperConfig(t, addr)
	cfg.MaxBatchRecords = 1000
	cfg.MaxBatchDelay = 5 * time.Second // only the size trigger should matter here
	cfg.MaxBatchBytes = 300
	s := newTestShipper(t, cfg)

	in := make(chan Line, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()

	huge := strings.Repeat("x", 1000)
	in <- testLine("app.log", "small", 0)
	in <- testLine("app.log", huge, 10)
	close(in)

	waitForRun(t, done, 2*time.Second)

	got := stub.receivedBatches()
	for _, b := range got {
		if sz := proto.Size(b); sz > cfg.MaxBatchBytes {
			t.Fatalf("batch marshaled to %d bytes, want <= %d — an oversized record must never ship", sz, cfg.MaxBatchBytes)
		}
		for _, r := range b.GetRecords() {
			if r.GetMessage() == huge {
				t.Fatalf("the oversized record was shipped instead of being dropped: %+v", b)
			}
		}
	}
	if n := counterValue(t, s.metrics.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonRecordTooLarge)); n != 1 {
		t.Fatalf("record_too_large drop count = %v, want 1", n)
	}
}
