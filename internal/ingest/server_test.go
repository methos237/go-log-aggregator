package ingest

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

func testIngestConfig() config.Ingest {
	// Port 0 so tests never collide with each other or with a running dev stack.
	return config.Ingest{Addr: "127.0.0.1:0", MaxRecvMsgBytes: 4 << 20, BufferSize: 64, PublishWorkers: 4}
}

// start brings up a server on an ephemeral port and returns a client for it.
func start(t *testing.T) (*Server, logaggv1.LogServiceClient) {
	t.Helper()
	return startWith(t, &queuetest.Publisher{}, NewMetrics(nil))
}

// startWith is start with a caller-supplied publisher and metrics, so a test can
// make the queue fail on command or assert on counters.
func startWith(t *testing.T, pub queue.Publisher, metrics *Metrics) (*Server, logaggv1.LogServiceClient) {
	t.Helper()
	return startCfg(t, testIngestConfig(), pub, metrics)
}

// startCfg is startWith plus a caller-supplied config, for tests that need a
// specific buffer or publisher count.
//
//nolint:gocritic // hugeParam: test helper
func startCfg(t *testing.T, cfg config.Ingest, pub queue.Publisher, metrics *Metrics) (*Server, logaggv1.LogServiceClient) {
	t.Helper()

	srv, err := New(context.Background(), cfg, pub, metrics, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", srv.Addr(), err)
	}

	t.Cleanup(func() {
		_ = conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutErr := srv.Shutdown(ctx); shutErr != nil {
			t.Errorf("Shutdown: %v", shutErr)
		}
		// Serve must report a clean stop as nil, or the errgroup in cmd/collector
		// would treat every graceful shutdown as a failure.
		if serveErr := <-served; serveErr != nil {
			t.Errorf("Serve returned %v, want nil after a clean shutdown", serveErr)
		}
	})

	return srv, logaggv1.NewLogServiceClient(conn)
}

func TestServerAnswers(t *testing.T) {
	t.Parallel()

	_, client := start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Stream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err = stream.Send(validBatch("b1", 1)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED {
		t.Fatalf("code = %s, detail %q, want ACCEPTED", ack.GetCode(), ack.GetDetail())
	}
}

func TestServerAddrIsResolved(t *testing.T) {
	t.Parallel()

	srv, _ := start(t)

	// Asking for port 0 and getting it back would make the address useless to a
	// test that needs to dial it.
	_, port, err := net.SplitHostPort(srv.Addr())
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", srv.Addr(), err)
	}
	if port == "0" || port == "" {
		t.Errorf("Addr() = %q, want a resolved port", srv.Addr())
	}
}

// A port conflict has to surface from New, before the caller registers health
// checks and starts serving, or it looks like a node that started and then died.
func TestNewFailsOnBusyPort(t *testing.T) {
	t.Parallel()

	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = held.Close() }()

	cfg := testIngestConfig()
	cfg.Addr = held.Addr().String()

	if _, err = New(context.Background(), cfg, &queuetest.Publisher{}, nil, nil); err == nil {
		t.Fatal("New succeeded on an address already in use")
	}
}

// GracefulStop has no deadline of its own, and a bidirectional stream stays open
// until the client closes it. Without the bounded wait plus hard Stop, an idle
// agent would hold shutdown open forever.
func TestShutdownGivesUpOnAStuckStream(t *testing.T) {
	t.Parallel()

	srv, err := New(context.Background(), testIngestConfig(), &queuetest.Publisher{}, NewMetrics(nil), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = logaggv1.NewLogServiceClient(conn).Stream(ctx); err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// Already expired: Shutdown must fall back to a hard stop immediately rather
	// than wait for a client that is not going to hang up.
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()

	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(expired) }()

	select {
	case shutErr := <-done:
		if shutErr == nil {
			t.Error("Shutdown reported a clean drain despite an open stream and an expired context")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown blocked on an open stream instead of forcing a stop")
	}

	if serveErr := <-served; serveErr != nil {
		t.Errorf("Serve returned %v, want nil after Stop", serveErr)
	}
}
