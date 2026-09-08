package ingest

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
)

func testIngestConfig() config.Ingest {
	// Port 0 so tests never collide with each other or with a running dev stack.
	return config.Ingest{Addr: "127.0.0.1:0", MaxRecvMsgBytes: 4 << 20, BufferSize: 64}
}

// start brings up a server on an ephemeral port and returns a client for it.
func start(t *testing.T) (*Server, logaggv1.LogServiceClient) {
	t.Helper()

	srv, err := New(context.Background(), testIngestConfig(), slog.New(slog.DiscardHandler))
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
	// The port answering is the whole subtask. Until the handler lands, the
	// registered service reports Unimplemented rather than refusing the connection.
	//
	// Send's error is deliberately ignored: gRPC reports a stream the server has
	// already finished as a bare EOF, and the real status only comes from Recv. That
	// race is exactly what happens here, because Unimplemented closes the stream
	// before the first batch is written.
	_ = stream.Send(&logaggv1.LogBatch{BatchId: "b1"})
	if _, err = stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Recv returned %v (code %s), want Unimplemented", err, status.Code(err))
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

	if _, err = New(context.Background(), cfg, nil); err == nil {
		t.Fatal("New succeeded on an address already in use")
	}
}

// GracefulStop has no deadline of its own, and a bidirectional stream stays open
// until the client closes it. Without the bounded wait plus hard Stop, an idle
// agent would hold shutdown open forever.
func TestShutdownGivesUpOnAStuckStream(t *testing.T) {
	t.Parallel()

	srv, err := New(context.Background(), testIngestConfig(), slog.New(slog.DiscardHandler))
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
