package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
)

// The client is what loadgen, logctl and the phase 3 agent all use, so the round
// trip through the real server is worth asserting here rather than only in each tool.
func TestClientSendsBatchesToARealServer(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	srv, _ := startWith(t, pub, NewMetrics(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := Dial(ctx, ClientConfig{Addr: srv.Addr(), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	ack, err := client.SendBatch(ctx, validBatch("b1", 3))
	if err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if ack.GetCode() != logaggv1.AckCode_ACK_CODE_ACCEPTED || ack.GetAccepted() != 3 {
		t.Fatalf("ack = %v, want ACCEPTED with 3 accepted", ack)
	}
	if pub.Count() != 1 {
		t.Errorf("published %d batches, want 1", pub.Count())
	}
}

// Many batches on one stream, sent and acked concurrently, is the shape loadgen
// uses; a sequential client would deadlock once the ack window filled.
func TestClientStreamsManyBatches(t *testing.T) {
	t.Parallel()

	pub := &queuetest.Publisher{}
	srv, _ := startWith(t, pub, NewMetrics(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := Dial(ctx, ClientConfig{Addr: srv.Addr(), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	stream, err := client.Stream(ctx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	const batches = 32
	acked := make(chan int, 1)
	go func() {
		count := 0
		for {
			if _, recvErr := stream.Recv(); recvErr != nil {
				acked <- count
				return
			}
			count++
		}
	}()

	for i := 0; i < batches; i++ {
		if err = stream.Send(validBatch("b", 2)); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if err = stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	if got := <-acked; got != batches {
		t.Errorf("received %d acks, want %d", got, batches)
	}
}

// Half-configured client TLS cannot reach an mTLS listener, so it is reported as a
// configuration mistake rather than left to fail as a handshake error.
func TestClientRejectsPartialTLS(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "some.pem")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	for name, cfg := range map[string]ClientConfig{
		"cert only":    {Addr: "127.0.0.1:1", CertFile: file},
		"cert and key": {Addr: "127.0.0.1:1", CertFile: file, KeyFile: file},
		"ca only":      {Addr: "127.0.0.1:1", CAFile: file},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Dial(context.Background(), cfg); !errors.Is(err, ErrPartialClientTLS) {
				t.Fatalf("err = %v, want ErrPartialClientTLS", err)
			}
		})
	}
}

func TestClientNeedsAnAddress(t *testing.T) {
	t.Parallel()

	if _, err := Dial(context.Background(), ClientConfig{}); err == nil {
		t.Fatal("Dial accepted an empty address")
	}
}

// A CLI must say "cannot connect" now rather than hang until its timeout.
func TestClientReportsAnUnreachableCollector(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started := time.Now()
	// Port 1 refuses immediately on every platform this runs on.
	if _, err := Dial(ctx, ClientConfig{Addr: "127.0.0.1:1", DialTimeout: 5 * time.Second}); err == nil {
		t.Fatal("Dial succeeded against a closed port")
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Errorf("Dial took %s; a refused connection should be reported at once", elapsed)
	}
}

// TestDialLazyDoesNotWaitForReadiness pins the one behavioral difference between
// DialLazy and Dial: an unreachable collector is an error for the CLI-facing
// Dial and not an error for the agent-facing DialLazy.
//
// This is worth a test rather than being left to the doc comment because the
// distinction is the whole reason DialLazy exists. An agent that inherited
// Dial's fail-fast behavior would refuse to start whenever it came up before its
// collector, which is the ordinary case in a compose stack.
func TestDialLazyDoesNotWaitForReadiness(t *testing.T) {
	// Port 1 on loopback: nothing listens, and connecting is refused promptly
	// rather than timing out, so Dial's failure is not just a slow test.
	const dead = "127.0.0.1:1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if c, err := Dial(ctx, ClientConfig{Addr: dead, DialTimeout: 2 * time.Second}); err == nil {
		_ = c.Close()
		t.Error("Dial() to an unreachable address returned no error; it is supposed to report transient failure")
	}

	start := time.Now()
	c, err := DialLazy(ClientConfig{Addr: dead})
	if err != nil {
		t.Fatalf("DialLazy() error = %v, want nil for an unreachable address", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("DialLazy() took %v; it must not wait for the connection to be usable", elapsed)
	}
}

func TestDialLazyRequiresAddress(t *testing.T) {
	if _, err := DialLazy(ClientConfig{}); err == nil {
		t.Error("DialLazy() with no address returned no error")
	}
}
