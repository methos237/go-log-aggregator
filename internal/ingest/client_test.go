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
