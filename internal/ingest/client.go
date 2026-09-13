package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/tlsx"
)

// The client lives in this package, next to the server, because both speak the same
// contract and that contract has invariants a caller can get wrong: batches on one
// stream must stay in order, and an ack refers to a specific batch ID. One place to
// read the protocol is worth more than a tidier dependency graph. The phase 3 agent
// wraps this rather than reimplementing it, as do loadgen and logctl.

// ClientConfig is how a sender reaches a collector.
type ClientConfig struct {
	// Addr is host:port of a collector's ingest listener.
	Addr string
	// TLS material. All three or none: a client that verifies the server without
	// presenting its own certificate cannot get past an mTLS listener, so the
	// half-configured case is a mistake worth reporting rather than a handshake
	// failure to debug.
	CertFile string
	KeyFile  string
	CAFile   string
	// DialTimeout bounds establishing the connection.
	DialTimeout time.Duration
}

// Client is a connection to one collector.
type Client struct {
	conn *grpc.ClientConn
}

// Dial connects to a collector and waits for the connection to be usable.
//
// Waiting is deliberate. gRPC connects lazily, so without this a bad address or a
// rejected handshake would surface much later as a confusing error on the first
// batch, and a load generator would report it as an ingest failure.
func Dial(ctx context.Context, cfg ClientConfig) (*Client, error) {
	conn, err := newConn(cfg)
	if err != nil {
		return nil, err
	}

	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn.Connect()
	if err = waitReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// DialLazy connects to a collector without waiting for the connection to become
// usable.
//
// This is the long-lived agent's counterpart to Dial, and the difference is
// deliberate rather than a convenience. Dial reports a transient failure instead
// of retrying, which is right for a CLI: a typo in the address should say so now.
// An agent started before its collector — the ordinary case in a compose stack or
// a rolling deploy — must not treat that as fatal, and an agent that re-dialed
// from scratch on every attempt would throw away gRPC's own connection
// management, including its backoff. So this returns immediately and lets the
// channel reconnect underneath; the caller retries opening the stream, spooling
// while it cannot.
//
// It takes no context because nothing here blocks. Connect only moves the channel
// out of idle so the first reconnect attempt starts now rather than on the first
// RPC; cancellation belongs on Stream, which is where the blocking actually
// happens.
func DialLazy(cfg ClientConfig) (*Client, error) {
	conn, err := newConn(cfg)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	return &Client{conn: conn}, nil
}

// newConn validates cfg and builds the channel, without connecting it.
func newConn(cfg ClientConfig) (*grpc.ClientConn, error) {
	if cfg.Addr == "" {
		return nil, errors.New("client needs a collector address")
	}

	creds, err := clientCredentials(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Addr, err)
	}
	return conn, nil
}

// waitReady blocks until the connection is usable, ctx expires, or the attempt
// fails.
//
// A transient failure is reported rather than retried. gRPC would keep retrying
// behind the scenes, which is right for a long-lived agent and wrong for a CLI: a
// typo in the address, or a plaintext client meeting an mTLS listener, should say so
// now instead of hanging until the timeout.
func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		switch state := conn.GetState(); state {
		case connectivity.Ready:
			return nil
		case connectivity.TransientFailure:
			return fmt.Errorf("connect to %s: connection failed", conn.Target())
		case connectivity.Shutdown:
			return fmt.Errorf("connect to %s: connection is shut down", conn.Target())
		case connectivity.Idle, connectivity.Connecting:
			if !conn.WaitForStateChange(ctx, state) {
				return fmt.Errorf("connect to %s: %w (last state %s)", conn.Target(), ctx.Err(), state)
			}
		}
	}
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Stream opens a bidirectional stream. Cancel ctx to abandon it.
func (c *Client) Stream(ctx context.Context) (*Stream, error) {
	s, err := logaggv1.NewLogServiceClient(c.conn).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}
	return &Stream{stream: s}, nil
}

// Stream is one open ingest stream.
//
// Send and Recv are safe to call from two goroutines — one sending, one receiving —
// which is the shape a pipelined sender wants, and is exactly what gRPC guarantees
// for a stream. Two senders are not safe, and would interleave batches on a
// connection whose ordering the ack protocol depends on.
type Stream struct {
	stream grpc.BidiStreamingClient[logaggv1.LogBatch, logaggv1.Ack]
}

// Send queues a batch. An error here is usually stale — the real reason arrives on
// Recv — so a caller that sees one should drain Recv before reporting.
func (s *Stream) Send(batch *logaggv1.LogBatch) error {
	return s.stream.Send(batch)
}

// Recv waits for the next ack.
func (s *Stream) Recv() (*logaggv1.Ack, error) {
	return s.stream.Recv()
}

// CloseSend says "no more batches", which is what makes the server's receive loop
// finish cleanly instead of the stream ending as a client that vanished.
func (s *Stream) CloseSend() error { return s.stream.CloseSend() }

// SendBatch is the one-shot form: open a stream, send one batch, read its ack.
func (c *Client) SendBatch(ctx context.Context, batch *logaggv1.LogBatch) (*logaggv1.Ack, error) {
	stream, err := c.Stream(ctx)
	if err != nil {
		return nil, err
	}
	// A send error is deliberately not returned: a stream the server has already
	// finished reports a bare EOF here, and the real status only comes from Recv.
	_ = stream.Send(batch)
	if err = stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close send: %w", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("await ack: %w", err)
	}
	return ack, nil
}

// clientCredentials builds transport security for the client side.
//
//nolint:gocritic // hugeParam: called once per process
func clientCredentials(cfg ClientConfig) (credentials.TransportCredentials, error) {
	if cfg.CertFile == "" && cfg.KeyFile == "" && cfg.CAFile == "" {
		return insecure.NewCredentials(), nil
	}
	return tlsx.Client(cfg.CertFile, cfg.KeyFile, cfg.CAFile)
}

// ErrPartialClientTLS is returned when only some of the client TLS files are set.
var ErrPartialClientTLS = tlsx.ErrPartial
