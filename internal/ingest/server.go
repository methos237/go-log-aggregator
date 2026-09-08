// Package ingest is the gRPC front door: agents stream batches in, records are
// validated and fingerprinted, and the batch is published to the queue before the
// agent is told it is safe.
//
// The ordering in that last sentence is the whole correctness argument. An ack
// means "this batch is durable, you may drop it from your spool", so sending one
// before JetStream has acknowledged the publish would turn a collector crash into
// data loss that nothing can detect. Everything in this package is arranged around
// keeping the ack last.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

// Server is the gRPC listener that agents stream into, plus the bounded pipeline
// that carries their batches to the queue.
type Server struct {
	grpc     *grpc.Server
	lis      net.Listener
	pipeline *pipeline
	mtls     bool
	log      *slog.Logger
}

// service implements logaggv1.LogServiceServer. The Stream handler lives in
// handler.go; the embedded Unimplemented type keeps this compiling when the proto
// gains a method, answering Unimplemented instead of failing the build.
type service struct {
	logaggv1.UnimplementedLogServiceServer

	pipeline *pipeline
	log      *slog.Logger
	metrics  *Metrics
	now      nowFunc
}

// New binds the ingest listener and registers the service.
//
// Binding here rather than in Serve is deliberate: a port that is already in use
// is a startup error, and reporting it before the process registers health checks
// and starts draining is the difference between "failed to start" and "started,
// then mysteriously shut down".
//
// ctx bounds binding the socket only, not the listener's lifetime; Shutdown is
// what stops serving. metrics may be nil, which builds unregistered ones.
//
//nolint:gocritic // hugeParam: one copy per process; by value keeps it immutable
func New(ctx context.Context, cfg config.Ingest, q queue.Publisher, metrics *Metrics, log *slog.Logger) (*Server, error) {
	if q == nil {
		return nil, errors.New("ingest needs a queue publisher")
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With(slog.String("component", "ingest"))

	// Certificates are loaded before anything is bound or started, so a bad TLS
	// configuration leaves nothing to unwind.
	opts, err := serverOptions(cfg)
	if err != nil {
		return nil, err
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	// Publishers start here rather than in Serve. Starting them in Serve would let a
	// Shutdown that arrives first wait on a WaitGroup another goroutine is still
	// adding to, which is a data race and, worse, an occasional missed drain.
	pipe := newPipeline(ctx, cfg, q, metrics, log, time.Now)
	pipe.start()

	srv := grpc.NewServer(opts...)
	logaggv1.RegisterLogServiceServer(srv, &service{
		pipeline: pipe,
		log:      log,
		metrics:  metrics,
		now:      time.Now,
	})

	return &Server{grpc: srv, lis: lis, pipeline: pipe, mtls: cfg.TLSCertFile != "", log: log}, nil
}

// serverOptions is the server's resource envelope.
//
// MaxRecvMsgSize is the first line of defense on an untrusted boundary: agents
// batch, so it bounds how much memory one unauthenticated peer can make this
// process allocate for a single message. gRPC's own default is 4MB, but leaving it
// implicit would mean the limit silently changed with a dependency bump.
//
// Transport security is part of this envelope: an unauthenticated peer should not
// get as far as allocating a message.
//
//nolint:gocritic // hugeParam: called once per process
func serverOptions(cfg config.Ingest) ([]grpc.ServerOption, error) {
	creds, err := transportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgBytes),
		creds,
	}, nil
}

// Addr reports the address actually bound, which is what a test that asked for
// port 0 needs.
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Serve blocks until the server stops. A clean Shutdown returns nil.
func (s *Server) Serve() error {
	// mTLS state is logged because "why is this agent being rejected" and "why is
	// this port answering in the clear" are both answered by this one line.
	s.log.Info("ingest server listening",
		slog.String("addr", s.Addr()),
		slog.Bool("mtls", s.mtls),
	)
	if err := s.grpc.Serve(s.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// Shutdown stops accepting new streams and waits for in-flight ones, giving up
// when ctx expires.
//
// GracefulStop on its own has no deadline, and a bidirectional stream is held open
// by the client rather than by a request that ends on its own — an idle agent that
// never closes its stream would block shutdown forever. So the wait is bounded and
// the fallback is a hard Stop, which kills the remaining streams. That is safe by
// construction: a batch that has not been acked yet is one the agent will resend.
// The pipeline is closed only after the handlers are gone, and that order is not
// interchangeable: a handler waiting for its publish to complete would otherwise be
// waiting on publishers that had already exited.
func (s *Server) Shutdown(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	var errs []error
	select {
	case <-stopped:
		s.log.Info("ingest server stopped")
	case <-ctx.Done():
		s.grpc.Stop()
		<-stopped
		errs = append(errs, fmt.Errorf("ingest server did not drain in time: %w", ctx.Err()))
	}

	if err := s.pipeline.close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("ingest pipeline: %w", err))
	}
	return errors.Join(errs...)
}
