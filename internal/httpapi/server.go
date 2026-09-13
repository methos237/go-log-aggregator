// Package httpapi serves the public HTTP surface: health probes, and the query
// and live-tail APIs under /v1 behind a bearer token.
//
// Metrics and pprof deliberately live on the separate admin listener in
// internal/observability.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
	"github.com/jamespolk/go-log-aggregator/internal/tail"
)

// Server is the public API listener.
type Server struct {
	srv   *http.Server
	tails *tail.Registry
	log   *slog.Logger
}

// Deps are the collaborators the /v1 handlers call. Cluster may be nil when
// clustering is off; Runner defaults to running every query on DB alone.
type Deps struct {
	DB      executor.Querier
	Runner  executor.Runner
	Cluster ClusterView
	// Node is this process's name, shown by /v1/cluster.
	Node string
	// Tails serves /v1/tail. Nil leaves the route unregistered.
	Tails *tail.Registry
}

// New builds the public server.
func New(cfg *config.HTTP, health *observability.Health, deps Deps, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", health.ReadyHandler())

	runner := deps.Runner
	if runner == nil {
		runner = executor.Single{DB: deps.DB}
	}
	api := &queryAPI{db: deps.DB, run: runner, timeout: cfg.QueryTimeout, maxRows: cfg.QueryMaxRows, log: log, node: deps.Node, cluster: deps.Cluster}
	auth := bearerAuth(cfg.AuthToken)
	mux.Handle("POST /v1/query", auth(http.HandlerFunc(api.query)))
	mux.Handle("GET /v1/labels", auth(http.HandlerFunc(api.labels)))
	mux.Handle("GET /v1/labels/{name}/values", auth(http.HandlerFunc(api.labelValues)))
	mux.Handle("GET /v1/cluster", auth(http.HandlerFunc(api.clusterState)))
	if deps.Tails != nil {
		t := &tailAPI{reg: deps.Tails, ping: cfg.TailPingInterval, write: cfg.WriteTimeout, log: log}
		mux.Handle("GET /v1/tail", auth(http.HandlerFunc(t.tail)))
	}
	mux.HandleFunc("/", notFound)

	handler := recoverPanic(log)(requestLog(log)(mux))

	return &Server{
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		tails: deps.Tails,
		log:   log,
	}
}

// Handler exposes the routed handler for tests without binding a port.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// ListenAndServe blocks until the server stops. A clean Shutdown returns nil.
func (s *Server) ListenAndServe() error {
	s.log.Info("http server listening", slog.String("addr", s.srv.Addr))
	err := s.srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting connections and waits for in-flight requests.
//
// Tail connections are hijacked, so net/http neither tracks nor closes them;
// ending their subscriptions first is what makes each handler send its close
// frame and return.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.tails != nil {
		s.tails.Close()
	}
	return s.srv.Shutdown(ctx)
}
