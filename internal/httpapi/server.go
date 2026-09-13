// Package httpapi serves the public HTTP surface: health probes, the query API
// under /v1 behind a bearer token, and live tail in a later phase.
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
)

// Server is the public API listener.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// Deps are the collaborators the /v1 handlers call. Cluster may be nil when
// clustering is off; Runner defaults to running every query on DB alone.
type Deps struct {
	DB      executor.Querier
	Runner  executor.Runner
	Cluster ClusterView
	// Node is this process's name, shown by /v1/cluster.
	Node string
}

// New builds the public server. The tail endpoint arrives in phase 6.
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
		log: log,
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
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
