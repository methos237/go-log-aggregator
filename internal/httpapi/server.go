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

// New builds the public server. db answers /v1 queries; the tail endpoint
// arrives in phase 6.
func New(cfg *config.HTTP, health *observability.Health, db executor.Querier, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", health.ReadyHandler())

	api := &queryAPI{db: db, timeout: cfg.QueryTimeout, maxRows: cfg.QueryMaxRows, log: log}
	auth := bearerAuth(cfg.AuthToken)
	mux.Handle("POST /v1/query", auth(http.HandlerFunc(api.query)))
	mux.Handle("GET /v1/labels", auth(http.HandlerFunc(api.labels)))
	mux.Handle("GET /v1/labels/{name}/values", auth(http.HandlerFunc(api.labelValues)))
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
