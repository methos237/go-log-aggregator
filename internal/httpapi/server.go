// Package httpapi serves the public HTTP surface: health probes now, query and
// live tail in later phases.
//
// Metrics and pprof deliberately live on the separate admin listener in
// internal/observability, so nothing here needs authentication yet.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// Server is the public API listener.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// New builds the public server. Handlers for /v1/* arrive in phases 4 and 6.
func New(cfg config.HTTP, health *observability.Health, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", health.ReadyHandler())
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

// Addr reports the configured listen address.
func (s *Server) Addr() string { return s.srv.Addr }

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
