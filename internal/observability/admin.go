package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

// AdminServer serves metrics and profiling on the internal port.
//
// This listener must never be published outside the cluster network: pprof
// exposes heap contents and allows an unauthenticated caller to trigger a CPU
// profile, which is both an information leak and a cheap way to slow the process
// down. config.Validate enforces that it does not share the public port.
type AdminServer struct {
	srv *http.Server
	log *slog.Logger
}

// NewAdminServer wires /metrics and, when enabled, /debug/pprof/*.
func NewAdminServer(cfg config.Admin, m *Metrics, log *slog.Logger) *AdminServer {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		// Surface scrape-time collector errors as HTTP 500 rather than serving a
		// silently truncated response, so a broken collector is visible as a
		// scrape failure instead of a metric that quietly disappears.
		ErrorHandling: promhttp.HTTPErrorOnError,
		ErrorLog:      slog.NewLogLogger(log.Handler(), slog.LevelError),
	}))

	if cfg.EnablePprof {
		// Registered explicitly instead of relying on net/http/pprof's init()
		// side effect on DefaultServeMux, which would attach pprof to any server
		// that happens to use the default mux — including the public one.
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	return &AdminServer{
		srv: &http.Server{
			Addr:    cfg.Addr,
			Handler: mux,
			// No WriteTimeout: a 30s CPU profile or execution trace is a long,
			// legitimate response and a write deadline would truncate it.
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		log: log,
	}
}

// ListenAndServe blocks until the server stops. A clean Shutdown returns nil.
func (a *AdminServer) ListenAndServe() error {
	a.log.Info("admin server listening", slog.String("addr", a.srv.Addr))
	err := a.srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting connections and waits for in-flight requests.
func (a *AdminServer) Shutdown(ctx context.Context) error {
	return a.srv.Shutdown(ctx)
}
