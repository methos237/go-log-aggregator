// Command collector is the log aggregator server.
//
// One binary runs every server-side role: ingest (gRPC), query (HTTP), and
// cluster membership. Phase 0 wires configuration, logging, metrics, health
// probes and graceful shutdown; the ingest, storage, query and cluster
// subsystems land in later phases.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/httpapi"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	healthcheck := flag.Bool("healthcheck", false, "probe this process's own /readyz and exit 0 or 1")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String("collector"))
		return
	}

	// The container image is distroless: no shell, no curl. So the binary doubles
	// as its own container healthcheck.
	if *healthcheck {
		if err := probeSelf(); err != nil {
			fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		// The logger may not exist yet when configuration fails, so this path
		// writes to stderr directly.
		fmt.Fprintf(os.Stderr, "collector: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := observability.NewLogger(cfg.Log, os.Stdout, cfg.Node.Name)
	metrics := observability.NewMetrics(cfg.Node.Name)
	health := observability.NewHealth(2 * time.Second)

	// Signals are trapped before anything starts listening, so a Ctrl-C during
	// startup still shuts down cleanly instead of killing the process mid-init.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("collector starting",
		slog.String("env", cfg.Node.Env),
		slog.String("commit", version.Commit),
		slog.String("http_addr", cfg.HTTP.Addr),
		slog.String("admin_addr", cfg.Admin.Addr),
		slog.Bool("cluster_enabled", cfg.Cluster.Enabled),
	)

	// Phase 1 replaces this with a real TimescaleDB ping, phase 2 adds a
	// JetStream check. Registering a placeholder now keeps /readyz meaningful:
	// it reports ready only once every registered dependency answers.
	health.Register("self", func(context.Context) error { return nil })

	apiSrv := httpapi.New(cfg.HTTP, health, log)
	adminSrv := observability.NewAdminServer(cfg.Admin, metrics, log)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if serveErr := apiSrv.ListenAndServe(); serveErr != nil {
			return fmt.Errorf("http server: %w", serveErr)
		}
		return nil
	})
	g.Go(func() error {
		if serveErr := adminSrv.ListenAndServe(); serveErr != nil {
			return fmt.Errorf("admin server: %w", serveErr)
		}
		return nil
	})

	// Shutdown is driven by whichever comes first: a signal, or a listener
	// failing. gctx covers both, so a port already in use does not leave the
	// process half-running.
	g.Go(func() error {
		<-gctx.Done()

		// Detached from gctx on purpose: gctx is already canceled here, so
		// passing it to Shutdown would abort in-flight requests immediately
		// instead of draining them.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), cfg.Node.ShutdownTimeout)
		defer cancel()

		log.Info("shutting down", slog.Duration("timeout", cfg.Node.ShutdownTimeout))

		// Stop serving new public traffic first, then tear down admin, so metrics
		// and profiles stay scrapeable while the API drains.
		var errs []error
		if apiErr := apiSrv.Shutdown(shutdownCtx); apiErr != nil {
			errs = append(errs, fmt.Errorf("http shutdown: %w", apiErr))
		}
		if adminErr := adminSrv.Shutdown(shutdownCtx); adminErr != nil {
			errs = append(errs, fmt.Errorf("admin shutdown: %w", adminErr))
		}
		return errors.Join(errs...)
	})

	err = g.Wait()
	// A signal-driven exit surfaces as context.Canceled from gctx; that is the
	// success path, not a failure.
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("collector stopped with error", slog.Any("error", err))
		return err
	}

	log.Info("collector stopped")
	return nil
}
