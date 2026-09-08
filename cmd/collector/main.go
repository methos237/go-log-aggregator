// Command collector is the log aggregator server.
//
// One binary runs every server-side role: ingest (gRPC), query (HTTP), and
// cluster membership. Phase 0 wired configuration, logging, metrics, health
// probes and graceful shutdown; phase 1 adds the schema and the write path. The
// ingest, query and cluster subsystems land in later phases.
//
// The binary also doubles as the migration tool (-migrate and friends) and as its
// own container healthcheck (-healthcheck), because the image is distroless and
// has no shell to run either from.
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
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/storage"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	healthcheck := flag.Bool("healthcheck", false, "probe this process's own /readyz and exit 0 or 1")
	migrateUp := flag.Bool("migrate", false, "apply pending database migrations and exit")
	migrateDown := flag.Bool("migrate-down", false, "revert every database migration and exit (destroys all data)")
	migrateStatus := flag.Bool("migrate-status", false, "print the applied schema version and exit")
	// Migrations are often run from the host or from CI, where the compose-internal
	// hostname in the default DSN does not resolve.
	dbDSN := flag.String("db-dsn", "", "override the configured database DSN")
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

	if *migrateUp || *migrateDown || *migrateStatus {
		if err := runMigrations(*migrateUp, *migrateDown, *migrateStatus, *dbDSN); err != nil {
			fmt.Fprintf(os.Stderr, "collector: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := run(*dbDSN); err != nil {
		// The logger may not exist yet when configuration fails, so this path
		// writes to stderr directly.
		fmt.Fprintf(os.Stderr, "collector: %v\n", err)
		os.Exit(1)
	}
}

// runMigrations handles the -migrate family of flags. Exactly one runs per
// invocation; the caller has already established that at least one is set.
func runMigrations(up, down, status bool, dsnOverride string) error {
	cfg, err := loadConfig(dsnOverride)
	if err != nil {
		return err
	}
	log := observability.NewLogger(cfg.Log, os.Stdout, cfg.Node.Name)

	// No signal handling here on purpose: golang-migrate holds an advisory lock and
	// interrupting a migration midway is how a schema ends up marked dirty. These
	// commands are short.
	ctx := context.Background()

	switch {
	case status:
		version, dirty, verr := storage.SchemaVersion(ctx, cfg.DB.DSN)
		if verr != nil {
			return fmt.Errorf("schema status: %w", verr)
		}
		fmt.Printf("schema version %d (dirty=%t)\n", version, dirty)
		return nil
	case down:
		return storage.MigrateDown(ctx, cfg.DB.DSN, log)
	case up:
		return storage.Migrate(ctx, cfg.DB.DSN, log)
	default:
		return errors.New("no migration action requested")
	}
}

// loadConfig loads configuration and applies the DSN override, if any.
func loadConfig(dsnOverride string) (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if dsnOverride != "" {
		cfg.DB.DSN = dsnOverride
	}
	return cfg, nil
}

func run(dsnOverride string) error {
	cfg, err := loadConfig(dsnOverride)
	if err != nil {
		return err
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

	// Migrations run before the pool is used for anything else, and before the
	// listeners come up, so a node never serves traffic against a schema it does not
	// understand. Concurrent replicas are safe: golang-migrate takes an advisory
	// lock and the losers observe "already current".
	if cfg.DB.MigrateOnStart {
		if migErr := storage.Migrate(ctx, cfg.DB.DSN, log); migErr != nil {
			return fmt.Errorf("migrate schema: %w", migErr)
		}
	}

	pool, err := storage.Open(ctx, cfg.DB, "logagg-collector/"+cfg.Node.Name)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()
	metrics.Registerer.MustRegister(storage.NewPoolCollector(pool))

	// A dead database makes this node unready rather than dead: restarting would not
	// bring Postgres back, and phase 2's JetStream buffer is what absorbs the outage.
	health.Register("database", storage.HealthCheck(pool))

	// Started with no producer yet: the JetStream consumer that will call Submit
	// lands later in phase 2. Wiring it now means the pool sizing, the metric
	// registration and the drain-on-shutdown path are exercised by `make dev` and by
	// the compose smoke test before there is any ingest traffic to debug at the same
	// time.
	writer, err := storage.NewWriter(pool, cfg.Writer, storage.NewMetrics(metrics.Registerer), log)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	writer.Start(ctx)

	// The queue is connected before any listener comes up: a node that cannot reach
	// JetStream cannot durably accept a batch, so there is nothing useful for it to
	// serve. An outage *after* startup is different — that is a readiness failure
	// with automatic reconnection, not a reason to exit.
	q, err := queue.Connect(ctx, cfg.Queue, queue.NewMetrics(metrics.Registerer), log)
	if err != nil {
		return fmt.Errorf("connect queue: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Node.ShutdownTimeout)
		defer cancel()
		if closeErr := q.Close(closeCtx); closeErr != nil {
			log.Error("queue close failed", slog.Any("error", closeErr))
		}
	}()
	health.Register("queue", queue.HealthCheck(q))

	// /readyz reports ready only once every registered dependency answers.
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

		// Stop serving new public traffic first, then drain the writer, then tear
		// down admin. Order matters: the writer must flush after nothing new can
		// arrive, and metrics stay scrapeable throughout so the drain is observable.
		//
		// The dependency checks are deregistered up front so a shutting-down node
		// reports unready to a load balancer instead of failing requests it has
		// already stopped serving.
		var errs []error
		health.Deregister("database")
		health.Deregister("queue")
		if apiErr := apiSrv.Shutdown(shutdownCtx); apiErr != nil {
			errs = append(errs, fmt.Errorf("http shutdown: %w", apiErr))
		}
		if writerErr := writer.Close(shutdownCtx); writerErr != nil {
			errs = append(errs, fmt.Errorf("writer shutdown: %w", writerErr))
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
