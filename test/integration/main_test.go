//go:build integration

// Package integration exercises the storage layer against a real TimescaleDB.
//
// These tests are behind the `integration` build tag so `go test ./...` stays fast
// and Docker-free. Run them with `make test-integration`.
//
// Two ways to get a database, in order of preference at the call site:
//
//   - LOGAGG_TEST_DB_DSN points at an existing server (typically `make dev`). This is
//     the fast inner loop: no container start per run.
//   - Otherwise testcontainers-go starts one TimescaleDB container for the whole
//     package, which is what CI and a cold clone use.
//
// Either way each test gets its own freshly created database, so tests cannot see
// each other's rows and the migration test is free to tear the schema down.
package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/storage"
)

// timescaleImage is pinned by digest to the same image deploy/docker-compose.yml
// runs, so an integration pass says something about the stack that actually ships.
const timescaleImage = "timescale/timescaledb@sha256:fba60021a224479e174ae1ec577c1a0576d5185b09fe9e622f1d19e4bf5bab0d"

// adminDSN points at a database used only for CREATE DATABASE / DROP DATABASE.
var adminDSN string

// dbCounter keeps per-test database names unique without needing a lock.
var dbCounter atomic.Int64

func TestMain(m *testing.M) {
	code, err := runTests(m)
	if err != nil {
		log.Printf("integration setup: %v", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runTests(m *testing.M) (int, error) {
	// A generous ceiling: pulling the image on a cold machine dominates it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if dsn := os.Getenv("LOGAGG_TEST_DB_DSN"); dsn != "" {
		adminDSN = dsn
		return m.Run(), nil
	}

	container, err := postgres.Run(ctx, timescaleImage,
		postgres.WithDatabase("logagg_admin"),
		postgres.WithUsername("logagg"),
		postgres.WithPassword("logagg"),
		postgres.BasicWaitStrategies(),
	)
	// Deferred before the error check: Run can fail after the container is created,
	// and leaking a container per failed run fills a laptop's disk quickly.
	defer func() {
		if container == nil {
			return
		}
		if terr := testcontainers.TerminateContainer(container); terr != nil {
			log.Printf("terminating container: %v", terr)
		}
	}()
	if err != nil {
		return 0, fmt.Errorf("start timescaledb container: %w", err)
	}

	adminDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, fmt.Errorf("read container dsn: %w", err)
	}

	return m.Run(), nil
}

// newDatabase creates an empty database and returns a DSN for it. The database is
// dropped when the test finishes.
//
// Per-test databases rather than per-test schemas: Timescale's catalog of
// hypertables, policies and continuous aggregates is per database, and the
// assertions in these tests read that catalog. Sharing it would make them depend on
// execution order.
func newDatabase(t *testing.T) string {
	t.Helper()

	name := fmt.Sprintf("logagg_test_%d_%d", os.Getpid(), dbCounter.Add(1))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminDSN)
	require.NoError(t, err, "connect to the admin database")
	defer func() { _ = admin.Close(context.Background()) }()

	// The identifier is generated from a PID and a counter, never from test input, so
	// there is nothing here for a caller to inject. pgx cannot parameterize a database
	// name in DDL, which is why this is formatted rather than bound.
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err, "create test database %s", name)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()

		conn, cerr := pgx.Connect(cleanupCtx, adminDSN)
		if cerr != nil {
			t.Logf("cleanup: connect: %v", cerr)
			return
		}
		defer func() { _ = conn.Close(context.Background()) }()

		// FORCE terminates any connection the test left behind; without it a leaked
		// pool keeps the database alive and the next run collides on the name.
		if _, derr := conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); derr != nil {
			t.Logf("cleanup: drop database %s: %v", name, derr)
		}
	})

	return replaceDatabase(adminDSN, name)
}

// replaceDatabase rewrites the database component of a DSN.
//
// String surgery rather than net/url parsing because the DSN may be in either URL or
// keyword/value form; only the URL form is used here, but the keyword form is what an
// operator is likely to paste into LOGAGG_TEST_DB_DSN.
func replaceDatabase(dsn, name string) string {
	if strings.Contains(dsn, "dbname=") {
		fields := strings.Fields(dsn)
		for i, f := range fields {
			if strings.HasPrefix(f, "dbname=") {
				fields[i] = "dbname=" + name
			}
		}
		return strings.Join(fields, " ")
	}

	// URL form: replace the single path segment.
	scheme, rest, found := strings.Cut(dsn, "://")
	if !found {
		return dsn
	}
	hostAndPath, query, hasQuery := strings.Cut(rest, "?")
	host, _, hasPath := strings.Cut(hostAndPath, "/")
	if !hasPath {
		host = hostAndPath
	}
	out := scheme + "://" + host + "/" + name
	if hasQuery {
		out += "?" + query
	}
	return out
}

// migratedDB returns a pool against a freshly migrated database.
//
// Background job scheduling is switched off immediately, while the tables are still
// empty and no policy has anything to do. See quiesceJobs: tests drive the policies
// explicitly, and a scheduler running the same job at the same time makes both fail.
func migratedDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	dsn := newDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	require.NoError(t, storage.Migrate(ctx, dsn, testLogger(t)), "apply migrations")

	pool, err := storage.Open(ctx, testDBConfig(dsn), "logagg-integration")
	require.NoError(t, err, "open pool")
	t.Cleanup(pool.Close)

	quiesceJobs(ctx, t, pool)

	return pool, dsn
}

func testDBConfig(dsn string) config.DB {
	return config.DB{
		DSN:             dsn,
		MaxConns:        8,
		MinConns:        1,
		ConnMaxLifetime: 10 * time.Minute,
		ConnectTimeout:  30 * time.Second,
	}
}
