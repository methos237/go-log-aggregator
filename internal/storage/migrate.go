package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	// Registers the "pgx/v5" database/sql driver that golang-migrate needs.
	// pgxpool is not usable here: golang-migrate's API is defined over *sql.DB.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/jamespolk/go-log-aggregator/migrations"
)

// ErrNoChange means the schema was already at the target version. Callers
// generally treat it as success; it is exported so they can tell "nothing to do"
// from "migrated".
var ErrNoChange = migrate.ErrNoChange

// Migrate applies every pending migration.
//
// Safe to call from every collector at startup: golang-migrate takes a Postgres
// advisory lock, so with N replicas booting together one applies the migrations
// and the rest block, then observe ErrNoChange. That is why config.DB
// MigrateOnStart can default to true without a separate migration job.
func Migrate(ctx context.Context, dsn string, log *slog.Logger) error {
	return withMigrator(ctx, dsn, log, func(m *migrate.Migrate) error {
		before, _, _ := m.Version()
		err := m.Up()
		switch {
		case errors.Is(err, migrate.ErrNoChange):
			log.Debug("schema already current", slog.Uint64("version", uint64(before)))
			return nil
		case err != nil:
			return fmt.Errorf("apply migrations: %w", err)
		}
		after, _, _ := m.Version()
		log.Info("schema migrated",
			slog.Uint64("from_version", uint64(before)),
			slog.Uint64("to_version", uint64(after)),
		)
		return nil
	})
}

// MigrateDown reverses every migration, leaving an empty schema.
//
// Destructive and therefore not reachable from normal startup: it exists for
// `make migrate-down` and for the integration test that exercises up/down/up.
// A down migration nobody runs is a down migration that does not work.
func MigrateDown(ctx context.Context, dsn string, log *slog.Logger) error {
	return withMigrator(ctx, dsn, log, func(m *migrate.Migrate) error {
		if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("revert migrations: %w", err)
		}
		log.Info("schema reverted")
		return nil
	})
}

// SchemaVersion reports the applied version and whether the last migration left
// the schema dirty.
//
// Dirty means a migration failed part way and the recorded version is not
// trustworthy; golang-migrate refuses to proceed until a human forces a version,
// which is the correct behavior and worth surfacing rather than hiding.
func SchemaVersion(ctx context.Context, dsn string) (version uint, dirty bool, err error) {
	err = withMigrator(ctx, dsn, slog.New(slog.DiscardHandler), func(m *migrate.Migrate) error {
		v, d, verr := m.Version()
		if verr != nil && !errors.Is(verr, migrate.ErrNilVersion) {
			return fmt.Errorf("read schema version: %w", verr)
		}
		version, dirty = v, d
		return nil
	})
	return version, dirty, err
}

// withMigrator wires the embedded source and the pgx driver together, runs fn, and
// tears everything down.
//
// The *sql.DB is opened here and closed on return rather than being shared with
// the pgxpool: migrations run once at startup and hold an advisory lock, so
// borrowing a pooled connection for that long would take a writer connection out
// of service for the duration.
func withMigrator(ctx context.Context, dsn string, log *slog.Logger, fn func(*migrate.Migrate) error) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}

	db, err := sql.Open("pgx/v5", dsn)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			log.Warn("closing migration connection", slog.Any("error", cerr))
		}
	}()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		return fmt.Errorf("ping for migrations: %w", pingErr)
	}

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		return fmt.Errorf("create migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "pgx/v5", driver)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	m.Log = migrateLogger{log: log}

	// Each migration file is sent as one multi-statement query, which Postgres
	// wraps in an implicit transaction, so a failure rolls the whole file back.
	// That is why MultiStatementEnabled is left off -- and why the continuous
	// aggregates in 0003 must be created WITH NO DATA.
	if err := fn(m); err != nil {
		return err
	}

	// Only the source is closed: closing the migrator would also close the
	// driver's *sql.DB, which the deferred Close above already owns.
	if err := src.Close(); err != nil {
		return fmt.Errorf("close migration source: %w", err)
	}
	return nil
}

// migrateLogger bridges golang-migrate's logging interface to slog so migration
// output lands in the same structured stream as everything else.
type migrateLogger struct{ log *slog.Logger }

func (l migrateLogger) Printf(format string, v ...any) {
	// golang-migrate terminates its messages with a newline, which would end up
	// embedded in the JSON message field.
	l.log.Debug("migrate: " + strings.TrimRight(fmt.Sprintf(format, v...), "\n"))
}

// Verbose off: golang-migrate's verbose output is per-statement noise that
// duplicates what the from/to version log line already says.
func (l migrateLogger) Verbose() bool { return false }
