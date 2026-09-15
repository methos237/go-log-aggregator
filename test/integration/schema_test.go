//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/storage"
)

// The point of this file: a migration returning no error proves only that Postgres
// parsed it. Timescale's DDL is largely function calls whose effects live in a
// catalog, so "the hypertable exists", "the policies are attached" and "the
// aggregates are real" have to be read back explicitly.

func TestMigrationsCreateHypertable(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	var (
		numDimensions int
		compression   bool
	)
	err := pool.QueryRow(ctx, `
		SELECT num_dimensions, compression_enabled
		FROM timescaledb_information.hypertables
		WHERE hypertable_name = 'logs'`).Scan(&numDimensions, &compression)
	require.NoError(t, err, "logs is not a hypertable")

	// One dimension: time. Space partitioning is deliberately absent -- Timescale
	// advises against hash-partitioning a single-node hypertable, and compression
	// segmentby provides the per-stream locality instead.
	require.Equal(t, 1, numDimensions, "logs should be partitioned by time only")
	require.True(t, compression, "compression should be enabled by migration 0002")

	var interval time.Duration
	err = pool.QueryRow(ctx, `
		SELECT time_interval
		FROM timescaledb_information.dimensions
		WHERE hypertable_name = 'logs' AND dimension_number = 1`).Scan(&interval)
	require.NoError(t, err)
	require.Equal(t, time.Hour, interval, "chunk interval should be 1 hour")
}

func TestMigrationsCreateDedupIndex(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	var definition string
	err := pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE tablename = 'logs' AND indexname = 'logs_dedup'`).Scan(&definition)
	require.NoError(t, err, "logs_dedup index is missing")

	// The whole at-least-once story rests on this index being UNIQUE and including
	// the partitioning column, which Timescale requires.
	require.Contains(t, definition, "UNIQUE")
	require.Contains(t, definition, "stream_id")
	require.Contains(t, definition, "seq")
	require.Contains(t, definition, "\"time\"")

	var levelIndex string
	err = pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE tablename = 'logs' AND indexname = 'logs_time_level'`).Scan(&levelIndex)
	require.NoError(t, err, "logs_time_level index is missing")
	require.Contains(t, levelIndex, "DESC", "the time column should be indexed descending")
}

func TestMigrationsAttachPolicies(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	rows, err := pool.Query(ctx, `
		SELECT proc_name, hypertable_name, config::text
		FROM timescaledb_information.jobs
		WHERE hypertable_name IS NOT NULL
		ORDER BY proc_name`)
	require.NoError(t, err)
	defer rows.Close()

	type job struct{ table, config string }
	jobs := map[string]job{}
	for rows.Next() {
		var name, table, cfg string
		require.NoError(t, rows.Scan(&name, &table, &cfg))
		jobs[name] = job{table: table, config: cfg}
	}
	require.NoError(t, rows.Err())

	compression, ok := jobs["policy_compression"]
	require.True(t, ok, "no compression policy is attached")
	require.Equal(t, "logs", compression.table)
	require.Contains(t, compression.config, "02:00:00", "compress_after should be 2 hours")

	retention, ok := jobs["policy_retention"]
	require.True(t, ok, "no retention policy is attached")
	require.Equal(t, "logs", retention.table)
	require.Contains(t, retention.config, "30 days")

	// Both aggregates share one proc_name, so count the refresh jobs separately.
	var refreshJobs int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.jobs
		WHERE proc_name = 'policy_refresh_continuous_aggregate'`).Scan(&refreshJobs)
	require.NoError(t, err)
	require.Equal(t, 2, refreshJobs, "both continuous aggregates should have a refresh policy")
}

func TestMigrationsCreateContinuousAggregates(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	rows, err := pool.Query(ctx, `
		SELECT view_name, materialized_only, finalized
		FROM timescaledb_information.continuous_aggregates
		ORDER BY view_name`)
	require.NoError(t, err)
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var name string
		var materializedOnly, finalized bool
		require.NoError(t, rows.Scan(&name, &materializedOnly, &finalized))

		// Real-time aggregation must be on. Timescale defaults materialized_only to
		// true since 2.13, which would make a query against logs_rate_1m silently omit
		// everything since the last refresh -- a minutes-wide hole at exactly the end
		// of the range a log query usually asks about.
		require.False(t, materializedOnly,
			"%s has real-time aggregation disabled; recent data would be missing", name)
		require.True(t, finalized, "%s should use the finalized (non-partial) format", name)
		seen[name] = true
	}
	require.NoError(t, rows.Err())

	require.True(t, seen["logs_rate_1m"], "logs_rate_1m is missing")
	require.True(t, seen["logs_rate_1h"], "logs_rate_1h is missing")
}

// TestMigrationsRoundTrip is why the down migrations exist. Reverting a schema with
// compressed chunks and hierarchical continuous aggregates has real ordering
// constraints, and the only way to know they are right is to run them.
func TestMigrationsRoundTrip(t *testing.T) {
	t.Parallel()

	dsn := newDatabase(t)
	ctx := testContext(t)
	log := testLogger(t)

	require.NoError(t, storage.Migrate(ctx, dsn, log), "first up")

	// Take the job scheduler out of the picture before rolling back; see quiesceJobs.
	pool, err := storage.Open(ctx, testDBConfig(dsn), "logagg-integration")
	require.NoError(t, err)
	quiesceJobs(ctx, t, pool)
	pool.Close()

	version, dirty, err := storage.SchemaVersion(ctx, dsn)
	require.NoError(t, err)
	require.False(t, dirty, "schema should not be dirty after a clean migration")
	require.Equal(t, uint(4), version, "expected four migrations to be applied")

	// Re-running must be a no-op, which is what makes MigrateOnStart safe on every
	// replica's boot.
	require.NoError(t, storage.Migrate(ctx, dsn, log), "second up should be a no-op")

	require.NoError(t, storage.MigrateDown(ctx, dsn, log), "down")

	pool, err = storage.Open(ctx, testDBConfig(dsn), "logagg-integration")
	require.NoError(t, err)
	defer pool.Close()

	var tables int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('logs', 'streams')`).Scan(&tables))
	require.Zero(t, tables, "down migrations left tables behind")

	var aggregates int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.continuous_aggregates`).Scan(&aggregates))
	require.Zero(t, aggregates, "down migrations left continuous aggregates behind")

	var jobs int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_name IS NOT NULL`).Scan(&jobs))
	require.Zero(t, jobs, "down migrations left policies behind")

	require.NoError(t, storage.Migrate(ctx, dsn, log), "up again after down")
}

// TestMigrationsDownAfterCompression covers the ordering the 0002 down migration has
// to get right: compression cannot be disabled while any chunk is still compressed,
// so every chunk must be decompressed first.
func TestMigrationsDownAfterCompression(t *testing.T) {
	t.Parallel()

	pool, dsn := migratedDB(t)
	ctx := testContext(t)

	seedStream(ctx, t, pool, 1, "api", "node-1", "prod")
	insertRows(ctx, t, pool, 1, time.Now().Add(-6*time.Hour), 500)
	compressAllChunks(ctx, t, pool)

	var compressed int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.chunks
		WHERE hypertable_name = 'logs' AND is_compressed`).Scan(&compressed))
	require.Positive(t, compressed, "expected at least one compressed chunk to test against")

	pool.Close()
	require.NoError(t, storage.MigrateDown(ctx, dsn, testLogger(t)),
		"down migration must decompress chunks before disabling compression")
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// The compression collector reads the columnstore catalog on every scrape; it
// must gather cleanly on a freshly migrated database where no chunk has been
// compressed yet, and report a zero ratio rather than a gap.
func TestCompressionCollectorGathersOnFreshDatabase(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)

	reg := prometheus.NewRegistry()
	reg.MustRegister(storage.NewCompressionCollector(pool, testLogger(t)))
	families, err := reg.Gather()
	require.NoError(t, err)

	got := map[string]float64{}
	for _, f := range families {
		got[f.GetName()] = f.GetMetric()[0].GetGauge().GetValue()
	}
	require.Contains(t, got, "logagg_storage_compression_ratio")
	require.Contains(t, got, "logagg_storage_compressed_bytes")
	require.Equal(t, 0.0, got["logagg_storage_compression_ratio"], "no compressed chunk yet, so the ratio must read 0")
}
