//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// Direct-SQL helpers. Tests that are about the schema use these instead of the
// writer, so a schema failure cannot be masked by a writer bug and vice versa.

func seedStream(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id model.StreamID, service, host, env string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO streams (stream_id, service, host, env, labels)
		VALUES ($1, $2, $3, $4, '{}'::jsonb)
		ON CONFLICT (stream_id) DO NOTHING`, int64(id), service, host, env)
	require.NoError(t, err, "seed stream %d", id)
}

// insertRows writes count rows at one-second intervals starting at start.
func insertRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id model.StreamID, start time.Time, count int) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO logs (time, stream_id, seq, level, message)
		SELECT $2::timestamptz + (n || ' seconds')::interval, $1, n, 3, 'row ' || n
		FROM generate_series(0, $3 - 1) AS n`, int64(id), start, count)
	require.NoError(t, err, "insert %d rows", count)
}

// quiesceJobs stops TimescaleDB's background scheduler from running this database's
// compression, retention and refresh policies.
//
// This is not papering over a product bug, it removes a race between the test and the
// scheduler. Tests here invoke run_job and refresh_continuous_aggregate directly so
// the assertions are deterministic rather than dependent on a background schedule.
// When the scheduler runs the same job concurrently, both attempts fail on the chunks
// the other already converted -- Timescale reports "columnstore policy failure" with
// "chunk is already compressed" in the detail. The scheduler also competes for
// advisory locks with a schema rollback, which deadlocks the down migrations.
//
// What is still under test: the policy procedures themselves, and that they are
// attached with the right configuration. Whether Timescale's scheduler fires on time
// is Timescale's business, not this project's.
//
// stop_background_workers rather than alter_job(scheduled => false): in Timescale
// 2.22 alter_job on the refresh policy of a hierarchical continuous aggregate fails
// with "multiple refresh policies are not supported for hierarchical continuous
// aggregates" -- see the note in migrations/0003. Stopping the per-database scheduler
// is both simpler and closer to what is actually wanted. The function lives in an
// internal schema, which is acceptable in a test helper and nowhere else; it is the
// same mechanism timescaledb_pre_restore() uses.
func quiesceJobs(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, "SELECT _timescaledb_functions.stop_background_workers()")
	require.NoError(t, err, "stop background job scheduling")
}

// compressAllChunks runs the compression policy's job immediately instead of waiting
// for the scheduler.
//
// run_job cannot execute inside a transaction, so it needs its own connection with
// autocommit -- which pool.Exec gives, as long as nothing wraps it in a Tx.
func compressAllChunks(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	var jobID int
	err := pool.QueryRow(ctx, `
		SELECT job_id FROM timescaledb_information.jobs
		WHERE proc_name = 'policy_compression' AND hypertable_name = 'logs'`).Scan(&jobID)
	require.NoError(t, err, "find the compression job")

	_, err = pool.Exec(ctx, fmt.Sprintf("CALL run_job(%d)", jobID))
	require.NoError(t, err, "run compression job %d", jobID)
}

// refreshAggregate materializes a continuous aggregate over the given window.
//
// The refresh policy would get there on its own schedule; calling it directly is what
// keeps the test deterministic instead of sleeping for a minute.
func refreshAggregate(ctx context.Context, t *testing.T, pool *pgxpool.Pool, view string, from, to time.Time) {
	t.Helper()
	// The casts are required: CALL gives the planner no context for an untyped
	// parameter, so without them Postgres reports
	// "could not determine data type of parameter $1".
	_, err := pool.Exec(ctx,
		fmt.Sprintf("CALL refresh_continuous_aggregate('%s', $1::timestamptz, $2::timestamptz)", view), from, to)
	require.NoError(t, err, "refresh %s", view)
}

func countLogs(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM logs").Scan(&n))
	return n
}

func countStreams(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM streams").Scan(&n))
	return n
}
