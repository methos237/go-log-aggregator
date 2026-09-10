//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/query"
)

// The unit tests pin the planner's SQL text; this file proves that text is
// valid against the real schema and that every argument type binds: the jsonb
// cast, the int16 level, the bigint[] id slot, timestamps and the limit.

const planGoldens = "../../internal/query/testdata/plan"

func TestQueryPlansExecute(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	req := query.Request{Start: start, End: start.Add(time.Hour), Limit: 100}

	paths, err := filepath.Glob(filepath.Join(planGoldens, "*.golden"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no golden files under %s", planGoldens)

	for _, path := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".golden"), func(t *testing.T) {
			plan := compilePlan(t, goldenQuery(t, path), req)

			ids := streamIDs(ctx, t, pool, plan)
			require.Empty(t, ids, "empty database should match no streams")

			// Any ids will do: the point is that the statement binds and runs.
			plan.Logs.Args[0] = []int64{1, 2, 3}
			rows, err := pool.Query(ctx, plan.Logs.SQL, plan.Logs.Args...)
			require.NoError(t, err, "logs statement: %s", plan.Logs.SQL)
			_, err = pgx.CollectRows(rows, pgx.RowToMap)
			require.NoError(t, err)
		})
	}

	t.Run("end_to_end", func(t *testing.T) {
		seedStream(ctx, t, pool, 1, "api", "web-1", "prod")
		seedStream(ctx, t, pool, 2, "web", "web-1", "prod")
		insertRows(ctx, t, pool, 1, start, 3)
		insertRows(ctx, t, pool, 2, start, 3)

		plan := compilePlan(t, `{service="api"}`, req)
		ids := streamIDs(ctx, t, pool, plan)
		require.Equal(t, []int64{1}, ids)

		plan.Logs.Args[0] = ids
		rows, err := pool.Query(ctx, plan.Logs.SQL, plan.Logs.Args...)
		require.NoError(t, err)
		got, err := pgx.CollectRows(rows, pgx.RowToMap)
		require.NoError(t, err)
		require.Len(t, got, 3)
		for i, r := range got {
			require.Equal(t, int64(1), r["stream_id"])
			require.Equal(t, int64(2-i), r["seq"], "rows should come back newest first")
			require.Equal(t, start.Add(time.Duration(2-i)*time.Second), r["time"].(time.Time).UTC())
		}
	})
}

// goldenQuery reads the query source from a golden file's header, so the
// integration cases are exactly the unit cases and cannot drift from them.
func goldenQuery(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.SplitN(string(data), "\n", 3)
	require.Len(t, lines, 3)
	require.Equal(t, "-- query", lines[0], "%s: unexpected golden header", path)
	return lines[1]
}

func compilePlan(t *testing.T, src string, req query.Request) *query.Plan {
	t.Helper()
	q, err := query.Parse(src)
	require.NoError(t, err, "parse %s", src)
	plan, err := query.Compile(q, req)
	require.NoError(t, err, "compile %s", src)
	return plan
}

func streamIDs(ctx context.Context, t *testing.T, pool *pgxpool.Pool, plan *query.Plan) []int64 {
	t.Helper()
	rows, err := pool.Query(ctx, plan.Streams.SQL, plan.Streams.Args...)
	require.NoError(t, err, "streams statement: %s", plan.Streams.SQL)
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(t, err)
	return ids
}
