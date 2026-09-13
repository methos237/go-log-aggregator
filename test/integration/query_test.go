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

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
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

		// Pipeline stages against the same stream: two structured rows (one
		// JSON, one logfmt) plus a malformed line that must be skipped, not
		// abort the query. seq 10 has fields the agent extracted at ingest.
		_, err = pool.Exec(ctx, `
			INSERT INTO logs (time, stream_id, seq, level, message, fields) VALUES
			($1, 1, 10, 4, '{"status": 503, "user": "root"}', '{"status": "503"}'),
			($1, 1, 11, 3, 'method=GET route="/v1/query" status=200', NULL),
			($1, 1, 12, 3, '{not json', NULL)`, start.Add(10*time.Second))
		require.NoError(t, err)
		for src, want := range map[string][]int64{
			`{service="api"} |= "ROW 1"`:                                     {1},
			`{service="api"} != "row"`:                                       {12, 11, 10},
			`{service="api"} |~ "^row [12]$"`:                                {2, 1},
			`{service="api"} | status >= 500`:                                {10},
			`{service="api"} | json | status >= 500`:                         {10},
			`{service="api"} | json | user = "root"`:                         {10},
			`{service="api"} | json | user != "root"`:                        {12, 11, 2, 1, 0},
			`{service="api"} | logfmt | route = "/v1/query"`:                 {11},
			`{service="api"} | logfmt | status < 300`:                        {11},
			`{service="api"} | regexp "^(?P<m>\\w+) (?P<n>\\d+)$" | n = "2"`: {2},
			`{service="api"} | level >= "warn"`:                              {10},
		} {
			plan := compilePlan(t, src, req)
			plan.Logs.Args[0] = ids
			rows, err := pool.Query(ctx, plan.Logs.SQL, plan.Logs.Args...)
			require.NoError(t, err, "%s: %s", src, plan.Logs.SQL)
			seqs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int64, error) {
				var seq int64
				return seq, row.Scan(nil, nil, &seq, nil, nil, nil, nil, nil)
			})
			require.NoError(t, err, src)
			require.Equal(t, want, seqs, src)
		}

		// Aggregations: the request is minute-aligned, so anything the
		// continuous aggregates can answer is routed to them and must agree
		// with the raw scan. All six stream-1 rows fall in the first bucket.
		aggregates := []struct {
			src, source string
			want        [][]any
		}{
			{`{service="api"} | count_over_time(1m)`, "logs_rate_1m", [][]any{{start, 6.0}}},
			{`{service="api"} | count_over_time(1h) by (level)`, "logs_rate_1h", [][]any{{start, 5.0, int16(3)}, {start, 1.0, int16(4)}}},
			{`{service="api"} | rate(1m) by (service, host)`, "logs_rate_1m", [][]any{{start, 0.1, "api", "web-1"}}},
			{`{service="api"} |= "row" | count_over_time(1m)`, "logs", [][]any{{start, 3.0}}},
			{`{service="api"} |= "row" | bytes_over_time(1m)`, "logs", [][]any{{start, 15.0}}},
			{`{service="api"} | json | count_over_time(1m) by (user)`, "logs", [][]any{{start, 1.0, "root"}, {start, 5.0, nil}}},
		}
		for _, tc := range aggregates {
			plan := compilePlan(t, tc.src, req)
			require.Equal(t, tc.source, plan.Source, tc.src)
			plan.Logs.Args[0] = ids
			rows, err := pool.Query(ctx, plan.Logs.SQL, plan.Logs.Args...)
			require.NoError(t, err, "%s: %s", tc.src, plan.Logs.SQL)
			got, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]any, error) {
				vals, err := row.Values()
				if err == nil {
					vals[0] = vals[0].(time.Time).UTC()
				}
				return vals, err
			})
			require.NoError(t, err, tc.src)
			require.Equal(t, tc.want, got, tc.src)
		}

		// The executor over the same data: both statements, the empty-set
		// short circuit, and every column and label type scanned.
		run := func(src string) *executor.Result {
			q, err := query.Parse(src)
			require.NoError(t, err, src)
			res, err := executor.Run(ctx, pool, q, req)
			require.NoError(t, err, src)
			require.Positive(t, res.Elapsed)
			return res
		}

		res := run(`{service="nope"}`)
		require.Equal(t, &executor.Result{Source: "logs", Elapsed: res.Elapsed}, res)

		res = run(`{service="api"} |= "row"`)
		require.Equal(t, 1, res.Streams)
		require.Len(t, res.Records, 3)
		require.Equal(t, model.LogRecord{StreamID: 1, Time: res.Records[0].Time, Seq: 2, Level: model.LevelInfo, Message: "row 2"}, res.Records[0])
		require.Equal(t, start.Add(2*time.Second), res.Records[0].Time.UTC())

		res = run(`{service="api"} | json | user = "root"`)
		require.Len(t, res.Records, 1)
		require.Equal(t, map[string]string{"status": "503"}, res.Records[0].Fields)
		require.Equal(t, model.LevelWarn, res.Records[0].Level)

		res = run(`{service="api"} | count_over_time(1h) by (level)`)
		require.Equal(t, "logs_rate_1h", res.Source)
		require.Nil(t, res.Records)
		require.Equal(t, []executor.Point{
			{Bucket: start, Value: 5, Labels: map[string]string{"level": "info"}},
			{Bucket: start, Value: 1, Labels: map[string]string{"level": "warn"}},
		}, utc(res.Points))

		res = run(`{service="api"} | json | count_over_time(1m) by (user)`)
		require.Equal(t, "logs", res.Source)
		require.Equal(t, []executor.Point{
			{Bucket: start, Value: 1, Labels: map[string]string{"user": "root"}},
			{Bucket: start, Value: 5, Labels: map[string]string{}},
		}, utc(res.Points))

		res = run(`{service="api"} | rate(1m)`)
		require.Equal(t, []executor.Point{{Bucket: start, Value: 0.1}}, utc(res.Points))
	})
}

func utc(points []executor.Point) []executor.Point {
	for i := range points {
		points[i].Bucket = points[i].Bucket.UTC()
	}
	return points
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
