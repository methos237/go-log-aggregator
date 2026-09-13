// Package executor runs a compiled query against the database and shapes the
// rows for the API: records for a log query, points for an aggregation, plus
// the source that answered and how long it took.
//
// It is the only code that hands query.Plan statements to pgx, so the
// two-statement contract lives here: resolve the stream set first, stop if it
// is empty, otherwise bind it into the logs statement's first argument.
package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
)

// Point is one bucket of one `by` group in an aggregation result.
type Point struct {
	Bucket time.Time `json:"bucket"`
	Value  float64   `json:"value"`
	// Labels are the `by` values keyed by label name. A group whose value was
	// NULL, such as a field the record did not have, has no key. Nil when the
	// aggregation had no `by`.
	Labels map[string]string `json:"labels,omitempty"`
}

// Result is a query's answer. Exactly one of Records and Points is populated,
// depending on whether the query ended in an aggregation; both are nil when
// the selector matched no stream.
type Result struct {
	Records []model.LogRecord
	Points  []Point
	// Source is the relation the planner read: "logs" or a continuous aggregate.
	Source string
	// Start and End are the range actually covered, which for an aggregation
	// is the request widened to whole buckets.
	Start, End time.Time
	// Streams is how many streams the selector matched.
	Streams int
	// Truncated reports that more rows matched than r.Limit allowed, so the
	// caller sees a prefix: the newest records, or the newest buckets of a
	// series, and must not read the result as complete.
	Truncated bool
	// Warnings are shards that could not be answered, when the query fanned
	// out across a cluster. Empty for a single-node query.
	Warnings []string
	// Elapsed covers both statements, from the first Query to the last row.
	Elapsed time.Duration
}

// Querier is the slice of pgx the executor uses. *pgxpool.Pool, *pgx.Conn and
// pgx.Tx all satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Shape says how to read a logs statement's rows: log records, or points
// grouped by the named labels.
type Shape struct {
	Aggregate bool
	By        []string
}

// ShapeOf is the shape of q's result.
func ShapeOf(q *query.Query) Shape {
	if q.Agg == nil {
		return Shape{}
	}
	return Shape{Aggregate: true, By: q.Agg.By}
}

// Runner answers a parsed query. *Single and the cluster coordinator both do.
type Runner interface {
	Run(ctx context.Context, q *query.Query, r query.Request) (*Result, error)
}

// Single runs every query against one database, the whole plan on this node.
type Single struct{ DB Querier }

// Run compiles q for r and executes it here. Deadlines and cancellation come
// from ctx; the row cap is r.Limit, which query.Compile requires.
func (s Single) Run(ctx context.Context, q *query.Query, r query.Request) (*Result, error) {
	return Run(ctx, s.DB, q, r)
}

// Run compiles q for r and executes it against db.
func Run(ctx context.Context, db Querier, q *query.Query, r query.Request) (*Result, error) {
	plan, limit, err := Plan(q, r)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	ids, err := ResolveStreams(ctx, db, plan)
	if err != nil {
		return nil, err
	}
	res := &Result{Source: plan.Source, Start: plan.Start, End: plan.End, Streams: len(ids)}
	if len(ids) > 0 {
		plan.Logs.Args[0] = ids
		if res.Records, res.Points, err = ExecLogs(ctx, db, plan.Logs, ShapeOf(q)); err != nil {
			return nil, err
		}
	}
	res.Truncate(limit)
	res.Elapsed = time.Since(started)
	return res, nil
}

// Plan compiles q for r with one row more than r.Limit, so truncation is a
// fact rather than a guess from len == limit, and returns the caller's limit
// to truncate to.
func Plan(q *query.Query, r query.Request) (*query.Plan, int, error) {
	limit := r.Limit
	if limit > 0 {
		r.Limit++
	}
	plan, err := query.Compile(q, r)
	return plan, limit, err
}

// ResolveStreams runs the plan's streams statement and returns the matching
// ids, the value that goes into Logs.Args[0].
func ResolveStreams(ctx context.Context, db Querier, plan *query.Plan) ([]int64, error) {
	rows, err := db.Query(ctx, plan.Streams.SQL, plan.Streams.Args...)
	if err != nil {
		return nil, fmt.Errorf("executor: resolve streams: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("executor: resolve streams: %w", err)
	}
	return ids, nil
}

// ExecLogs runs a logs statement whose id slot is already bound and scans its
// rows by shape. It is the unit of work a peer performs for a coordinator.
func ExecLogs(ctx context.Context, db Querier, logs query.Stmt, shape Shape) ([]model.LogRecord, []Point, error) {
	rows, err := db.Query(ctx, logs.SQL, logs.Args...)
	if err != nil {
		return nil, nil, fmt.Errorf("executor: query: %w", err)
	}
	if !shape.Aggregate {
		records, scanErr := pgx.CollectRows(rows, scanRecord)
		if scanErr != nil {
			return nil, nil, fmt.Errorf("executor: scan: %w", scanErr)
		}
		return records, nil, nil
	}
	points, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Point, error) {
		return scanPoint(row, shape.By)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("executor: scan: %w", err)
	}
	return nil, points, nil
}

// Truncate cuts the result to limit rows and records that it did.
func (r *Result) Truncate(limit int) {
	if len(r.Records) > limit {
		r.Records, r.Truncated = r.Records[:limit], true
	}
	if len(r.Points) > limit {
		r.Points, r.Truncated = r.Points[:limit], true
	}
}

// scanRecord reads the eight logs columns in table order, the shape
// query.Plan documents for a non-aggregate statement. Times come back in UTC
// rather than the connection's zone so every consumer renders them the same.
func scanRecord(row pgx.CollectableRow) (model.LogRecord, error) {
	var rec model.LogRecord
	err := row.Scan(&rec.Time, &rec.StreamID, &rec.Seq, &rec.Level, &rec.Message, &rec.TraceID, &rec.SpanID, &rec.Fields)
	rec.Time = rec.Time.UTC()
	return rec, err
}

// scanPoint reads (bucket, value, by...) with the by columns untyped, since
// level arrives as a smallint and everything else as nullable text.
func scanPoint(row pgx.CollectableRow, by []string) (Point, error) {
	var p Point
	vals := make([]any, len(by))
	dst := []any{&p.Bucket, &p.Value}
	for i := range vals {
		dst = append(dst, &vals[i])
	}
	if err := row.Scan(dst...); err != nil {
		return p, err
	}
	p.Bucket = p.Bucket.UTC()
	if len(by) > 0 {
		p.Labels = make(map[string]string, len(by))
	}
	for i, name := range by {
		switch v := vals[i].(type) {
		case nil:
		case int16:
			p.Labels[name] = model.Level(v).String()
		case string:
			p.Labels[name] = v
		default:
			return p, fmt.Errorf("unexpected %T for label %q", v, name)
		}
	}
	return p, nil
}
