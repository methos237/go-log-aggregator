package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// This file is the SQL-injection surface of the project, so it follows two
// rules that the tests enforce mechanically:
//
//   - No user bytes ever reach SQL text. Every literal, including the NAME of
//     an extra label, goes through stmt.arg and becomes a $n placeholder. The
//     only identifiers written into a statement are the fixed table and column
//     names below.
//   - The logs statement always carries a time range and a LIMIT, so no query
//     can ask the hypertable for everything.

// Stmt is one parameterized statement.
type Stmt struct {
	SQL  string
	Args []any
}

// Plan is a compiled query, executed as two statements in order.
//
// Resolving streams first and then scanning the hypertable by stream_id, rather
// than joining, is what lets the streams table's indexes do the selector work
// while the hypertable scan stays a plain (stream_id, time) range. It also
// gives the executor an early exit: a selector that matches nothing never
// touches logs at all.
type Plan struct {
	// Streams resolves the selector against the streams table: SELECT stream_id ...
	Streams Stmt
	// Logs scans the hypertable. Logs.Args[0] is the []int64 stream-ID slot: nil
	// here, filled by the executor from Streams' result. An empty set means the
	// executor skips Logs entirely.
	Logs Stmt
	// Source is the relation Logs reads. Always "logs" until subtask 8 adds the
	// continuous-aggregate choice.
	Source string
}

// Direction is the time order of the logs statement.
type Direction int

// Backward is newest first, the default for a log viewer; Forward is oldest
// first, for tailing from a point in time.
const (
	Backward Direction = iota
	Forward
)

// Request is the per-call half of a query: range, cap, order. All required.
type Request struct {
	Start, End time.Time
	Limit      int
	Direction  Direction
}

// ErrUnsupported is returned for pipeline stages and aggregations until
// subtasks 7 and 8.
var ErrUnsupported = errors.New("query: pipeline stages and aggregations are not supported yet")

// sqlOps maps an Op to its Postgres spelling. Indexed by Op, so it must stay in
// the grammar's order.
var sqlOps = [...]string{
	OpEq: "=", OpNeq: "<>", OpRe: "~", OpNre: "!~",
	OpGte: ">=", OpLte: "<=", OpGt: ">", OpLt: "<",
}

// promoted are the labels that live in their own column on streams. Anything
// else is a key in the labels JSONB column.
var promoted = map[string]bool{"service": true, "host": true, "env": true}

// Compile turns a parsed query and a request into the two statements that
// answer it.
func Compile(q *Query, r Request) (*Plan, error) {
	switch {
	case r.Start.IsZero():
		return nil, errors.New("query: start time is required")
	case !r.End.After(r.Start):
		return nil, errors.New("query: end time must be after start time")
	case r.Limit <= 0:
		return nil, errors.New("query: limit must be positive")
	}
	if len(q.Stages) > 0 || q.Agg != nil {
		return nil, ErrUnsupported
	}

	var streams, logs stmt
	streams.WriteString("SELECT stream_id FROM streams")
	// The ids slot is typed even though it is nil so pgx knows it is binding an
	// array and does not have to guess from a bare nil.
	logs.WriteString("SELECT time, stream_id, seq, level, message, trace_id, span_id, fields FROM logs WHERE stream_id = ANY(" +
		logs.arg([]int64(nil)) + ") AND time >= " + logs.arg(r.Start) + " AND time < " + logs.arg(r.End))

	first := true
	for _, m := range q.Selector.Matchers {
		if m.Label == "level" {
			// level is a column on the record, not a stream label, so it is the
			// one matcher that filters logs rather than streams.
			lvl, err := model.ParseLevel(m.Value)
			if err != nil {
				return nil, fmt.Errorf("query: %w", err)
			}
			logs.WriteString(" AND level " + sqlOps[m.Op] + " " + logs.arg(int16(lvl)))
			continue
		}
		if first {
			streams.WriteString(" WHERE ")
			first = false
		} else {
			streams.WriteString(" AND ")
		}
		streams.matcher(m)
	}

	order := "DESC"
	if r.Direction == Forward {
		order = "ASC"
	}
	logs.WriteString(" ORDER BY time " + order + ", seq " + order + " LIMIT " + logs.arg(r.Limit))

	return &Plan{Streams: streams.stmt(), Logs: logs.stmt(), Source: "logs"}, nil
}

// stmt accumulates SQL text and its arguments together, so a placeholder can
// only be minted by handing over the value it stands for.
type stmt struct {
	strings.Builder
	args []any
}

// arg records v as the next argument and returns its placeholder. This is the
// only place a "$n" is written, which is what a structural lint can anchor on.
func (s *stmt) arg(v any) string {
	s.args = append(s.args, v)
	return "$" + strconv.Itoa(len(s.args))
}

func (s *stmt) stmt() Stmt { return Stmt{SQL: s.String(), Args: s.args} }

// matcher writes one selector predicate.
func (s *stmt) matcher(m Matcher) {
	v := m.Value
	if m.Op.IsRegex() {
		v = anchor(v)
	}
	switch {
	case promoted[m.Label]:
		s.WriteString(m.Label + " " + sqlOps[m.Op] + " " + s.arg(v))
	case m.Op == OpEq:
		// Containment is the one form the jsonb_path_ops GIN index on labels
		// can serve, so equality gets it rather than the ->> comparison below.
		// Marshal cannot fail on a map[string]string.
		doc, _ := json.Marshal(map[string]string{m.Label: v})
		s.WriteString("labels @> " + s.arg(string(doc)) + "::jsonb")
	default:
		// coalesce so a stream without the label compares as "": Prometheus
		// semantics, where {region!="eu"} also matches streams with no region.
		// The label name is user input too, hence an argument, not text.
		s.WriteString("coalesce(labels ->> " + s.arg(m.Label) + ", '') " + sqlOps[m.Op] + " " + s.arg(v))
	}
}

// anchor makes a regex match the whole value, as Prometheus and LogQL do.
//
// The parser validated the pattern with Go's regexp; Postgres evaluates it as
// an ARE. The two agree on everything a label selector realistically uses, but
// exotic syntax may diverge. Accepted for now.
func anchor(re string) string { return "^(?:" + re + ")$" }
