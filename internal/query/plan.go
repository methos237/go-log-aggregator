package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
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
	// Logs scans Source. Logs.Args[0] is the []int64 stream-ID slot: nil here,
	// filled by the executor from Streams' result. An empty set means the
	// executor skips Logs entirely.
	//
	// Without an aggregation the rows are the eight logs columns in table
	// order. With one they are (bucket timestamptz, value float8, then one
	// column per `by` label in query order): level is a smallint, the rest text.
	Logs Stmt
	// Source is the relation Logs reads: "logs", or one of the continuous
	// aggregates when chooseSource can prove they answer the query exactly.
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
	var streams, logs stmt
	streams.WriteString("SELECT stream_id FROM streams")

	// logs accumulates only the WHERE conditions; the projection and FROM are
	// prepended at the end, once the stages have said whether streams must be
	// joined. The ids slot is typed even though it is nil so pgx knows it is
	// binding an array and does not have to guess from a bare nil.
	source := chooseSource(q, r)
	timeCol := "time"
	if source != "logs" {
		timeCol = "bucket"
	}
	logs.WriteString("stream_id = ANY(" + logs.arg([]int64(nil)) + ") AND " +
		timeCol + " >= " + logs.arg(r.Start) + " AND " + timeCol + " < " + logs.arg(r.End))

	first := true
	for _, m := range q.Selector.Matchers {
		if m.Label == "level" {
			// level is a column on the record, not a stream label, so it is the
			// one matcher that filters logs rather than streams.
			if err := logs.level(m.Op, m.Value); err != nil {
				return nil, err
			}
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

	// Every stage filters records, so they all land on the logs statement. A
	// parser stage changes where later label filters read their value from.
	extract := fromFields
	for _, st := range q.Stages {
		switch st := st.(type) {
		case LineFilter:
			logs.lineFilter(st)
		case ParserStage:
			extract = st.extractor()
		case LabelFilter:
			if err := logs.labelFilter(st, extract); err != nil {
				return nil, err
			}
		}
	}

	order := "DESC"
	if r.Direction == Forward {
		order = "ASC"
	}
	var head, tail string
	if q.Agg == nil {
		head = "SELECT time, stream_id, seq, level, message, trace_id, span_id, fields"
		tail = " ORDER BY time " + order + ", seq " + order
	} else {
		var err error
		if head, tail, err = logs.aggregate(q.Agg, extract, source, timeCol, order); err != nil {
			return nil, err
		}
	}
	from := " FROM " + source
	if logs.join {
		from += " JOIN streams USING (stream_id)"
	}
	sql := head + from + " WHERE " + logs.String() + tail + " LIMIT " + logs.arg(r.Limit)
	return &Plan{Streams: streams.stmt(), Logs: Stmt{SQL: sql, Args: logs.args}, Source: source}, nil
}

// grains are the continuous aggregates by bucket width, finest first, so the
// coarsest one that fits is the last to match in chooseSource.
var grains = []struct {
	source string
	width  time.Duration
}{{"logs_rate_1m", time.Minute}, {"logs_rate_1h", time.Hour}}

// chooseSource picks the relation that answers the query exactly at the least
// cost. A continuous aggregate only has (bucket, stream_id, level, n), so it
// serves an aggregation with no pipeline stages, no message bytes, and no
// `by` label outside level and the promoted stream columns; and only when the
// aggregation's buckets and the request's edges fall on its bucket boundaries,
// since a partially covered bucket would be counted whole or not at all.
func chooseSource(q *Query, r Request) string {
	a := q.Agg
	if a == nil || len(q.Stages) > 0 || a.Func == AggBytesOverTime {
		return "logs"
	}
	for _, l := range a.By {
		if l != "level" && !promoted[l] {
			return "logs"
		}
	}
	source := "logs"
	for _, g := range grains {
		if a.Range%g.width == 0 && r.Start.Truncate(g.width).Equal(r.Start) && r.End.Truncate(g.width).Equal(r.End) {
			source = g.source
		}
	}
	return source
}

// stmt accumulates SQL text and its arguments together, so a placeholder can
// only be minted by handing over the value it stands for.
type stmt struct {
	strings.Builder
	args []any
	// join is set when a predicate or projection read a streams column, so the
	// FROM clause must join streams.
	join bool
}

// arg records v as the next argument and returns its placeholder. This is the
// only place a "$n" is written, which is what a structural lint can anchor on.
func (s *stmt) arg(v any) string {
	s.args = append(s.args, v)
	return "$" + strconv.Itoa(len(s.args))
}

func (s *stmt) stmt() Stmt { return Stmt{SQL: s.String(), Args: s.args} }

// level writes a predicate on the record's level column, shared by selector
// matchers and label filters. The parser already rejected regex operators.
func (s *stmt) level(op Op, value string) error {
	lvl, err := model.ParseLevel(value)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	s.WriteString(" AND level " + sqlOps[op] + " " + s.arg(int16(lvl)))
	return nil
}

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

// lineSQL maps a LineOp to its Postgres spelling, indexed like sqlOps.
var lineSQL = [...]string{LineContains: "ILIKE", LineNotContains: "NOT ILIKE", LineMatches: "~", LineNotMatches: "!~"}

// likeMeta escapes the characters LIKE treats specially, so a filter text is
// matched literally. Postgres's default escape character is the backslash.
var likeMeta = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// lineFilter writes a predicate on the raw message. Substring search is the
// ILIKE baseline from roadmap §2.4; phase 8 benchmarks it against pg_trgm and
// tsvector and lands the winner here. Unlike selector regexes, line regexes are
// deliberately unanchored: `|~ "timeout"` is a search, not a whole-line match.
func (s *stmt) lineFilter(f LineFilter) {
	v := f.Text
	if !f.Op.IsRegex() {
		v = "%" + likeMeta.Replace(v) + "%"
	}
	s.WriteString(" AND message " + lineSQL[f.Op] + " " + s.arg(v))
}

// An extractor writes the SQL expression that yields a label's value for the
// current record: text, or NULL when the record does not have it.
type extractor func(s *stmt, name string) (string, error)

// fromFields reads the structured fields the agent extracted at ingest. It is
// the extractor in force before any parser stage.
func fromFields(s *stmt, name string) (string, error) {
	return "fields ->> " + s.arg(name), nil
}

// fromJSON parses the message as a JSON object at query time. The validity
// guard is what keeps one malformed line from aborting the whole query; a
// message that is valid JSON but not an object yields NULL from ->>.
func fromJSON(s *stmt, name string) (string, error) {
	return "CASE WHEN pg_input_is_valid(message, 'jsonb') THEN message::jsonb ->> " + s.arg(name) + " END", nil
}

// fromLogfmt takes the first key=value or key="quoted value" pair in the
// message, quotes stripped. The key is a user-chosen label name, so it is
// regex-escaped and travels inside the argument, never in the SQL text.
func fromLogfmt(s *stmt, name string) (string, error) {
	pat := `(?:^|\s)` + regexp.QuoteMeta(name) + `=("[^"]*"|\S*)`
	return `btrim(substring(message FROM ` + s.arg(pat) + `), '"')`, nil
}

// extractor returns the extractor a parser stage installs for the label
// filters after it.
func (p ParserStage) extractor() extractor {
	switch p.Kind {
	case ParserJSON:
		return fromJSON
	case ParserLogfmt:
		return fromLogfmt
	}
	are, groups := namedGroups(p.Pattern)
	return func(s *stmt, name string) (string, error) {
		i, ok := groups[name]
		if !ok {
			return "", fmt.Errorf("query: label %q is not a capture group of the regexp stage", name)
		}
		return "(regexp_match(message, " + s.arg(are) + "))[" + s.arg(i) + "]", nil
	}
}

// namedGroups rewrites a Go pattern for Postgres, whose regexes have no named
// groups: every (?P<name>...) and (?<name>...) becomes a plain (...), and the
// map gives each name's 1-based capture index. The parser already compiled the
// pattern, so it is well-formed; the scan only has to skip escapes and
// bracket classes so that a "(" inside them is not counted as a group.
func namedGroups(pat string) (string, map[string]int) {
	groups := make(map[string]int)
	var out strings.Builder
	n, class := 0, false
	for i := 0; i < len(pat); i++ {
		c := pat[i]
		switch {
		case c == '\\' && i+1 < len(pat):
			out.WriteString(pat[i : i+2])
			i++
			continue
		case class:
			class = c != ']'
		case c == '[':
			// A "]" right after "[" or "[^" is a literal, not the close.
			class = true
			j := i + 1
			if j < len(pat) && pat[j] == '^' {
				j++
			}
			if j < len(pat) && pat[j] == ']' {
				j++
			}
			out.WriteString(pat[i:j])
			i = j - 1
			continue
		case c == '(':
			rest := pat[i+1:]
			if !strings.HasPrefix(rest, "?") {
				n++
				break
			}
			rest = strings.TrimPrefix(strings.TrimPrefix(rest, "?"), "P")
			if end := strings.IndexByte(rest, '>'); strings.HasPrefix(rest, "<") && end > 0 {
				n++
				groups[rest[1:end]] = n
				out.WriteByte('(')
				// Skip past the name: the bytes consumed from pat[i+1:] plus ">".
				i += len(pat[i+1:]) - len(rest) + end + 1
				continue
			}
		}
		out.WriteByte(c)
	}
	return out.String(), groups
}

// column returns the expression for a label name used in the pipeline: the
// level column, a promoted stream column (which needs the join), or otherwise
// a record field from the extractor in force. Extra stream labels are not
// addressable here; they belong in the selector.
func (s *stmt) column(name string, extract extractor) (string, error) {
	switch {
	case name == "level":
		return "level", nil
	case promoted[name]:
		s.join = true
		return name, nil
	}
	return extract(s, name)
}

// aggregate returns the projection and the GROUP BY/ORDER BY tail for an
// aggregation. Grouping and ordering use ordinals so the `by` expressions,
// which may carry placeholders, are written once.
func (s *stmt) aggregate(a *Aggregation, extract extractor, source, timeCol, order string) (head, tail string, err error) {
	head = "SELECT time_bucket(" + s.arg(a.Range) + "::interval, " + timeCol + ") AS bucket, "
	count := "count(*)"
	if source != "logs" {
		count = "sum(n)"
	}
	switch a.Func {
	case AggRate:
		head += count + "::float8 / " + s.arg(a.Range.Seconds())
	case AggCountOverTime:
		head += count + "::float8"
	case AggBytesOverTime:
		head += "sum(octet_length(message))::float8"
	}
	group := "1"
	for i, l := range a.By {
		col, err := s.column(l, extract)
		if err != nil {
			return "", "", err
		}
		head += ", " + col
		group += ", " + strconv.Itoa(i+3)
	}
	return head, " GROUP BY " + group + " ORDER BY 1 " + order + strings.TrimPrefix(group, "1"), nil
}

// labelFilter writes a predicate on an extracted label. Text comparisons
// coalesce a missing label to "" for the same Prometheus semantics as matcher;
// numeric ones compare as numeric and drop records whose value is missing or
// not a number, since neither is meaningfully >= 500.
func (s *stmt) labelFilter(f LabelFilter, extract extractor) error {
	if f.Label == "level" {
		return s.level(f.Op, f.Value)
	}
	v, err := s.column(f.Label, extract)
	if err != nil {
		return err
	}
	if f.Numeric {
		// The literal stays a string and is cast by Postgres, so "4.5" and
		// "500" keep their exact numeric value instead of a float64's.
		s.WriteString(" AND CASE WHEN pg_input_is_valid(" + v + ", 'numeric') THEN (" + v + ")::numeric END " +
			sqlOps[f.Op] + " " + s.arg(f.Value) + "::numeric")
		return nil
	}
	val := f.Value
	if f.Op.IsRegex() {
		val = anchor(val)
	}
	s.WriteString(" AND coalesce(" + v + ", '') " + sqlOps[f.Op] + " " + s.arg(val))
	return nil
}
