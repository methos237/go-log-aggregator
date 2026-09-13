package query

import (
	"regexp"
	"strings"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// Evaluator evaluates a parsed query against records in memory, predicate for
// predicate the way the planner compiles it to SQL. It exists for live tail,
// where there is no database in the path, and mirroring plan.go rather than
// re-deriving the semantics is the point: a record the tail shows and the
// query would not find, or the reverse, is a bug in this file.
//
// Only the grammar a tail can honor is accepted: the selector, including the
// reserved level label, and line filters. Parser stages, label filters and
// aggregations need the executor; NewEvaluator rejects them with an *Error at
// the offending stage so the API reports it like a parse error.
type Evaluator struct {
	stream []func(model.LabelSet) bool
	record []func(*model.LogRecord) bool
}

// NewEvaluator compiles q. Regexes were validated by the parser, so compiling
// them again cannot fail; the only error is an unsupported stage.
func NewEvaluator(q *Query) (*Evaluator, error) {
	m := &Evaluator{}
	for _, sel := range q.Selector.Matchers {
		if sel.Label == "level" {
			// Cannot fail: the parser already ran ParseLevel on the value.
			lvl, _ := model.ParseLevel(sel.Value)
			m.record = append(m.record, levelPred(sel.Op, lvl))
			continue
		}
		m.stream = append(m.stream, labelPred(sel))
	}
	for _, st := range q.Stages {
		f, ok := st.(LineFilter)
		if !ok {
			return nil, unsupported(st)
		}
		m.record = append(m.record, linePred(f))
	}
	if q.Agg != nil {
		return nil, &Error{Pos: q.Agg.Pos, Msg: "tail cannot aggregate; use query"}
	}
	return m, nil
}

// MatchStream reports whether the selector's stream matchers accept ls. Level
// is a record predicate and is not consulted here, so a stream is matched once
// and its records are then tested individually.
func (m *Evaluator) MatchStream(ls model.LabelSet) bool {
	for _, p := range m.stream {
		if !p(ls) {
			return false
		}
	}
	return true
}

// MatchRecord reports whether the level matchers and line filters accept r.
func (m *Evaluator) MatchRecord(r *model.LogRecord) bool {
	for _, p := range m.record {
		if !p(r) {
			return false
		}
	}
	return true
}

func unsupported(st Stage) error {
	switch s := st.(type) {
	case LabelFilter:
		return &Error{Pos: s.Pos, Msg: "tail supports selectors and line filters; label filter | " + s.Label + " needs query"}
	case ParserStage:
		return &Error{Pos: s.Pos, Msg: "tail supports selectors and line filters; | " + s.Kind.String() + " needs query"}
	default:
		return &Error{Msg: "tail: unsupported stage"}
	}
}

// labelPred mirrors stmt.matcher. Promoted labels are compared directly, as
// their columns are. An extra label with = mirrors the jsonb containment the
// planner uses for the GIN index: the key must be present, so {region=""} does
// not match a stream without region. Every other operator mirrors the
// coalesce(labels ->> name, ”) the planner writes, where a missing label is
// "" — Prometheus semantics, so {region!="eu"} matches streams with no region.
func labelPred(sel Matcher) func(model.LabelSet) bool {
	get := func(ls model.LabelSet) (string, bool) {
		switch sel.Label {
		case "service":
			return ls.Service, true
		case "host":
			return ls.Host, true
		case "env":
			return ls.Env, true
		}
		v, ok := ls.Extra[sel.Label]
		return v, ok
	}
	if sel.Op == OpEq {
		return func(ls model.LabelSet) bool {
			v, ok := get(ls)
			return ok && v == sel.Value
		}
	}
	cmp := textPred(sel.Op, sel.Value)
	return func(ls model.LabelSet) bool {
		v, _ := get(ls)
		return cmp(v)
	}
}

// textPred compares text the way the planner's sqlOps do. Regexes are anchored
// like Prometheus and LogQL. Ordering operators compare bytewise, which is the
// "C" collation; a database with another default collation orders mixed-case
// or non-ASCII text differently, an edge tail accepts rather than emulating a
// locale.
func textPred(op Op, value string) func(string) bool {
	switch op {
	case OpEq:
		return func(s string) bool { return s == value }
	case OpNeq:
		return func(s string) bool { return s != value }
	case OpRe, OpNre:
		re := regexp.MustCompile(anchor(value))
		if op == OpRe {
			return re.MatchString
		}
		return func(s string) bool { return !re.MatchString(s) }
	case OpGte:
		return func(s string) bool { return s >= value }
	case OpLte:
		return func(s string) bool { return s <= value }
	case OpGt:
		return func(s string) bool { return s > value }
	default: // OpLt
		return func(s string) bool { return s < value }
	}
}

// levelPred mirrors stmt.level: an integer comparison on the record column.
// The parser rejected the regex operators for level.
func levelPred(op Op, want model.Level) func(*model.LogRecord) bool {
	switch op {
	case OpEq:
		return func(r *model.LogRecord) bool { return r.Level == want }
	case OpNeq:
		return func(r *model.LogRecord) bool { return r.Level != want }
	case OpGte:
		return func(r *model.LogRecord) bool { return r.Level >= want }
	case OpLte:
		return func(r *model.LogRecord) bool { return r.Level <= want }
	case OpGt:
		return func(r *model.LogRecord) bool { return r.Level > want }
	default: // OpLt
		return func(r *model.LogRecord) bool { return r.Level < want }
	}
}

// linePred mirrors stmt.lineFilter: |= is ILIKE, a case-insensitive substring
// search, and |~ is an unanchored regex, a search rather than a whole-line
// match.
func linePred(f LineFilter) func(*model.LogRecord) bool {
	if f.Op.IsRegex() {
		re := regexp.MustCompile(f.Text)
		want := f.Op == LineMatches
		return func(r *model.LogRecord) bool { return re.MatchString(r.Message) == want }
	}
	needle := strings.ToLower(f.Text)
	want := f.Op == LineContains
	return func(r *model.LogRecord) bool {
		return strings.Contains(strings.ToLower(r.Message), needle) == want
	}
}
