// Package query is the log query DSL: a LogQL-shaped language compiled to
// parameterized TimescaleDB SQL.
//
// The pipeline is lexer -> parser -> AST -> planner. Everything in this package
// is pure logic with no I/O, so the whole compiler is unit-testable and fuzzable.
//
// Grammar (EBNF; the parser is the implementation of exactly this):
//
//	query        = selector { line_filter | "|" stage } [ "|" aggregation ] ;
//	selector     = "{" matcher { "," matcher } "}" ;
//	matcher      = label op string ;
//	op           = "=" | "!=" | "=~" | "!~" | ">=" | "<=" | ">" | "<" ;
//	line_filter  = ( "|=" | "!=" | "|~" | "!~" ) string ;
//	stage        = parser_stage | label_filter ;
//	parser_stage = "json" | "logfmt" | "regexp" string ;
//	label_filter = label op ( string | number ) ;
//	aggregation  = func "(" duration ")" [ "by" "(" label { "," label } ")" ] ;
//	func         = "rate" | "count_over_time" | "bytes_over_time" ;
//	duration     = number ( "s" | "m" | "h" | "d" ) ;
//	string       = Go string literal, "double-quoted" or `raw` ;
//
// Line filters attach directly to the selector with no leading "|", as in
// LogQL; parser stages, label filters and the aggregation are each introduced
// by "|".
//
// The label "level" is reserved: it names the record's level column rather
// than a stream label, so `{service="api", level>="warn"}` filters records,
// not streams. Its value must be a level name model.ParseLevel accepts, and it
// supports every operator except the regex pair.
//
// Pipeline stages filter records, never streams. A line filter tests the raw
// message; `|=` and `!=` are case-insensitive substring searches. A label
// filter reads its value from the nearest preceding parser stage: `json`
// parses the message as a JSON object, `logfmt` takes the first key=value pair,
// and `regexp` exposes its named capture groups. Before any parser stage, label
// filters read the structured fields the agent extracted at ingest. A missing
// label compares as "" for text operators; for numeric operators (a bare
// number on the right) a missing or non-numeric value never matches.
package query

import "time"

// Pos is a 1-based line and column (in runes) into the query text. Every
// node that can be the subject of an error carries one so the message can
// point at it.
type Pos struct {
	Line, Col int
}

// Query is a parsed query: a stream selector, zero or more pipeline stages in
// source order, and an optional trailing aggregation.
type Query struct {
	Selector Selector
	Stages   []Stage
	Agg      *Aggregation
}

// Selector is the `{...}` block. It always has at least one matcher.
type Selector struct {
	Pos      Pos
	Matchers []Matcher
}

// Matcher is one `label op "value"` inside a selector.
type Matcher struct {
	Pos   Pos
	Label string
	Op    Op
	Value string
}

// Op is a comparison operator, shared by selector matchers and label filters.
type Op int

// Operators in the order the grammar lists them.
const (
	OpEq  Op = iota // =
	OpNeq           // !=
	OpRe            // =~
	OpNre           // !~
	OpGte           // >=
	OpLte           // <=
	OpGt            // >
	OpLt            // <
)

var opNames = [...]string{"=", "!=", "=~", "!~", ">=", "<=", ">", "<"}

func (o Op) String() string {
	if int(o) < len(opNames) {
		return opNames[o]
	}
	return "Op(?)"
}

// IsRegex reports whether the operator's right-hand side is a regular
// expression.
func (o Op) IsRegex() bool { return o == OpRe || o == OpNre }

// Stage is one pipeline step: a LineFilter, LabelFilter or ParserStage.
type Stage interface {
	stage()
}

// LineFilter tests the raw message: `|= "x"`, `!= "x"`, `|~ "re"`, `!~ "re"`.
type LineFilter struct {
	Pos  Pos
	Op   LineOp
	Text string
}

// LineOp is a line-filter operator.
type LineOp int

// Line-filter operators.
const (
	LineContains    LineOp = iota // |=
	LineNotContains               // !=
	LineMatches                   // |~
	LineNotMatches                // !~
)

var lineOpNames = [...]string{"|=", "!=", "|~", "!~"}

func (o LineOp) String() string {
	if int(o) < len(lineOpNames) {
		return lineOpNames[o]
	}
	return "LineOp(?)"
}

// IsRegex reports whether the operator's right-hand side is a regular
// expression.
func (o LineOp) IsRegex() bool { return o == LineMatches || o == LineNotMatches }

// LabelFilter tests a label after extraction: `| status >= 500`. Numeric is
// set when the literal was a bare number rather than a string.
type LabelFilter struct {
	Pos     Pos
	Label   string
	Op      Op
	Value   string
	Numeric bool
}

// ParserStage extracts labels from the message: `| json`, `| logfmt`,
// `| regexp "(?P<name>...)"`. Pattern is set only for regexp.
type ParserStage struct {
	Pos     Pos
	Kind    ParserKind
	Pattern string
}

// ParserKind is which extractor a ParserStage runs.
type ParserKind int

// Parser stages.
const (
	ParserJSON ParserKind = iota
	ParserLogfmt
	ParserRegexp
)

var parserKindNames = [...]string{"json", "logfmt", "regexp"}

func (k ParserKind) String() string {
	if int(k) < len(parserKindNames) {
		return parserKindNames[k]
	}
	return "ParserKind(?)"
}

func (LineFilter) stage()  {}
func (LabelFilter) stage() {}
func (ParserStage) stage() {}

// Aggregation is the trailing `| rate(5m) by (level)`.
type Aggregation struct {
	Pos   Pos
	Func  AggFunc
	Range time.Duration
	By    []string
}

// AggFunc is an aggregation function.
type AggFunc int

// Aggregation functions.
const (
	AggRate AggFunc = iota
	AggCountOverTime
	AggBytesOverTime
)

var aggFuncNames = [...]string{"rate", "count_over_time", "bytes_over_time"}

func (f AggFunc) String() string {
	if int(f) < len(aggFuncNames) {
		return aggFuncNames[f]
	}
	return "AggFunc(?)"
}
