package query

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// MaxRegexLen bounds any regular expression literal (matcher, line filter,
// regexp stage). Go's regexp is linear-time, so the cap is about memory and
// about refusing to ship a kilobyte of pattern to Postgres as a query
// argument, not about catastrophic backtracking.
const MaxRegexLen = 1024

// Error is a parse error with the 1-based position of the token it is about.
type Error struct {
	Pos Pos
	Msg string
}

func (e *Error) Error() string { return fmt.Sprintf("%d:%d: %s", e.Pos.Line, e.Pos.Col, e.Msg) }

// Parse parses one query. The error is always a *Error pointing at the
// offending token.
func Parse(src string) (q *Query, err error) {
	p := &parser{lex: newLexer(src)}
	// Bailing out with a panic keeps the grammar functions free of error
	// plumbing, the same trade text/template makes. Anything that is not our
	// own *Error is a real bug and is re-raised.
	defer func() {
		if r := recover(); r != nil {
			pe, ok := r.(*Error)
			if !ok {
				panic(r)
			}
			q, err = nil, pe
		}
	}()
	p.advance()
	q = &Query{Text: src, Selector: p.parseSelector()}
	for p.tok.Kind != tokEOF {
		switch p.tok.Kind {
		case tokPipeExact, tokNeq, tokPipeRe, tokNre:
			q.Stages = append(q.Stages, p.parseLineFilter())
		case tokPipe:
			p.advance()
			kw := p.expect(tokIdent, stageWhat)
			if fn, ok := aggFuncs[kw.Text]; ok {
				q.Agg = p.parseAggregation(kw, fn)
				if p.tok.Kind != tokEOF {
					p.bail(p.tok.Pos, "aggregation must be the last stage")
				}
				return q, nil
			}
			q.Stages = append(q.Stages, p.parseStage(kw))
		default:
			p.bail(p.tok.Pos, `expected line filter, "|" or end of input, got %s`, describe(p.tok))
		}
	}
	return q, nil
}

// parser is a single-pass recursive-descent parser with one token of
// lookahead. It stops at the first error; there is no recovery, because a
// query is one line typed by a person and the first mistake is the one they
// want to hear about.
type parser struct {
	lex *lexer
	tok token
}

// advance moves to the next token. An illegal token is an error the moment it
// becomes the lookahead: it can never satisfy any rule, and since it is only
// lexed once the previous token has been accepted, reporting it here keeps
// errors in source order.
func (p *parser) advance() {
	p.tok = p.lex.next()
	if p.tok.Kind == tokIllegal {
		p.bail(p.tok.Pos, "%s", p.tok.Err)
	}
}

func (p *parser) bail(pos Pos, format string, args ...any) {
	panic(&Error{Pos: pos, Msg: fmt.Sprintf(format, args...)})
}

// want checks the current token without consuming it, so a caller can
// validate the token's text before advance() lexes what follows it.
func (p *parser) want(kind tokenKind, what string) token {
	if p.tok.Kind != kind {
		p.bail(p.tok.Pos, "expected %s, got %s", what, describe(p.tok))
	}
	return p.tok
}

func (p *parser) expect(kind tokenKind, what string) token {
	t := p.want(kind, what)
	p.advance()
	return t
}

// describe renders a token for the "got ..." half of an error. Literals show
// their text because "got identifier" alone does not tell the user which word
// the parser tripped over; punctuation is already self-describing.
func describe(t token) string {
	switch t.Kind {
	case tokIdent, tokString, tokNumber, tokDuration:
		return fmt.Sprintf("%s %q", t.Kind, t.Text)
	}
	return t.Kind.String()
}

var ops = map[tokenKind]Op{
	tokEq: OpEq, tokNeq: OpNeq, tokRe: OpRe, tokNre: OpNre,
	tokGte: OpGte, tokLte: OpLte, tokGt: OpGt, tokLt: OpLt,
}

// parseOp consumes a comparison operator.
func (p *parser) parseOp() Op {
	op, ok := ops[p.tok.Kind]
	if !ok {
		p.bail(p.tok.Pos, "expected matcher operator, got %s", describe(p.tok))
	}
	p.advance()
	return op
}

func (p *parser) parseSelector() Selector {
	sel := Selector{Pos: p.expect(tokLBrace, `"{"`).Pos}
	if p.tok.Kind == tokRBrace {
		p.bail(sel.Pos, "selector needs at least one matcher")
	}
	for {
		sel.Matchers = append(sel.Matchers, p.parseMatcher())
		if p.tok.Kind == tokComma {
			p.advance()
			continue
		}
		p.expect(tokRBrace, `"}" or ","`)
		return sel
	}
}

func (p *parser) parseMatcher() Matcher {
	label := p.expect(tokIdent, "label name")
	m := Matcher{Pos: label.Pos, Label: label.Text}
	opPos := p.tok.Pos
	m.Op = p.parseOp()
	m.Value = p.parseValue(m.Label, m.Op, opPos)
	return m
}

// parseValue consumes the string operand of a matcher and applies the checks
// that depend on the label and operator: level names must be real levels, and
// regex patterns must compile. Doing this at parse time means a bad query is
// rejected before it costs a database round trip. The original text is kept;
// the planner re-parses level names into their numeric form.
func (p *parser) parseValue(label string, op Op, opPos Pos) string {
	if label == "level" && op.IsRegex() {
		p.bail(opPos, "level does not support %s", op)
	}
	val := p.want(tokString, "string")
	if label == "level" {
		p.checkLevel(val)
	} else if op.IsRegex() {
		p.checkRegex(val)
	}
	p.advance()
	return val.Text
}

func (p *parser) checkLevel(val token) {
	if _, err := model.ParseLevel(val.Text); err != nil {
		p.bail(val.Pos, "unknown level %q", val.Text)
	}
}

// checkRegex validates a pattern with Go's regexp, then rejects the Go-only
// syntax Postgres's ARE dialect cannot evaluate, so the mismatch is a parse
// error rather than a failed statement: \p and \P Unicode classes, \Q...\E
// quoting, and inline flags anywhere but the very start. The planner handles
// the escapes that merely differ in spelling (\b, \B, \z).
func (p *parser) checkRegex(val token) {
	if len(val.Text) > MaxRegexLen {
		p.bail(val.Pos, "regex is %d bytes, max %d", len(val.Text), MaxRegexLen)
	}
	if _, err := regexp.Compile(val.Text); err != nil {
		p.bail(val.Pos, "invalid regex: %s", err.Error())
	}
	re := val.Text
	for i := 0; i < len(re); i++ {
		switch re[i] {
		case '\\':
			if i+1 < len(re) && strings.IndexByte("pPQE", re[i+1]) >= 0 {
				p.bail(val.Pos, `invalid regex: \%c is not supported`, re[i+1])
			}
			i++
		case '(':
			if i > 0 && strings.HasPrefix(re[i:], "(?") && i+2 < len(re) && strings.IndexByte(":P<", re[i+2]) < 0 {
				p.bail(val.Pos, "invalid regex: flags like (?i) are only supported at the start of the pattern")
			}
		}
	}
}

var lineOps = map[tokenKind]LineOp{
	tokPipeExact: LineContains, tokNeq: LineNotContains, tokPipeRe: LineMatches, tokNre: LineNotMatches,
}

func (p *parser) parseLineFilter() LineFilter {
	f := LineFilter{Pos: p.tok.Pos, Op: lineOps[p.tok.Kind]}
	p.advance()
	val := p.want(tokString, "string")
	if f.Op.IsRegex() {
		p.checkRegex(val)
	}
	p.advance()
	f.Text = val.Text
	return f
}

const stageWhat = "a stage (json, logfmt, regexp, a label filter, or an aggregation)"

// parseStage parses a stage given the identifier that follows the "|". The
// lexer hands keywords over as plain identifiers, so this is where "json" is
// told apart from a label called json: an identifier followed by an operator
// is a label filter, anything else has to be a keyword.
func (p *parser) parseStage(kw token) Stage {
	switch kw.Text {
	case "json":
		return ParserStage{Pos: kw.Pos, Kind: ParserJSON}
	case "logfmt":
		return ParserStage{Pos: kw.Pos, Kind: ParserLogfmt}
	case "regexp":
		pat := p.want(tokString, "string")
		p.checkRegex(pat)
		p.advance()
		return ParserStage{Pos: kw.Pos, Kind: ParserRegexp, Pattern: pat.Text}
	}
	if _, ok := ops[p.tok.Kind]; !ok {
		p.bail(kw.Pos, "expected %s, got %s", stageWhat, describe(kw))
	}
	return p.parseLabelFilter(kw)
}

func (p *parser) parseLabelFilter(label token) LabelFilter {
	f := LabelFilter{Pos: label.Pos, Label: label.Text}
	opPos := p.tok.Pos
	f.Op = p.parseOp()
	// A bare number is only meaningful for an ordering or equality test on an
	// extracted field; regexes and level names are always strings.
	if p.tok.Kind == tokNumber && !f.Op.IsRegex() && f.Label != "level" {
		f.Value, f.Numeric = p.tok.Text, true
		p.advance()
		return f
	}
	f.Value = p.parseValue(f.Label, f.Op, opPos)
	return f
}

var aggFuncs = map[string]AggFunc{
	"rate": AggRate, "count_over_time": AggCountOverTime, "bytes_over_time": AggBytesOverTime,
}

func (p *parser) parseAggregation(kw token, fn AggFunc) *Aggregation {
	agg := &Aggregation{Pos: kw.Pos, Func: fn}
	p.expect(tokLParen, `"("`)
	agg.Range = p.parseDuration()
	p.expect(tokRParen, `")"`)
	if p.tok.Kind != tokIdent || p.tok.Text != "by" {
		return agg
	}
	p.advance()
	p.expect(tokLParen, `"("`)
	for {
		label := p.expect(tokIdent, "label name")
		// GROUP BY a, a is legal SQL but never what the user meant.
		if slices.Contains(agg.By, label.Text) {
			p.bail(label.Pos, "duplicate label %q in by (...)", label.Text)
		}
		agg.By = append(agg.By, label.Text)
		if p.tok.Kind != tokComma {
			break
		}
		p.advance()
	}
	p.expect(tokRParen, `")" or ","`)
	return agg
}

// units is keyed by the duration suffix the lexer accepts. Done by hand
// because time.ParseDuration has no "d", and a retention-scale query needs
// one.
var units = map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour}

func (p *parser) parseDuration() time.Duration {
	tok := p.want(tokDuration, "duration")
	unit := units[tok.Text[len(tok.Text)-1]]
	n, err := strconv.Atoi(tok.Text[:len(tok.Text)-1])
	d := time.Duration(n) * unit
	// Atoi catches digits that overflow an int; the division catches a count
	// that fits but whose nanoseconds do not.
	if err != nil || d/unit != time.Duration(n) {
		p.bail(tok.Pos, "duration out of range")
	}
	if d == 0 {
		p.bail(tok.Pos, "duration must be positive")
	}
	p.advance()
	return d
}
