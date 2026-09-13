package query

import (
	"fmt"
	"strconv"
	"unicode/utf8"
)

type tokenKind int

const (
	tokEOF       tokenKind = iota
	tokIllegal             // unexpected byte or bad literal; Text is the offending text, Err the reason
	tokIdent               // [A-Za-z_][A-Za-z0-9_]*
	tokString              // "..." with Go escapes or `...` raw; Text is the decoded value
	tokNumber              // [0-9]+ ( "." [0-9]+ )?
	tokDuration            // [0-9]+ ("s"|"m"|"h"|"d")
	tokLBrace              // {
	tokRBrace              // }
	tokLParen              // (
	tokRParen              // )
	tokComma               // ,
	tokPipe                // |
	tokEq                  // =
	tokNeq                 // !=
	tokRe                  // =~
	tokNre                 // !~
	tokGte                 // >=
	tokLte                 // <=
	tokGt                  // >
	tokLt                  // <
	tokPipeExact           // |=
	tokPipeRe              // |~
)

// tokenKindNames is indexed by tokenKind. Keep in sync with the constants above.
var tokenKindNames = [...]string{
	tokEOF:       "end of input",
	tokIllegal:   "illegal token",
	tokIdent:     "identifier",
	tokString:    "string",
	tokNumber:    "number",
	tokDuration:  "duration",
	tokLBrace:    `"{"`,
	tokRBrace:    `"}"`,
	tokLParen:    `"("`,
	tokRParen:    `")"`,
	tokComma:     `","`,
	tokPipe:      `"|"`,
	tokEq:        `"="`,
	tokNeq:       `"!="`,
	tokRe:        `"=~"`,
	tokNre:       `"!~"`,
	tokGte:       `">="`,
	tokLte:       `"<="`,
	tokGt:        `">"`,
	tokLt:        `"<"`,
	tokPipeExact: `"|="`,
	tokPipeRe:    `"|~"`,
}

// String is the human name used in parser errors ("expected string, got
// identifier").
func (k tokenKind) String() string {
	if int(k) < len(tokenKindNames) {
		return tokenKindNames[k]
	}
	return fmt.Sprintf("tokenKind(%d)", int(k))
}

// token is one lexeme. Text is the decoded value for strings and the source
// text for everything else; Err is set only when Kind is tokIllegal.
type token struct {
	Kind tokenKind
	Text string
	Pos  Pos
	Err  string
}

// lexer turns query text into tokens. It knows nothing about the grammar:
// keywords such as "json" or "rate" come out as plain identifiers, because
// the parser already has to check them against context and a second copy of
// the keyword list here would only drift.
type lexer struct {
	src  string
	off  int // byte offset of the next unread rune
	line int
	col  int
}

func newLexer(src string) *lexer {
	return &lexer{src: src, line: 1, col: 1}
}

// next returns the next token. Once the input is exhausted it returns tokEOF
// forever. It never panics: invalid UTF-8 and unterminated strings surface as
// tokIllegal, and every non-EOF token consumes at least one byte, so a caller
// looping to EOF always terminates.
func (l *lexer) next() token {
	for l.off < len(l.src) && isSpace(l.src[l.off]) {
		l.advance()
	}
	start, pos := l.off, Pos{Line: l.line, Col: l.col}
	if l.off >= len(l.src) {
		return token{Kind: tokEOF, Pos: pos}
	}
	tok := func(kind tokenKind) token {
		return token{Kind: kind, Text: l.src[start:l.off], Pos: pos}
	}
	illegal := func(err string) token {
		t := tok(tokIllegal)
		t.Err = err
		return t
	}

	r := l.advance()
	switch {
	case r == '"' || r == '`':
		for {
			c, w := l.peek()
			if w == 0 {
				return illegal("unterminated string")
			}
			l.advance()
			if c == r {
				break
			}
			// Only the interpreted form has escapes; consuming the escaped rune
			// here is what stops \" from closing the string.
			if c == '\\' && r == '"' {
				if _, w := l.peek(); w == 0 {
					return illegal("unterminated string")
				}
				l.advance()
			}
		}
		t := tok(tokString)
		// Unquote maps a bad byte in a "quoted" string to U+FFFD and passes one
		// in a `raw` string through; both would turn a typo into a silent miss
		// or a database error, so the source bytes get the same check as text
		// outside strings, before Unquote can paper over them.
		if !utf8.ValidString(t.Text) {
			return illegal("invalid UTF-8")
		}
		s, err := strconv.Unquote(t.Text)
		if err != nil {
			return illegal("invalid string escape")
		}
		t.Text = s
		return t

	case isIdentStart(r):
		l.eatWhile(isIdentChar)
		return tok(tokIdent)

	case '0' <= r && r <= '9':
		l.eatWhile(isDigit)
		frac := false
		if l.off+1 < len(l.src) && l.src[l.off] == '.' && isDigit(l.src[l.off+1]) {
			l.advance()
			l.eatWhile(isDigit)
			frac = true
		}
		unitStart := l.off
		// Swallow the whole trailing word so "5ms" is one bad token rather
		// than a duration followed by a stray identifier.
		l.eatWhile(isIdentChar)
		switch unit := l.src[unitStart:l.off]; {
		case unit == "":
			return tok(tokNumber)
		case !frac && (unit == "s" || unit == "m" || unit == "h" || unit == "d"):
			return tok(tokDuration)
		default:
			return illegal("bad duration unit")
		}
	}

	// Two-character operators must be tried before their one-character
	// prefixes or "|=" would lex as "|" then "=".
	switch r {
	case '{':
		return tok(tokLBrace)
	case '}':
		return tok(tokRBrace)
	case '(':
		return tok(tokLParen)
	case ')':
		return tok(tokRParen)
	case ',':
		return tok(tokComma)
	case '|':
		if l.eat('=') {
			return tok(tokPipeExact)
		}
		if l.eat('~') {
			return tok(tokPipeRe)
		}
		return tok(tokPipe)
	case '=':
		if l.eat('~') {
			return tok(tokRe)
		}
		return tok(tokEq)
	case '!':
		if l.eat('=') {
			return tok(tokNeq)
		}
		if l.eat('~') {
			return tok(tokNre)
		}
	case '>':
		if l.eat('=') {
			return tok(tokGte)
		}
		return tok(tokGt)
	case '<':
		if l.eat('=') {
			return tok(tokLte)
		}
		return tok(tokLt)
	}
	// A real U+FFFD in the source is three bytes; a one-byte RuneError is the
	// decoder's marker for a byte that is not UTF-8 at all.
	if r == utf8.RuneError && l.off-start == 1 {
		return illegal("invalid UTF-8")
	}
	return illegal(fmt.Sprintf("unexpected %q", r))
}

// peek returns the rune at the cursor without consuming it. Width 0 means end
// of input; width 1 with utf8.RuneError means an invalid byte.
func (l *lexer) peek() (rune, int) {
	if l.off >= len(l.src) {
		return utf8.RuneError, 0
	}
	return utf8.DecodeRuneInString(l.src[l.off:])
}

// advance consumes one rune. Columns count runes rather than bytes so error
// positions line up with what a terminal shows for non-ASCII input.
func (l *lexer) advance() rune {
	r, w := l.peek()
	if w == 0 {
		return r
	}
	l.off += w
	if r == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
	return r
}

func (l *lexer) eat(c byte) bool {
	if l.off < len(l.src) && l.src[l.off] == c {
		l.advance()
		return true
	}
	return false
}

func (l *lexer) eatWhile(pred func(byte) bool) {
	for l.off < len(l.src) && pred(l.src[l.off]) {
		l.advance()
	}
}

// The character classes are all ASCII, so byte tests are exact: no byte of a
// multi-byte UTF-8 sequence is below 0x80.
func isSpace(c byte) bool      { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }
func isDigit(c byte) bool      { return '0' <= c && c <= '9' }
func isIdentStart(r rune) bool { return r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') }
func isIdentChar(c byte) bool  { return isIdentStart(rune(c)) || isDigit(c) }
