package query

import (
	"reflect"
	"testing"
)

// lexAll drains the lexer, capped so a lexer that never reaches EOF fails the
// test instead of hanging it.
func lexAll(t *testing.T, src string) []token {
	t.Helper()
	l := newLexer(src)
	var toks []token
	for range len(src) + 2 {
		tok := l.next()
		toks = append(toks, tok)
		if tok.Kind == tokEOF {
			return toks
		}
	}
	t.Fatalf("lexer did not reach EOF within %d tokens", len(src)+2)
	return nil
}

func tk(kind tokenKind, text string, line, col int) token {
	return token{Kind: kind, Text: text, Pos: Pos{Line: line, Col: col}}
}

func ill(text, err string, line, col int) token {
	t := tk(tokIllegal, text, line, col)
	t.Err = err
	return t
}

func eof(line, col int) token { return tk(tokEOF, "", line, col) }

func TestLexer(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []token
	}{
		{"empty", "", []token{eof(1, 1)}},
		{"whitespace only", " \t\r\n ", []token{eof(2, 2)}},
		{
			"every operator",
			"{ } ( ) , | = != =~ !~ >= <= > < |= |~",
			[]token{
				tk(tokLBrace, "{", 1, 1), tk(tokRBrace, "}", 1, 3),
				tk(tokLParen, "(", 1, 5), tk(tokRParen, ")", 1, 7),
				tk(tokComma, ",", 1, 9), tk(tokPipe, "|", 1, 11),
				tk(tokEq, "=", 1, 13), tk(tokNeq, "!=", 1, 15),
				tk(tokRe, "=~", 1, 18), tk(tokNre, "!~", 1, 21),
				tk(tokGte, ">=", 1, 24), tk(tokLte, "<=", 1, 27),
				tk(tokGt, ">", 1, 30), tk(tokLt, "<", 1, 32),
				tk(tokPipeExact, "|=", 1, 34), tk(tokPipeRe, "|~", 1, 37),
				eof(1, 39),
			},
		},
		{
			"operators without spaces take the longest match",
			"|==~!~>=<=",
			[]token{
				tk(tokPipeExact, "|=", 1, 1), tk(tokRe, "=~", 1, 3),
				tk(tokNre, "!~", 1, 5), tk(tokGte, ">=", 1, 7), tk(tokLte, "<=", 1, 9),
				eof(1, 11),
			},
		},
		{
			"keywords are plain identifiers",
			"json rate by _x1",
			[]token{
				tk(tokIdent, "json", 1, 1), tk(tokIdent, "rate", 1, 6),
				tk(tokIdent, "by", 1, 11), tk(tokIdent, "_x1", 1, 14), eof(1, 17),
			},
		},
		{
			"strings decode escapes",
			`"a\"b" "tab\t" "plain"`,
			[]token{
				tk(tokString, `a"b`, 1, 1), tk(tokString, "tab\t", 1, 8),
				tk(tokString, "plain", 1, 16), eof(1, 23),
			},
		},
		{
			"raw string keeps backslashes",
			"`a\\b\\n` x",
			[]token{tk(tokString, `a\b\n`, 1, 1), tk(tokIdent, "x", 1, 9), eof(1, 10)},
		},
		{
			"numbers",
			"500 1.5",
			[]token{tk(tokNumber, "500", 1, 1), tk(tokNumber, "1.5", 1, 5), eof(1, 8)},
		},
		{
			"trailing dot is not part of the number",
			"5.",
			[]token{tk(tokNumber, "5", 1, 1), ill(".", `unexpected '.'`, 1, 2), eof(1, 3)},
		},
		{
			"durations",
			"5m 30s 1h 7d",
			[]token{
				tk(tokDuration, "5m", 1, 1), tk(tokDuration, "30s", 1, 4),
				tk(tokDuration, "1h", 1, 8), tk(tokDuration, "7d", 1, 11), eof(1, 13),
			},
		},
		{"bad duration unit", "5x", []token{ill("5x", "bad duration unit", 1, 1), eof(1, 3)}},
		{"bad duration unit is one token", "5ms", []token{ill("5ms", "bad duration unit", 1, 1), eof(1, 4)}},
		{"fractional duration is illegal", "1.5m", []token{ill("1.5m", "bad duration unit", 1, 1), eof(1, 5)}},
		{"unterminated string", `x "abc`, []token{tk(tokIdent, "x", 1, 1), ill(`"abc`, "unterminated string", 1, 3), eof(1, 7)}},
		{"unterminated string ending in backslash", `"abc\`, []token{ill(`"abc\`, "unterminated string", 1, 1), eof(1, 6)}},
		{"unterminated raw string", "`abc", []token{ill("`abc", "unterminated string", 1, 1), eof(1, 5)}},
		{"invalid escape", `"\q"`, []token{ill(`"\q"`, "invalid string escape", 1, 1), eof(1, 5)}},
		{"raw newline in quoted string", "\"a\nb\"", []token{ill("\"a\nb\"", "invalid string escape", 1, 1), eof(2, 3)}},
		{"lone bang", "!", []token{ill("!", `unexpected '!'`, 1, 1), eof(1, 2)}},
		{"unexpected byte", "a # b", []token{tk(tokIdent, "a", 1, 1), ill("#", `unexpected '#'`, 1, 3), tk(tokIdent, "b", 1, 5), eof(1, 6)}},
		{"non-ASCII outside a string", "é", []token{ill("é", `unexpected 'é'`, 1, 1), eof(1, 2)}},
		{"rune whose low byte is a digit is not a number", "\u0130", []token{ill("\u0130", "unexpected '\u0130'", 1, 1), eof(1, 2)}},
		{"invalid UTF-8", "\xff", []token{ill("\xff", "invalid UTF-8", 1, 1), eof(1, 2)}},
		{
			"multi-line positions",
			"{\n  a=\"b\"\n}",
			[]token{
				tk(tokLBrace, "{", 1, 1),
				tk(tokIdent, "a", 2, 3), tk(tokEq, "=", 2, 4), tk(tokString, "b", 2, 5),
				tk(tokRBrace, "}", 3, 1), eof(3, 2),
			},
		},
		{
			"columns count runes not bytes",
			`"é" x`,
			[]token{tk(tokString, "é", 1, 1), tk(tokIdent, "x", 1, 5), eof(1, 6)},
		},
		{
			"full query",
			`{service="api"} | json | rate(5m) by (level)`,
			[]token{
				tk(tokLBrace, "{", 1, 1), tk(tokIdent, "service", 1, 2), tk(tokEq, "=", 1, 9),
				tk(tokString, "api", 1, 10), tk(tokRBrace, "}", 1, 15),
				tk(tokPipe, "|", 1, 17), tk(tokIdent, "json", 1, 19),
				tk(tokPipe, "|", 1, 24), tk(tokIdent, "rate", 1, 26), tk(tokLParen, "(", 1, 30),
				tk(tokDuration, "5m", 1, 31), tk(tokRParen, ")", 1, 33),
				tk(tokIdent, "by", 1, 35), tk(tokLParen, "(", 1, 38), tk(tokIdent, "level", 1, 39),
				tk(tokRParen, ")", 1, 44), eof(1, 45),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lexAll(t, tt.src)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("lex(%q)\n got: %+v\nwant: %+v", tt.src, got, tt.want)
			}
		})
	}
}

func TestLexerEOFIsSticky(t *testing.T) {
	l := newLexer("a")
	if got := l.next(); got.Kind != tokIdent {
		t.Fatalf("first token = %v, want identifier", got.Kind)
	}
	for i := range 3 {
		got := l.next()
		if got.Kind != tokEOF || got.Pos != (Pos{Line: 1, Col: 2}) {
			t.Errorf("next() #%d after end = %+v, want EOF at 1:2", i, got)
		}
	}
}

func TestTokenKindString(t *testing.T) {
	tests := map[tokenKind]string{
		tokEOF:        "end of input",
		tokIdent:      "identifier",
		tokString:     "string",
		tokDuration:   "duration",
		tokLBrace:     `"{"`,
		tokPipeExact:  `"|="`,
		tokenKind(99): "tokenKind(99)",
	}
	for k, want := range tests {
		if got := k.String(); got != want {
			t.Errorf("tokenKind(%d).String() = %q, want %q", int(k), got, want)
		}
	}
}
