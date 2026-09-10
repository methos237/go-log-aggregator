package query

import "testing"

func FuzzLexer(f *testing.F) {
	for _, seed := range []string{
		`{service="api"}`,
		`{service="api", env!="dev"} |~ "timeout|deadline"`,
		`{service=~"api-.*", level>="warn"} != "healthcheck"`,
		`{service="api"} | json | rate(5m) by (level)`,
		`"unterminated`,
		"`unterminated raw",
		"\xff",
		`\`,
		"\"\xff\"",
		"5m5x5.5.5",
		"|=~!~>=<=!",
		"{\n\"a\nb\"\n}",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, src string) {
		l := newLexer(src)
		var prev Pos
		for i := 0; ; i++ {
			if i > len(src)+1 {
				t.Fatalf("no EOF within %d tokens", len(src)+1)
			}
			tok := l.next()
			if tok.Pos.Line < prev.Line || (tok.Pos.Line == prev.Line && tok.Pos.Col < prev.Col) {
				t.Fatalf("position went backwards: %+v after %+v", tok.Pos, prev)
			}
			prev = tok.Pos
			if tok.Kind == tokIllegal && tok.Err == "" {
				t.Fatalf("illegal token without Err: %+v", tok)
			}
			if tok.Kind == tokEOF {
				return
			}
		}
	})
}
