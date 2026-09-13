package query

import "testing"

// FuzzCompile drives arbitrary text through the whole compiler and checks the
// SQL-injection invariants on whatever comes out: every bare word in the
// generated SQL is on the allow-list and every placeholder has an argument.
// Any user byte that leaked into statement text would surface as an unlisted
// word, since the lexer's identifiers and the fuzzer's strings are exactly
// what a smuggled value would look like.
func FuzzCompile(f *testing.F) {
	for _, tc := range goldenCases {
		f.Add(tc.src)
	}
	f.Fuzz(func(t *testing.T, src string) {
		q, err := Parse(src)
		if err != nil {
			return
		}
		p, err := Compile(q, goldenRequest)
		if (p == nil) == (err == nil) {
			t.Fatalf("Compile(%q) = %v, %v: want exactly one of plan and error", src, p, err)
		}
		if err != nil {
			return
		}
		for _, s := range []Stmt{p.Streams, p.Logs} {
			if err := CheckSQL(s.SQL); err != nil {
				t.Fatalf("Compile(%q): %v in %q", src, err, s.SQL)
			}
			checkPlaceholders(t, s)
		}
	})
}
