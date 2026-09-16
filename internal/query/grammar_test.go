package query

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestGrammarMatchesReference keeps the EBNF in docs/query-language.md
// identical to the one in this package's doc comment, so the reference cannot
// drift from the parser without a failing test.
func TestGrammarMatchesReference(t *testing.T) {
	src, err := os.ReadFile("ast.go")
	if err != nil {
		t.Fatal(err)
	}
	var pkg []string
	for _, l := range strings.Split(string(src), "\n") {
		if strings.HasPrefix(l, "//\t") {
			pkg = append(pkg, strings.TrimPrefix(l, "//\t"))
		}
	}
	doc, err := os.ReadFile("../../docs/query-language.md")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile("(?s)```ebnf\n(.*?)```").FindSubmatch(doc)
	if m == nil {
		t.Fatal("docs/query-language.md has no ```ebnf block")
	}
	ref := strings.TrimRight(string(m[1]), "\n")
	if got := strings.Join(pkg, "\n"); got != ref {
		t.Errorf("grammar differs\n-- ast.go --\n%s\n-- docs/query-language.md --\n%s", got, ref)
	}
}
