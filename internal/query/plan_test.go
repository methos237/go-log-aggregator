package query

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite the golden files under testdata/plan")

var (
	goldenStart   = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	goldenRequest = Request{Start: goldenStart, End: goldenStart.Add(time.Hour), Limit: 100}
)

// goldenCases are the planner's fixtures. The query source is written into
// each golden file's header so the integration test can replay every case
// against a real schema without a second copy of this table.
var goldenCases = []struct {
	name    string
	src     string
	forward bool
}{
	{name: "service_eq", src: `{service="api"}`},
	{name: "service_neq", src: `{service!="api"}`},
	{name: "service_re", src: `{service=~"api-.*"}`},
	{name: "service_nre", src: `{service!~"api-.*"}`},
	{name: "service_gte", src: `{service>="api"}`},
	{name: "service_lte", src: `{service<="api"}`},
	{name: "service_gt", src: `{service>"api"}`},
	{name: "service_lt", src: `{service<"api"}`},
	{name: "host_and_env", src: `{host="web-1", env!="dev"}`},
	{name: "extra_eq", src: `{region="eu"}`},
	{name: "extra_neq", src: `{region!="eu"}`},
	{name: "extra_regex", src: `{region=~"eu-.*"}`},
	{name: "extra_gte", src: `{shard>="10"}`},
	{name: "level_gte_with_service", src: `{service="api", level>="warn"}`},
	{name: "level_only", src: `{level>="warn"}`},
	{name: "level_eq_and_neq", src: `{level="info", level!="debug"}`},
	{name: "forward", src: `{service="api"}`, forward: true},
	{name: "mixed_everything", src: `{service=~"api-.*", level>="warn", env!="dev", region="eu", shard!~"1.*", host="web-1"}`},
	{name: "line_contains", src: `{service="api"} |= "time%out_"`},
	{name: "line_not_contains", src: `{service="api"} != "healthcheck"`},
	{name: "line_regex", src: `{service="api"} |~ "timeout|deadline"`},
	{name: "line_not_regex", src: `{service="api"} !~ "^GET /healthz"`},
	{name: "fields_text", src: `{service="api"} | status = "500"`},
	{name: "fields_numeric", src: `{service="api"} | status >= 500`},
	{name: "fields_regex", src: `{service="api"} | path =~ "/v1/.*"`},
	{name: "level_stage", src: `{service="api"} | level >= "warn"`},
	{name: "json_text", src: `{service="api"} | json | user != "root"`},
	{name: "json_numeric", src: `{service="api"} | json | latency_ms > 250.5`},
	{name: "logfmt_text", src: `{service="api"} | logfmt | method = "GET"`},
	{name: "regexp_numeric", src: `{service="api"} | regexp "^(?P<method>\\w+) \\S+ (?P<status>\\d{3})$" | status >= 500`},
	{name: "agg_rate_1h", src: `{service="api"} | rate(1h)`},
	{name: "agg_rate_1m_by_level", src: `{service="api", level>="warn"} | rate(5m) by (level)`},
	{name: "agg_count_by_service_env", src: `{env="prod"} | count_over_time(1h) by (service, env)`},
	{name: "agg_bytes", src: `{service="api"} | bytes_over_time(5m)`},
	{name: "agg_raw_by_field", src: `{service="api"} | rate(5m) by (status)`},
	{name: "agg_after_pipeline", src: `{service="api"} |= "GET" | json | status >= 500 | count_over_time(1m) by (route, level)`},
	{name: "agg_forward", src: `{service="api"} | rate(1m)`, forward: true},
	{name: "pipeline_everything", src: `{service="api", level>="info"} |= "GET" !~ "healthz" | logfmt | route = "/v1/query" | json | status >= 400 | level != "warn"`},
}

func render(src string, p *Plan) string {
	var b strings.Builder
	b.WriteString("-- query\n" + src + "\n-- source\n" + p.Source + "\n")
	for _, s := range []struct {
		name string
		stmt Stmt
	}{{"streams", p.Streams}, {"logs", p.Logs}} {
		fmt.Fprintf(&b, "-- %s\n%s\n-- %s args\n", s.name, s.stmt.SQL, s.name)
		for _, a := range s.stmt.Args {
			fmt.Fprintf(&b, "%T %#v\n", a, a)
		}
	}
	return b.String()
}

func compile(t *testing.T, src string, r Request) *Plan {
	t.Helper()
	q, err := Parse(src)
	if err != nil {
		t.Fatalf("Parse(%s): %v", src, err)
	}
	p, err := Compile(q, r)
	if err != nil {
		t.Fatalf("Compile(%s): %v", src, err)
	}
	return p
}

func TestCompileGolden(t *testing.T) {
	for _, tc := range goldenCases {
		t.Run(tc.name, func(t *testing.T) {
			r := goldenRequest
			if tc.forward {
				r.Direction = Forward
			}
			got := render(tc.src, compile(t, tc.src, r))
			path := filepath.Join("testdata", "plan", tc.name+".golden")
			if *update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run with -update to create it)", err)
			}
			if got != string(want) {
				t.Errorf("plan differs from %s (run with -update to accept)\n--- got\n%s--- want\n%s", path, got, want)
			}
		})
	}
}

func TestChooseSource(t *testing.T) {
	aligned := goldenRequest
	offHour := Request{Start: goldenStart.Add(time.Minute), End: goldenStart.Add(61 * time.Minute), Limit: 100}
	offMinute := Request{Start: goldenStart.Add(time.Second), End: goldenStart.Add(time.Hour), Limit: 100}
	cases := []struct {
		src  string
		r    Request
		want string
	}{
		{`{service="api"}`, aligned, "logs"},
		{`{service="api"} | rate(1h)`, aligned, "logs_rate_1h"},
		{`{service="api"} | rate(2h) by (level, service)`, aligned, "logs_rate_1h"},
		{`{service="api"} | count_over_time(1d)`, aligned, "logs_rate_1h"},
		{`{service="api"} | rate(1h)`, offHour, "logs_rate_1m"},
		{`{service="api"} | rate(5m)`, aligned, "logs_rate_1m"},
		{`{service="api"} | rate(90s)`, aligned, "logs"},
		{`{service="api"} | rate(5m)`, offMinute, "logs"},
		{`{service="api"} | bytes_over_time(5m)`, aligned, "logs"},
		{`{service="api"} |= "x" | rate(5m)`, aligned, "logs"},
		{`{service="api"} | rate(5m) by (status)`, aligned, "logs"},
	}
	for _, tc := range cases {
		if got := compile(t, tc.src, tc.r).Source; got != tc.want {
			t.Errorf("Source(%s, %v..%v) = %q, want %q", tc.src, tc.r.Start, tc.r.End, got, tc.want)
		}
	}
}

func TestCompileRejectsUncapturedLabel(t *testing.T) {
	src := `{service="api"} | regexp "(?P<method>\\w+)" | status >= 500`
	q, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Compile(q, goldenRequest)
	if err == nil || !strings.Contains(err.Error(), `"status"`) {
		t.Errorf("Compile(%s) error = %v, want one naming the label", src, err)
	}
}

func TestNamedGroups(t *testing.T) {
	cases := []struct {
		pat, want string
		groups    map[string]int
	}{
		{`(?P<a>x)`, `(x)`, map[string]int{"a": 1}},
		{`(?<a>x)`, `(x)`, map[string]int{"a": 1}},
		{`(x)(?P<a>y)(?:z)(?P<b>w)`, `(x)(y)(?:z)(w)`, map[string]int{"a": 2, "b": 3}},
		{`\((?P<a>x)`, `\((x)`, map[string]int{"a": 1}},
		{`[(](?P<a>x)`, `[(](x)`, map[string]int{"a": 1}},
		{`[](](?P<a>x)`, `[](](x)`, map[string]int{"a": 1}},
		{`[^]](?P<a>x)`, `[^]](x)`, map[string]int{"a": 1}},
		{`(?i)(?P<a>x)`, `(?i)(x)`, map[string]int{"a": 1}},
		{`a\(?P<b>`, `a\(?P<b>`, map[string]int{}},
	}
	for _, tc := range cases {
		are, groups := namedGroups(tc.pat)
		if are != tc.want || !reflect.DeepEqual(groups, tc.groups) {
			t.Errorf("namedGroups(%q) = %q, %v; want %q, %v", tc.pat, are, groups, tc.want, tc.groups)
		}
	}
}

func TestCompileRejectsBadRequest(t *testing.T) {
	q, err := Parse(`{service="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]Request{
		"zero start":   {End: goldenStart, Limit: 1},
		"end == start": {Start: goldenStart, End: goldenStart, Limit: 1},
		"end < start":  {Start: goldenStart, End: goldenStart.Add(-time.Second), Limit: 1},
		"limit 0":      {Start: goldenStart, End: goldenStart.Add(time.Second)},
	}
	for name, r := range cases {
		if _, err := Compile(q, r); err == nil {
			t.Errorf("%s: Compile accepted %+v", name, r)
		}
	}
}

var placeholder = regexp.MustCompile(`\$(\d+)`)

// checkPlaceholders asserts the statement's placeholders are exactly $1..$n
// for n arguments: no gaps, no duplicates, none out of range.
func checkPlaceholders(t *testing.T, s Stmt) {
	t.Helper()
	seen := make(map[int]bool)
	for _, m := range placeholder.FindAllStringSubmatch(s.SQL, -1) {
		n, _ := strconv.Atoi(m[1])
		if n < 1 || n > len(s.Args) {
			t.Errorf("placeholder $%d out of range for %d args in %q", n, len(s.Args), s.SQL)
		}
		seen[n] = true
	}
	if len(seen) != len(s.Args) {
		t.Errorf("%d distinct placeholders for %d args in %q", len(seen), len(s.Args), s.SQL)
	}
}

func TestCompileNoUserBytesInSQL(t *testing.T) {
	hostile := []string{"'; DROP TABLE logs; --", `" OR 1=1`, "$1", "--", `evil"; --`}
	q := &Query{Selector: Selector{Matchers: []Matcher{
		{Label: "service", Op: OpEq, Value: hostile[0]},
		{Label: "host", Op: OpRe, Value: hostile[1]},
		{Label: "env", Op: OpNeq, Value: hostile[2]},
		{Label: "region", Op: OpEq, Value: hostile[3]},
		{Label: "region", Op: OpNeq, Value: hostile[0]},
		// The lexer would never produce this label name; the AST is built by
		// hand to prove the planner does not rely on it.
		{Label: hostile[4], Op: OpEq, Value: "x"},
		{Label: hostile[4], Op: OpGt, Value: hostile[1]},
	}}, Stages: []Stage{
		LineFilter{Op: LineContains, Text: hostile[0]},
		LineFilter{Op: LineMatches, Text: hostile[1]},
		LabelFilter{Label: hostile[4], Op: OpEq, Value: hostile[2]},
		LabelFilter{Label: hostile[4], Op: OpGte, Value: "1; DROP TABLE logs; --", Numeric: true},
		ParserStage{Kind: ParserJSON},
		LabelFilter{Label: hostile[4], Op: OpNeq, Value: hostile[3]},
		ParserStage{Kind: ParserLogfmt},
		LabelFilter{Label: hostile[4], Op: OpRe, Value: hostile[0]},
		ParserStage{Kind: ParserRegexp, Pattern: "(?P<x>" + hostile[0] + ")"},
		LabelFilter{Label: "x", Op: OpEq, Value: hostile[1]},
	}}
	p, err := Compile(q, goldenRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []Stmt{p.Streams, p.Logs} {
		for _, h := range hostile {
			if h != "$1" && strings.Contains(s.SQL, h) {
				t.Errorf("SQL %q contains user input %q", s.SQL, h)
			}
		}
		checkPlaceholders(t, s)
	}
	// "$1" is legitimately in the SQL as a placeholder; it must not also be a
	// literal, which the count of placeholders already proves. What is left to
	// check is that the value reached the args instead.
	if !containsArg(p.Streams.Args, "$1") {
		t.Errorf("value %q missing from streams args %v", "$1", p.Streams.Args)
	}
	// The numeric literal is the one value the lexer guarantees is digits; the
	// hand-built AST above lies about that, and it must still be an argument.
	if !containsArg(p.Logs.Args, "1; DROP TABLE logs; --") {
		t.Errorf("numeric literal missing from logs args %v", p.Logs.Args)
	}
}

func TestLineFilterEscapesLikeMeta(t *testing.T) {
	var s stmt
	s.lineFilter(LineFilter{Op: LineContains, Text: `50%_\`})
	if got, want := s.args[0], `%50\%\_\\%`; got != want {
		t.Errorf("like arg = %q, want %q", got, want)
	}
}

func containsArg(args []any, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// allowedWords is every bare word a generated statement may contain. The test
// below tokenizes the golden SQL on non-identifier characters, so anything a
// user could smuggle in would show up as an unlisted word.
var allowedWords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "ORDER": true, "BY": true,
	"DESC": true, "ASC": true, "LIMIT": true, "ANY": true, "coalesce": true, "jsonb": true,
	"ILIKE": true, "NOT": true, "CASE": true, "WHEN": true, "THEN": true, "END": true,
	"pg_input_is_valid": true, "numeric": true, "btrim": true, "substring": true, "regexp_match": true,
	"time_bucket": true, "interval": true, "AS": true, "bucket": true, "GROUP": true, "JOIN": true,
	"USING": true, "count": true, "sum": true, "n": true, "float8": true, "octet_length": true,
	"logs_rate_1m": true, "logs_rate_1h": true,
	"streams": true, "logs": true,
	"stream_id": true, "service": true, "host": true, "env": true, "labels": true, "time": true,
	"seq": true, "level": true, "message": true, "trace_id": true, "span_id": true, "fields": true,
}

var words = regexp.MustCompile(`[A-Za-z_$][A-Za-z0-9_]*`)

func TestCompileAllowlistedIdentifiersOnly(t *testing.T) {
	for _, tc := range goldenCases {
		p := compile(t, tc.src, goldenRequest)
		for _, s := range []Stmt{p.Streams, p.Logs} {
			for _, w := range words.FindAllString(s.SQL, -1) {
				if !allowedWords[w] && !placeholder.MatchString(w) {
					t.Errorf("%s: word %q is not on the allow-list in %q", tc.name, w, s.SQL)
				}
			}
			checkPlaceholders(t, s)
		}
	}
}
