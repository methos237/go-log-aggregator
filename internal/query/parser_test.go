package query

import (
	"reflect"
	"strings"
	"testing"
)

// roadmapExamples are the §4 queries the roadmap says the suite must cover.
// They double as fuzz seeds.
var roadmapExamples = []string{
	`{service="api"}`,
	`{service="api", env!="dev"} |~ "timeout|deadline"`,
	`{service=~"api-.*", level>="warn"} != "healthcheck"`,
	`{service="api"} | json | rate(5m) by (level)`,
}

type parseCase struct {
	src  string
	want *Query
}

type errCase struct {
	src     string
	wantErr string
}

func at(line, col int) Pos { return Pos{Line: line, Col: col} }

func matcher(line, col int, label string, op Op, value string) Matcher {
	return Matcher{Pos: at(line, col), Label: label, Op: op, Value: value}
}

func runParseCases(t *testing.T, cases []parseCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.src, func(t *testing.T) {
			got, err := Parse(tc.src)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.src, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Parse(%q)\n got %#v\nwant %#v", tc.src, got, tc.want)
			}
		})
	}
}

func runErrCases(t *testing.T, cases []errCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.src, func(t *testing.T) {
			got, err := Parse(tc.src)
			if err == nil {
				t.Fatalf("Parse(%q) = %#v, want error %q", tc.src, got, tc.wantErr)
			}
			if got != nil {
				t.Errorf("Parse(%q) returned non-nil query with error", tc.src)
			}
			if err.Error() != tc.wantErr {
				t.Errorf("Parse(%q) error\n got %q\nwant %q", tc.src, err.Error(), tc.wantErr)
			}
		})
	}
}

func TestParseSelectors(t *testing.T) {
	runParseCases(t, []parseCase{
		{`{service="api"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "service", OpEq, "api"),
		}}}},
		{`{service!="api"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "service", OpNeq, "api"),
		}}}},
		{`{service=~"api-.*"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "service", OpRe, "api-.*"),
		}}}},
		{`{service!~"api-.*"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "service", OpNre, "api-.*"),
		}}}},
		{`{version>="2"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "version", OpGte, "2"),
		}}}},
		{`{version<="2"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "version", OpLte, "2"),
		}}}},
		{`{version>"2"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "version", OpGt, "2"),
		}}}},
		{`{version<"2"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "version", OpLt, "2"),
		}}}},
		{`{service="api", env!="dev", region=~"us-.*"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "service", OpEq, "api"),
			matcher(1, 17, "env", OpNeq, "dev"),
			matcher(1, 29, "region", OpRe, "us-.*"),
		}}}},
		{"  {\n\tservice = \"api\" ,\n\tenv=\"dev\"\n}\n", &Query{Selector: Selector{Pos: at(1, 3), Matchers: []Matcher{
			matcher(2, 2, "service", OpEq, "api"),
			matcher(3, 2, "env", OpEq, "dev"),
		}}}},
		{`{level="warn"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpEq, "warn"),
		}}}},
		{`{level!="debug"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpNeq, "debug"),
		}}}},
		{`{level>="warn"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpGte, "warn"),
		}}}},
		{`{level<="info"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpLte, "info"),
		}}}},
		{`{level>"info"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpGt, "info"),
		}}}},
		{`{level<"error"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpLt, "error"),
		}}}},
		// Aliases and case are ParseLevel's business; the parser keeps the text.
		{`{level="WARNING"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "level", OpEq, "WARNING"),
		}}}},
		{"{path=~`^/v1/\\d+$`}", &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "path", OpRe, `^/v1/\d+$`),
		}}}},
		{`{msg="say \"hi\"\n"}`, &Query{Selector: Selector{Pos: at(1, 1), Matchers: []Matcher{
			matcher(1, 2, "msg", OpEq, "say \"hi\"\n"),
		}}}},
	})
}

var selectorErrorCases = []errCase{
	{`{}`, `1:1: selector needs at least one matcher`},
	{`{service}`, `1:9: expected matcher operator, got "}"`},
	{`{service=}`, `1:10: expected string, got "}"`},
	{`{service=500}`, `1:10: expected string, got number "500"`},
	{`{service="a",}`, `1:14: expected label name, got "}"`},
	{`{service="a"`, `1:13: expected "}" or ",", got end of input`},
	{`{service="a" env="b"}`, `1:14: expected "}" or ",", got identifier "env"`},
	{`service="a"}`, `1:1: expected "{", got identifier "service"`},
	{``, `1:1: expected "{", got end of input`},
	{`{level=~"warn"}`, `1:7: level does not support =~`},
	{`{level!~"warn"}`, `1:7: level does not support !~`},
	{`{level="warnn"}`, `1:8: unknown level "warnn"`},
	{`{service=~"("}`, "1:11: invalid regex: error parsing regexp: missing closing ): `(`"},
	{`{service=~"` + strings.Repeat("a", MaxRegexLen+1) + `"}`, `1:11: regex is 1025 bytes, max 1024`},
	{`{service="a`, `1:10: unterminated string`},
	{`{service!"a"}`, `1:9: unexpected '!'`},
	{"{service=\"a\"}\xff", `1:14: invalid UTF-8`},
	{"\xff{service=\"a\"}", `1:1: invalid UTF-8`},
	{`{service="a"} }`, `1:15: expected line filter, "|" or end of input, got "}"`},
}

func TestParseSelectorErrors(t *testing.T) {
	runErrCases(t, selectorErrorCases)
}

// api is the selector every stage and aggregation case starts with; it ends
// at column 15 so the next token starts at 17.
var api = Selector{Pos: at(1, 1), Matchers: []Matcher{matcher(1, 2, "service", OpEq, "api")}}

func TestParseStages(t *testing.T) {
	runParseCases(t, []parseCase{
		{`{service="api"} |= "timeout"`, &Query{Selector: api, Stages: []Stage{
			LineFilter{Pos: at(1, 17), Op: LineContains, Text: "timeout"},
		}}},
		{`{service="api"} != "healthcheck"`, &Query{Selector: api, Stages: []Stage{
			LineFilter{Pos: at(1, 17), Op: LineNotContains, Text: "healthcheck"},
		}}},
		{`{service="api"} |~ "timeout|deadline"`, &Query{Selector: api, Stages: []Stage{
			LineFilter{Pos: at(1, 17), Op: LineMatches, Text: "timeout|deadline"},
		}}},
		{`{service="api"} !~ "^DEBUG"`, &Query{Selector: api, Stages: []Stage{
			LineFilter{Pos: at(1, 17), Op: LineNotMatches, Text: "^DEBUG"},
		}}},
		{`{service="api"} | json`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
		}}},
		{`{service="api"} | logfmt`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserLogfmt},
		}}},
		{"{service=\"api\"} | regexp `(?P<status>\\d{3})`", &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserRegexp, Pattern: `(?P<status>\d{3})`},
		}}},
		{`{service="api"} | json | status >= 500`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "status", Op: OpGte, Value: "500", Numeric: true},
		}}},
		{`{service="api"} | json | status = "500"`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "status", Op: OpEq, Value: "500"},
		}}},
		{`{service="api"} | json | latency < 1.5`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "latency", Op: OpLt, Value: "1.5", Numeric: true},
		}}},
		{`{service="api"} | json | path != "/health"`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "path", Op: OpNeq, Value: "/health"},
		}}},
		{`{service="api"} | json | user =~ "bob.*"`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "user", Op: OpRe, Value: "bob.*"},
		}}},
		{`{service="api"} | json | user !~ "bob.*"`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "user", Op: OpNre, Value: "bob.*"},
		}}},
		{`{service="api"} | level >= "warn"`, &Query{Selector: api, Stages: []Stage{
			LabelFilter{Pos: at(1, 19), Label: "level", Op: OpGte, Value: "warn"},
		}}},
		// Line filters may follow a parser stage; the grammar does not order them.
		{`{service="api"} | json |= "x"`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LineFilter{Pos: at(1, 24), Op: LineContains, Text: "x"},
		}}},
		{`{service="api"} | json | status > 400 !~ "retry" | logfmt`, &Query{Selector: api, Stages: []Stage{
			ParserStage{Pos: at(1, 19), Kind: ParserJSON},
			LabelFilter{Pos: at(1, 26), Label: "status", Op: OpGt, Value: "400", Numeric: true},
			LineFilter{Pos: at(1, 39), Op: LineNotMatches, Text: "retry"},
			ParserStage{Pos: at(1, 52), Kind: ParserLogfmt},
		}}},
	})
}

const wantStage = `expected a stage (json, logfmt, regexp, a label filter, or an aggregation)`

var stageErrorCases = []errCase{
	{`{service="api"} | foo`, `1:19: ` + wantStage + `, got identifier "foo"`},
	{`{service="api"} | status`, `1:19: ` + wantStage + `, got identifier "status"`},
	{`{service="api"} |`, `1:18: ` + wantStage + `, got end of input`},
	{`{service="api"} | "x"`, `1:19: ` + wantStage + `, got string "x"`},
	{`{service="api"} | regexp`, `1:25: expected string, got end of input`},
	{`{service="api"} | regexp "("`, "1:26: invalid regex: error parsing regexp: missing closing ): `(`"},
	{`{service="api"} | json extra`, `1:24: expected line filter, "|" or end of input, got identifier "extra"`},
	{`{service="api"} | json = "x"`, `1:24: expected line filter, "|" or end of input, got "="`},
	{`{service="api"} | status =~ 500`, `1:29: expected string, got number "500"`},
	{`{service="api"} | status =`, `1:27: expected string, got end of input`},
	{`{service="api"} |= 500`, `1:20: expected string, got number "500"`},
	{`{service="api"} |~ "("`, "1:20: invalid regex: error parsing regexp: missing closing ): `(`"},
	{`{service="api"} | level =~ "warn"`, `1:25: level does not support =~`},
	{`{service="api"} | level = "warnn"`, `1:27: unknown level "warnn"`},
	{`{service="api"} | level >= 4`, `1:28: expected string, got number "4"`},
}

func TestParseStageErrors(t *testing.T) {
	runErrCases(t, stageErrorCases)
}

func TestErrorFormat(t *testing.T) {
	if got := (&Error{Pos: at(1, 17), Msg: "x"}).Error(); got != "1:17: x" {
		t.Errorf("Error() = %q, want %q", got, "1:17: x")
	}
}
