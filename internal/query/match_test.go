package query

import (
	"testing"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

func TestEvaluator(t *testing.T) {
	t.Parallel()

	api := model.LabelSet{Service: "api", Host: "web-1", Env: "prod", Extra: map[string]string{"region": "eu"}}
	web := model.LabelSet{Service: "web", Host: "web-2", Env: "prod"}
	warn := &model.LogRecord{Level: model.LevelWarn, Message: "Timeout after 30s"}
	info := &model.LogRecord{Level: model.LevelInfo, Message: "request ok"}

	tests := []struct {
		query       string
		api, web    bool // MatchStream
		warn, info  bool // MatchRecord
		description string
	}{
		{`{service="api"}`, true, false, true, true, "promoted equality"},
		{`{service!="api"}`, false, true, true, true, "promoted negation"},
		{`{service=~"a.*"}`, true, false, true, true, "regex is anchored: a.* means the whole value"},
		{`{service=~"p"}`, false, false, true, true, "regex without anchors does not search"},
		{`{service!~"a.*"}`, false, true, true, true, "negated regex"},
		{`{host>="web-2"}`, false, true, true, true, "text ordering is bytewise"},
		{`{region="eu"}`, true, false, true, true, "extra equality"},
		{`{region=""}`, false, false, true, true, "extra equality needs the key: containment semantics"},
		{`{region!="eu"}`, false, true, true, true, "missing extra compares as empty for !="},
		{`{region=~".*"}`, true, true, true, true, "missing extra compares as empty for regex"},
		{`{service="api", level>="warn"}`, true, false, true, false, "level filters records, not streams"},
		{`{service="api", level!="warn"}`, true, false, false, true, "level negation"},
		{`{service="api"} |= "timeout"`, true, false, true, false, "|= is case-insensitive"},
		{`{service="api"} != "timeout"`, true, false, false, true, "!= line filter"},
		{`{service="api"} |~ "\\d+s"`, true, false, true, false, "|~ is an unanchored search"},
		{`{service="api"} !~ "^Timeout"`, true, false, false, true, "!~ line filter"},
		{`{service="api"} |= "TIMEOUT" |~ "30"`, true, false, true, false, "filters chain"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			q, err := Parse(tt.query)
			if err != nil {
				t.Fatal(err)
			}
			m, err := NewEvaluator(q)
			if err != nil {
				t.Fatal(err)
			}
			if got := m.MatchStream(api); got != tt.api {
				t.Errorf("MatchStream(api) = %v, want %v: %s", got, tt.api, tt.description)
			}
			if got := m.MatchStream(web); got != tt.web {
				t.Errorf("MatchStream(web) = %v, want %v: %s", got, tt.web, tt.description)
			}
			if got := m.MatchRecord(warn); got != tt.warn {
				t.Errorf("MatchRecord(warn) = %v, want %v: %s", got, tt.warn, tt.description)
			}
			if got := m.MatchRecord(info); got != tt.info {
				t.Errorf("MatchRecord(info) = %v, want %v: %s", got, tt.info, tt.description)
			}
		})
	}
}

func TestEvaluatorRejectsWhatNeedsTheExecutor(t *testing.T) {
	t.Parallel()

	tests := []struct{ query, want string }{
		{`{service="api"} | json`, `1:19: tail supports selectors and line filters; | json needs query`},
		{`{service="api"} | status >= 500`, `1:19: tail supports selectors and line filters; label filter | status needs query`},
		{`{service="api"} | rate(5m)`, `1:19: tail cannot aggregate; use query`},
	}
	for _, tt := range tests {
		q, err := Parse(tt.query)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewEvaluator(q)
		if err == nil || err.Error() != tt.want {
			t.Errorf("NewEvaluator(%s) error = %v, want %q", tt.query, err, tt.want)
		}
	}
}
