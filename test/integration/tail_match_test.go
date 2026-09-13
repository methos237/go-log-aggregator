//go:build integration

package integration

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
)

// The live-tail evaluator and the planner must agree on every record: a line
// the tail shows and a query would not find, or the reverse, is the bug the
// roadmap names. Both are run over the same data here and their answers
// compared, so any drift in either shows up as a failing query.
func TestEvaluatorAgreesWithSQL(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)

	streams := map[model.StreamID]model.LabelSet{
		1: {Service: "api", Host: "web-1", Env: "prod", Extra: map[string]string{"region": "eu"}},
		2: {Service: "api", Host: "web-2", Env: "staging", Extra: map[string]string{"region": "us", "tier": "b"}},
		3: {Service: "worker.jobs", Host: "Web-3", Env: "prod"},
		4: {Service: "web", Host: "web-4", Env: "prod", Extra: map[string]string{"region": ""}},
	}
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	messages := []struct {
		level model.Level
		text  string
	}{
		{model.LevelInfo, "request ok"},
		{model.LevelWarn, "Timeout after 30s"},
		{model.LevelError, "upstream 503: timeout"},
		{model.LevelDebug, "50% done, path=/a_b"},
		{model.LevelError, "Exception thrown\n  at foo()\n  at bar()"},
	}

	type key struct {
		stream model.StreamID
		seq    int64
	}
	records := map[key]model.LogRecord{}
	for id, ls := range streams {
		_, err := pool.Exec(ctx, `
			INSERT INTO streams (stream_id, service, host, env, labels)
			VALUES ($1, $2, $3, $4, $5)`, int64(id), ls.Service, ls.Host, ls.Env, orEmpty(ls.Extra))
		require.NoError(t, err)
		for seq, m := range messages {
			rec := model.LogRecord{StreamID: id, Time: start.Add(time.Duration(seq) * time.Second), Seq: int64(seq), Level: m.level, Message: m.text}
			_, err = pool.Exec(ctx, `INSERT INTO logs (time, stream_id, seq, level, message) VALUES ($1, $2, $3, $4, $5)`,
				rec.Time, int64(rec.StreamID), rec.Seq, int16(rec.Level), rec.Message)
			require.NoError(t, err)
			records[key{id, rec.Seq}] = rec
		}
	}

	queries := []string{
		`{service="api"}`,
		`{service!="api"}`,
		`{service=~"a.*"}`,
		`{service=~"api|web"}`,
		`{service!~"w.*"}`,
		`{service="worker.jobs"}`,
		`{host>="web-2"}`,
		`{host<"web-2"}`,
		`{env="prod", region="eu"}`,
		`{region=""}`,
		`{region!="eu"}`,
		`{region=~".*"}`,
		`{region=~"e.|us"}`,
		`{tier!~"b"}`,
		`{tier>"a"}`,
		`{service="api", level>="warn"}`,
		`{service="api", level!="warn"}`,
		`{service="api", level<"info"}`,
		`{env="prod"} |= "timeout"`,
		`{env="prod"} |= "TIMEOUT"`,
		`{env="prod"} != "timeout"`,
		`{env="prod"} |= "50%"`,
		`{env="prod"} |= "_b"`,
		`{env="prod"} |~ "\\d+s"`,
		`{env="prod"} |~ "^Timeout"`,
		`{env="prod"} !~ "^Timeout"`,
		`{env="prod"} |~ "(?i)timeout"`,
		`{env="prod"} |~ "Exception.*at bar"`,
		`{env="prod"} |~ "^  at"`,
		`{env="prod"} !~ "thrown.at"`,
		`{env="prod"} |= "TIMEOUT" |~ "30"`,
		`{env="prod", level>="warn"} |= "timeout" != "upstream"`,
	}
	req := query.Request{Start: start, End: start.Add(time.Hour), Limit: 100}
	for _, src := range queries {
		t.Run(src, func(t *testing.T) {
			q, err := query.Parse(src)
			require.NoError(t, err)
			ev, err := query.NewEvaluator(q)
			require.NoError(t, err)

			var want []string
			for k, rec := range records {
				if ev.MatchStream(streams[k.stream]) && ev.MatchRecord(&rec) {
					want = append(want, fmt.Sprintf("%d/%d", k.stream, k.seq))
				}
			}

			res, err := executor.Run(ctx, pool, q, req)
			require.NoError(t, err)
			var got []string
			for _, rec := range res.Records {
				got = append(got, fmt.Sprintf("%d/%d", rec.StreamID, rec.Seq))
			}
			sort.Strings(want)
			sort.Strings(got)
			require.Equal(t, want, got, "evaluator (want) and SQL (got) disagree")
		})
	}
}

func orEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
