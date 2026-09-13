package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestQueryBodyRange(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	opt := queryOptions{since: 15 * time.Minute, limit: 7}
	body, err := opt.body(`{service="api"}`, now)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"query": `{service="api"}`, "limit": 7.0, "start": "2026-09-01T11:45:00Z", "end": "2026-09-01T12:00:00Z"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if _, ok := got["direction"]; ok {
		t.Error("direction should be omitted unless -forward")
	}

	opt = queryOptions{since: time.Hour, start: "2026-09-01T00:00:00Z", end: "not a time"}
	if _, err := opt.body("x", now); err == nil || !strings.Contains(err.Error(), "-end") {
		t.Errorf("bad -end: err = %v", err)
	}
}

func TestQueryPrint(t *testing.T) {
	var resp queryResponse
	err := json.Unmarshal([]byte(`{"records":[{"time":"2026-09-01T00:00:01Z","level":"warn","message":"slow","fields":{"b":"2","a":"1"}}],"source":"logs","streams":1,"elapsed_ms":3.25}`), &resp)
	if err != nil {
		t.Fatal(err)
	}
	var out, summary bytes.Buffer
	if err = resp.print(&out, &summary); err != nil {
		t.Fatal(err)
	}
	if want := "2026-09-01T00:00:01Z  warn   slow  a=1 b=2\n"; out.String() != want {
		t.Errorf("out = %q, want %q", out.String(), want)
	}
	if want := "1 rows from logs (1 streams) in 3.2ms\n"; summary.String() != want {
		t.Errorf("summary = %q, want %q", summary.String(), want)
	}

	resp = queryResponse{}
	err = json.Unmarshal([]byte(`{"points":[{"bucket":"2026-09-01T00:00:00Z","value":0.5,"labels":{"level":"info"}}],"source":"logs_rate_1m","streams":2,"elapsed_ms":1}`), &resp)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := resp.print(&out, &summary); err != nil {
		t.Fatal(err)
	}
	if want := "2026-09-01T00:00:00Z  0.5  level=info\n"; out.String() != want {
		t.Errorf("points out = %q, want %q", out.String(), want)
	}
}
