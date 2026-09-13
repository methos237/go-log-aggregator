package cluster

import (
	"reflect"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/query"
)

func TestStatementRoundTrip(t *testing.T) {
	when := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	in := query.Stmt{SQL: "SELECT 1", Args: []any{
		[]int64{1, 2}, when, "text", int16(4), 100, 2.5, 5 * time.Minute, []int64(nil),
	}}
	wire, err := stmtToProto(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := stmtFromProto(wire)
	if err != nil {
		t.Fatal(err)
	}
	// A nil id slot arrives as an empty slice: the peer binds an empty array,
	// which matches nothing, rather than a bare NULL.
	in.Args[len(in.Args)-1] = []int64{}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip changed the statement:\n in  %#v\n out %#v", in, out)
	}
}

func TestStatementRejectsUnknownArgType(t *testing.T) {
	if _, err := stmtToProto(query.Stmt{SQL: "x", Args: []any{uint8(1)}}); err == nil {
		t.Error("stmtToProto accepted an argument type the wire format cannot carry")
	}
}

func TestEveryPlannerArgTypeIsCarried(t *testing.T) {
	// Every golden statement's arguments must survive the wire, which pins the
	// planner's argument types to the proto: adding one without a case here
	// fails this test before it fails on a peer.
	srcs := []string{
		`{service=~"api-.*", level>="warn", region="eu"} |= "x" | json | status >= 500 | rate(5m) by (level, host)`,
		`{service="api"} | logfmt | route = "/x" | count_over_time(1h)`,
		`{service="api"} | regexp "(?P<m>\\w+)" | m =~ "GET" | bytes_over_time(1m)`,
	}
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, src := range srcs {
		q, err := query.Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := query.Compile(q, query.Request{Start: start, End: start.Add(time.Hour), Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		plan.Logs.Args[0] = []int64{1}
		wire, err := stmtToProto(plan.Logs)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		back, err := stmtFromProto(wire)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if !reflect.DeepEqual(plan.Logs, back) {
			t.Errorf("%s: statement changed over the wire:\n %#v\n %#v", src, plan.Logs, back)
		}
	}
}
