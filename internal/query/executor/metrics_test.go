package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
)

type stubRunner struct {
	res *Result
	err error
}

func (s stubRunner) Run(context.Context, *query.Query, query.Request) (*Result, error) {
	return s.res, s.err
}

func TestInstrumentedObservesSuccessOnly(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	ok := Instrumented{Runner: stubRunner{res: &Result{
		Source: "logs", Elapsed: 20 * time.Millisecond, Records: make([]model.LogRecord, 3),
	}}, Metrics: m}
	if _, err := ok.Run(context.Background(), nil, query.Request{}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	failing := Instrumented{Runner: stubRunner{err: errors.New("boom")}, Metrics: m}
	if _, err := failing.Run(context.Background(), nil, query.Request{}); err == nil {
		t.Fatal("Run: want error")
	}

	if got := testutil.CollectAndCount(m.Duration, "logagg_query_duration_seconds"); got != 1 {
		t.Errorf("duration series = %d, want 1 (source=logs only)", got)
	}
	rows := sampleCount(t, reg, "logagg_query_rows_returned")
	if rows != 1 {
		t.Errorf("rows_returned samples = %d, want 1 (failure not observed)", rows)
	}
}

func sampleCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("metric %s not registered", name)
	return 0
}
