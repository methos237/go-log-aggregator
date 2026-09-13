package cluster

import (
	"reflect"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
)

var t0 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func rec(stream int64, sec int, seq int64) model.LogRecord {
	return model.LogRecord{StreamID: model.StreamID(stream), Time: t0.Add(time.Duration(sec) * time.Second), Seq: seq}
}

func TestMergeRecordsBackward(t *testing.T) {
	// Each shard is newest first, as the database returns it; the same
	// timestamp across shards orders by seq, and the limit stops the merge.
	a := []model.LogRecord{rec(1, 5, 9), rec(1, 3, 7), rec(1, 1, 5)}
	b := []model.LogRecord{rec(2, 4, 8), rec(2, 3, 6), rec(2, 0, 4)}
	got := mergeRecords([][]model.LogRecord{a, b, nil}, query.Backward, 5)
	want := []model.LogRecord{rec(1, 5, 9), rec(2, 4, 8), rec(1, 3, 7), rec(2, 3, 6), rec(1, 1, 5)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merge = %v\nwant    %v", seqs(got), seqs(want))
	}
}

func TestMergeRecordsForward(t *testing.T) {
	a := []model.LogRecord{rec(1, 1, 5), rec(1, 3, 7)}
	b := []model.LogRecord{rec(2, 0, 4), rec(2, 3, 6)}
	got := mergeRecords([][]model.LogRecord{a, b}, query.Forward, 10)
	want := []model.LogRecord{rec(2, 0, 4), rec(1, 1, 5), rec(2, 3, 6), rec(1, 3, 7)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merge = %v\nwant    %v", seqs(got), seqs(want))
	}
	if got := mergeRecords(nil, query.Forward, 10); len(got) != 0 {
		t.Errorf("merge of nothing = %v", got)
	}
}

func seqs(rs []model.LogRecord) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = r.Seq
	}
	return out
}

func TestMergePointsSumsAcrossShards(t *testing.T) {
	m1 := t0.Add(time.Minute)
	a := []executor.Point{
		{Bucket: m1, Value: 2, Labels: map[string]string{"level": "info"}},
		{Bucket: t0, Value: 1, Labels: map[string]string{"level": "warn"}},
	}
	b := []executor.Point{
		{Bucket: m1, Value: 3, Labels: map[string]string{"level": "info"}},
		{Bucket: m1, Value: 4},
		{Bucket: t0, Value: 5, Labels: map[string]string{"level": "error"}},
	}
	got := mergePoints([][]executor.Point{a, b}, query.Backward, []string{"level"})
	want := []executor.Point{
		{Bucket: m1, Value: 5, Labels: map[string]string{"level": "info"}},
		{Bucket: m1, Value: 4},
		{Bucket: t0, Value: 5, Labels: map[string]string{"level": "error"}},
		{Bucket: t0, Value: 1, Labels: map[string]string{"level": "warn"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merge =\n%+v\nwant\n%+v", got, want)
	}

	// No by: one point per bucket, values summed, forward order.
	got = mergePoints([][]executor.Point{{{Bucket: m1, Value: 1}}, {{Bucket: t0, Value: 2}, {Bucket: m1, Value: 3}}}, query.Forward, nil)
	want = []executor.Point{{Bucket: t0, Value: 2}, {Bucket: m1, Value: 4}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merge without by = %+v, want %+v", got, want)
	}
}
