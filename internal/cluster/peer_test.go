package cluster

import (
	"reflect"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
)

func TestRowsRoundTrip(t *testing.T) {
	when := time.Date(2026, 9, 1, 0, 0, 1, 500, time.UTC)
	records := []model.LogRecord{
		{StreamID: -7, Time: when, Seq: 3, Level: model.LevelWarn, Message: "slow", TraceID: []byte{1, 2}, Fields: map[string]string{"a": "1"}},
		{StreamID: 9, Time: when, Seq: 4, Level: model.LevelInfo, Message: "ok"},
	}
	if got := recordsFromProto(recordsToProto(records)); !reflect.DeepEqual(got, records) {
		t.Errorf("records changed over the wire:\n %#v\n %#v", records, got)
	}
	points := []executor.Point{
		{Bucket: when, Value: 0.5, Labels: map[string]string{"level": "info"}},
		{Bucket: when, Value: 2},
	}
	if got := pointsFromProto(pointsToProto(points)); !reflect.DeepEqual(got, points) {
		t.Errorf("points changed over the wire:\n %#v\n %#v", points, got)
	}
	if recordsFromProto(nil) != nil || pointsFromProto(nil) != nil {
		t.Error("empty replies should decode to nil, matching a local execution with no rows")
	}
}
