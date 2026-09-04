package model

import (
	"maps"
	"time"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
)

// Conversions between the protobuf wire types and the model.
//
// These live here, next to the validation they feed, rather than in the ingest
// service: the loadgen, the agent and the gRPC handler all need them, and a copy
// per caller is how the two representations drift apart.
//
// Nothing here validates. Conversion is total — every wire message maps to some
// model value — and the caller then runs Validate. Keeping the two separate means
// an invalid record can still be logged in full when it is rejected.

// LabelSetFromProto converts a wire label set. A nil message yields the zero
// LabelSet, which Validate then rejects for having no service.
func LabelSetFromProto(pb *logaggv1.LabelSet) LabelSet {
	if pb == nil {
		return LabelSet{}
	}
	ls := LabelSet{
		Service: pb.GetService(),
		Host:    pb.GetHost(),
		Env:     pb.GetEnv(),
	}
	if extra := pb.GetExtra(); len(extra) > 0 {
		// Cloned: the protobuf message may be pooled or reused by the gRPC stream
		// once the handler returns, and the stream ID is derived from this map.
		ls.Extra = maps.Clone(extra)
	}
	return ls
}

// Proto converts a label set to its wire form.
func (ls LabelSet) Proto() *logaggv1.LabelSet {
	pb := &logaggv1.LabelSet{
		Service: ls.Service,
		Host:    ls.Host,
		Env:     ls.Env,
	}
	if len(ls.Extra) > 0 {
		pb.Extra = maps.Clone(ls.Extra)
	}
	return pb
}

// LogRecordFromProto converts a wire record, attaching the stream it was resolved
// to.
//
// A zero time_unix_nano becomes the zero time.Time rather than the Unix epoch, so
// "the agent never set a timestamp" is distinguishable from "the agent claims
// 1970" and Validate can reject it with ErrMissingTime.
func LogRecordFromProto(streamID StreamID, pb *logaggv1.LogRecord) LogRecord {
	if pb == nil {
		return LogRecord{StreamID: streamID}
	}
	rec := LogRecord{
		StreamID: streamID,
		Seq:      pb.GetSeq(),
		Level:    Level(pb.GetLevel()),
		Message:  pb.GetMessage(),
		TraceID:  pb.GetTraceId(),
		SpanID:   pb.GetSpanId(),
	}
	if ns := pb.GetTimeUnixNano(); ns != 0 {
		rec.Time = time.Unix(0, ns).UTC()
	}
	if fields := pb.GetFields(); len(fields) > 0 {
		rec.Fields = maps.Clone(fields)
	}
	return rec
}

// Proto converts a record to its wire form. StreamID is not carried: it is
// derived from the batch's label set, so sending it would let the two disagree.
func (r *LogRecord) Proto() *logaggv1.LogRecord {
	pb := &logaggv1.LogRecord{
		Seq:     r.Seq,
		Level:   logaggv1.Level(r.Level),
		Message: r.Message,
		TraceId: r.TraceID,
		SpanId:  r.SpanID,
	}
	if !r.Time.IsZero() {
		pb.TimeUnixNano = r.Time.UnixNano()
	}
	if len(r.Fields) > 0 {
		pb.Fields = maps.Clone(r.Fields)
	}
	return pb
}
