package main

import (
	"strings"
	"testing"
	"time"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/model"
)

func testOptions() options {
	return options{
		records:     1000,
		batchSize:   10,
		streams:     4,
		senders:     2,
		messageSize: 120,
		env:         "dev",
	}
}

// Generated records have to pass the collector's own validation, or the tool
// measures the rejection path instead of the write path.
func TestGeneratedBatchesAreValid(t *testing.T) {
	t.Parallel()

	opt := testOptions()
	s := &sender{id: 0, opt: &opt}

	for i := int64(0); i < 4; i++ {
		batch := s.batch(i)

		labels := model.LabelSetFromProto(batch.GetLabels())
		if err := labels.Validate(); err != nil {
			t.Fatalf("batch %d labels invalid: %v", i, err)
		}
		if len(batch.GetRecords()) != opt.batchSize {
			t.Fatalf("batch %d has %d records, want %d", i, len(batch.GetRecords()), opt.batchSize)
		}

		for _, pb := range batch.GetRecords() {
			rec := model.LogRecordFromProto(labels.ID(), pb)
			if err := rec.Validate(time.Now()); err != nil {
				t.Fatalf("batch %d record seq=%d invalid: %v", i, rec.Seq, err)
			}
		}
	}
}

// Sequence numbers are the dedup key with the stream and the timestamp, so two
// batches must never reuse one.
func TestSequenceNumbersDoNotRepeat(t *testing.T) {
	t.Parallel()

	opt := testOptions()
	s := &sender{id: 0, opt: &opt}

	seen := make(map[int64]bool)
	for i := int64(0); i < 20; i++ {
		for _, rec := range s.batch(i).GetRecords() {
			if seen[rec.GetSeq()] {
				t.Fatalf("seq %d was generated twice", rec.GetSeq())
			}
			seen[rec.GetSeq()] = true
		}
	}
}

// Every batch carries one label set, which is the whole point of the wire format;
// spreading a batch across streams would mean the collector could not fingerprint it.
func TestBatchesSpreadAcrossStreams(t *testing.T) {
	t.Parallel()

	opt := testOptions()
	s := &sender{id: 0, opt: &opt}

	services := make(map[string]int)
	for i := int64(0); i < int64(opt.streams)*3; i++ {
		services[s.batch(i).GetLabels().GetService()]++
	}
	if len(services) != opt.streams {
		t.Errorf("saw %d distinct services, want %d", len(services), opt.streams)
	}
}

func TestMessageHonorsSize(t *testing.T) {
	t.Parallel()

	for _, size := range []int{120, 512, 4096} {
		if got := len(message(42, size)); got != size {
			t.Errorf("message(42, %d) is %d bytes", size, got)
		}
	}
	// Below the length of the "seq=" prefix the prefix wins: a size this small is a
	// misconfiguration, and a truncated sequence number would break the dedup story
	// the generated records exist to exercise.
	for _, size := range []int{1, 8} {
		got := message(1234567890123, size)
		if !strings.Contains(got, "1234567890123") {
			t.Errorf("message(_, %d) dropped its sequence number: %q", size, got)
		}
	}
}

func TestOptionsValidate(t *testing.T) {
	t.Parallel()

	valid := testOptions()
	if err := valid.validate(); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}

	tests := map[string]func(*options){
		"records":      func(o *options) { o.records = 0 },
		"batch size":   func(o *options) { o.batchSize = 0 },
		"streams":      func(o *options) { o.streams = -1 },
		"senders":      func(o *options) { o.senders = 0 },
		"message size": func(o *options) { o.messageSize = 0 },
	}
	for name, breakIt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opt := testOptions()
			breakIt(&opt)
			if err := opt.validate(); err == nil {
				t.Errorf("invalid %s was accepted", name)
			}
		})
	}
}

func TestTallyCountsByCode(t *testing.T) {
	t.Parallel()

	var tl tally
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_ACCEPTED, Accepted: 10})
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_OVERLOADED, Rejected: 5})
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_INVALID, Rejected: 2})
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_INTERNAL, Rejected: 1})

	if got := tl.accepted.Load(); got != 10 {
		t.Errorf("accepted = %d, want 10", got)
	}
	if got := tl.rejected.Load(); got != 8 {
		t.Errorf("rejected = %d, want 8", got)
	}
	// Overload is the backpressure chain working, so it is counted apart from the
	// codes that mean something is actually broken.
	if got := tl.overloaded.Load(); got != 1 {
		t.Errorf("overloaded = %d, want 1", got)
	}
	if got := tl.invalid.Load(); got != 1 {
		t.Errorf("invalid = %d, want 1", got)
	}
	if got := tl.internal.Load(); got != 1 {
		t.Errorf("internal = %d, want 1", got)
	}
	if got := tl.batches.Load(); got != 4 {
		t.Errorf("batches = %d, want 4", got)
	}
}
