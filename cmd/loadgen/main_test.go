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

// A run has to be reproducible from its flags, which means the same seq must
// always render the same text and different seqs must not collapse to one string.
func TestMessageIsDeterministicInSeq(t *testing.T) {
	t.Parallel()

	if a, b := message(7, 200), message(7, 200); a != b {
		t.Fatalf("message(7) differs between calls:\n%q\n%q", a, b)
	}
	if a, b := message(7, 200), message(8, 200); a[len("synthetic record seq=7"):] == b[len("synthetic record seq=8"):] {
		t.Fatalf("messages for seq 7 and 8 share their body: %q", a)
	}
}

// Zipf-drawn words are the whole point of the vocabulary: a common needle must hit
// most rows and a rare one few, or the search benchmark measures nothing.
func TestVocabularyIsSkewed(t *testing.T) {
	t.Parallel()

	const n = 2000
	hits := make(map[string]int)
	for seq := int64(0); seq < n; seq++ {
		for _, w := range strings.Fields(message(seq, 160))[3:] {
			hits[w]++
		}
	}
	common, rare := hits[vocabulary[0]], hits[vocabulary[len(vocabulary)-1]]
	if common < 10*rare || rare == 0 {
		t.Errorf("head word %q appeared %d times, tail word %q %d times: want a heavy skew with a non-empty tail",
			vocabulary[0], common, vocabulary[len(vocabulary)-1], rare)
	}
}

func TestScheduleReleaseTimes(t *testing.T) {
	t.Parallel()

	// Unpaced: everything is released at once.
	if got := (schedule{}).releaseAt(1_000_000); got != 0 {
		t.Errorf("unpaced release = %s, want 0", got)
	}

	// Constant rate: 1000 records/s means record 500 is released at 0.5s.
	flat := schedule{rate: 1000}
	if got := flat.releaseAt(500); got != 500*time.Millisecond {
		t.Errorf("flat release(500) = %s, want 500ms", got)
	}

	// Ramp: over a 10s ramp to 1000/s the first half of the ramp's records
	// (rate*ramp/2 = 5000 in total) are released on a square-root curve. At the end
	// of the ramp exactly 5000 records have been issued; from there the flat rate
	// applies, so record 6000 goes out at 11s.
	ramped := schedule{rate: 1000, ramp: 10 * time.Second}
	if got := ramped.releaseAt(5000); got != 10*time.Second {
		t.Errorf("ramped release(5000) = %s, want 10s", got)
	}
	if got := ramped.releaseAt(6000); got != 11*time.Second {
		t.Errorf("ramped release(6000) = %s, want 11s", got)
	}
	// Quarter of the ramp's records is released at half the ramp time (t ∝ √n).
	if got := ramped.releaseAt(1250); got != 5*time.Second {
		t.Errorf("ramped release(1250) = %s, want 5s", got)
	}
	// Release times never go backwards.
	prev := time.Duration(-1)
	for n := int64(0); n < 20000; n += 250 {
		if got := ramped.releaseAt(n); got < prev {
			t.Fatalf("release(%d) = %s < release(%d) = %s", n, got, n-250, prev)
		} else {
			prev = got
		}
	}
}

func TestPercentileNearestRank(t *testing.T) {
	t.Parallel()

	if got := percentile(nil, 0.99); got != 0 {
		t.Errorf("empty percentile = %s, want 0", got)
	}
	sorted := make([]time.Duration, 100)
	for i := range sorted {
		sorted[i] = time.Duration(i+1) * time.Millisecond
	}
	for p, want := range map[float64]time.Duration{0.5: 50 * time.Millisecond, 0.99: 99 * time.Millisecond, 1: 100 * time.Millisecond} {
		if got := percentile(sorted, p); got != want {
			t.Errorf("percentile(%v) = %s, want %s", p, got, want)
		}
	}
}

func TestPacingOptionsValidate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		set  func(*options)
		want bool
	}{
		"rate alone":            {func(o *options) { o.rate = 1000 }, true},
		"rate with ramp":        {func(o *options) { o.rate = 1000; o.ramp = time.Second }, true},
		"rate with duration":    {func(o *options) { o.rate = 1000; o.duration = time.Second }, true},
		"ramp without rate":     {func(o *options) { o.ramp = time.Second }, false},
		"duration without rate": {func(o *options) { o.duration = time.Second }, false},
		"negative rate":         {func(o *options) { o.rate = -1 }, false},
		"negative duration":     {func(o *options) { o.duration = -time.Second }, false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opt := testOptions()
			tc.set(&opt)
			if err := opt.validate(); (err == nil) != tc.want {
				t.Errorf("validate() error = %v, want ok=%v", err, tc.want)
			}
		})
	}
}
