package main

import (
	"testing"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
)

// Shedding is the backpressure chain working as designed, so it must not make the run
// fail — otherwise correct behavior is indistinguishable from a broken collector.
func TestShedRecordsDoNotCountAsRefused(t *testing.T) {
	t.Parallel()

	var tl tally
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_OVERLOADED, Rejected: 40})

	if got := tl.shed.Load(); got != 40 {
		t.Errorf("shed = %d, want 40", got)
	}
	if got := tl.refused.Load(); got != 0 {
		t.Errorf("refused = %d, want 0: overload is not a failure of this tool", got)
	}

	// A genuine refusal does count.
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_INVALID, Rejected: 3})
	tl.record(&logaggv1.Ack{Code: logaggv1.AckCode_ACK_CODE_INTERNAL, Rejected: 2})
	if got := tl.refused.Load(); got != 5 {
		t.Errorf("refused = %d, want 5", got)
	}
	// Both still show in the total.
	if got := tl.rejected.Load(); got != 45 {
		t.Errorf("rejected = %d, want 45", got)
	}
}
