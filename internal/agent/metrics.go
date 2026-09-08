package agent

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// agentSubsystem prefixes metrics specific to this package: logagg_agent_*.
const agentSubsystem = "agent"

// reasonMissedGeneration is the reason recorded against the shared
// observability.RecordsDropped family, under observability.ComponentAgent,
// when a rotation is discovered only at startup because the checkpointed
// FileID no longer matches the file now at the path.
//
// It is a single unexported constant, not a family built from a path or an
// error, because the tail source only ever opens its configured Path — Name()
// is that one path for the source's whole lifetime — so there is exactly one
// thing that can be missing a generation, and a reason built from anything
// more specific would not add information here the way it would for a layer
// that drops records from many callers.
const reasonMissedGeneration = "missed_generation"

// Shipper drop reasons, recorded against the same RecordsDropped family.
// Each names a distinct, closed-set reason a record never made it to the
// collector, mirroring how internal/ingest's own reasons are scoped to one
// short constant per cause rather than a message built from the error.
const (
	// reasonNoLabels means a source's Labels lookup returned false: a
	// configuration error (see ShipperConfig.Labels' doc comment), not a
	// data problem, but one that must still be visible rather than a
	// silent gap.
	reasonNoLabels = "no_labels"
	// reasonInvalidRecord means model.LogRecord.Validate rejected the
	// record. The collector would reject it identically, so catching it
	// here lets an operator see which source produced it.
	reasonInvalidRecord = "invalid_record"
	// reasonRecordTooLarge means a single record, even alone in a batch,
	// exceeds MaxBatchBytes and so can never be shipped at all.
	reasonRecordTooLarge = "record_too_large"
	// reasonCorruptSpoolEntry means a spooled payload failed to unmarshal
	// as a LogBatch on replay: corruption the spool's own checksum did not
	// catch. Retrying it would fail identically forever.
	reasonCorruptSpoolEntry = "corrupt_spool_entry"
	// reasonAckInvalid means the collector returned ACK_CODE_INVALID for a
	// batch: resending the same bytes would fail the same way, so it is
	// dropped rather than retried.
	reasonAckInvalid = "ack_invalid"
	// reasonEncodeFailed means marshaling a batch this package built
	// itself failed. Should not happen in practice; kept distinct from
	// reasonCorruptSpoolEntry because this is an encode failure on the way
	// out, not a decode failure on the way back in.
	reasonEncodeFailed = "encode_failed"
	// reasonSpoolAppendFailed means Spool.Append itself failed (disk
	// exhaustion or similar) for a batch that could not be sent. There is
	// nowhere else for it to go once this happens.
	reasonSpoolAppendFailed = "spool_append_failed"
)

// shipperAckCodeNames are the metric label values for Metrics.Acks,
// mirroring internal/ingest's ackCodeNames so the two packages' dashboards
// read the same way.
var shipperAckCodeNames = []string{"accepted", "overloaded", "invalid", "internal"}

// Metrics is the tail source's instrumentation.
//
// Rotations and truncations are not drops: every byte they name was either
// read (rotation, after the drain) or is still on disk under the same
// inode (truncation, next poll picks it back up from offset zero). They get
// their own counters rather than reasons on the shared drop family for
// exactly that reason — nothing was lost, so counting them as a "drop" would
// mislead an operator watching that series for loss. A missed generation is
// the one case among these where content is actually gone (see Decision 1 in
// the package doc: this source never opens anything but the configured
// path), which is why it alone is also recorded on RecordsDropped.
type Metrics struct {
	// RotationsDetected counts rename-and-recreate rotations noticed while
	// running: the path's inode changed under an fd this source already had
	// open. Each one was drained to true EOF before the switch, so none of
	// them lost data.
	RotationsDetected prometheus.Counter
	// TruncationsDetected counts in-place truncations that reset the read
	// offset to zero on the same fd: copytruncate's size shrink, and the
	// truncate-and-rewrite-past-the-old-offset case that only the head
	// fingerprint catches. Also incremented when a resumed cursor's fingerprint
	// or offset proves stale at startup, before the first read.
	TruncationsDetected prometheus.Counter
	// GenerationsMissed counts rotations discovered only at startup: the
	// checkpoint names a FileID that is not the file now at the path, meaning
	// a rotation (or more than one — indistinguishable from here, see the
	// package doc's Decision 1) happened while this agent was not running.
	// That generation's content is gone for good; this is the honest count of
	// how often that has happened.
	GenerationsMissed prometheus.Counter
	// RecordsDropped is shared with every other layer; see
	// observability.RecordsDropped. The tail source uses it only for
	// reasonMissedGeneration; the shipper uses the reasons declared above.
	RecordsDropped *prometheus.CounterVec

	// Acks counts collector responses to shipped batches, by code — the
	// shipper's side of the same shape internal/ingest.Metrics.Acks uses,
	// so an operator can compare what the agent sent against what the
	// collector recorded for it.
	Acks *prometheus.CounterVec
	// Reconnects counts every scheduled attempt to (re)open the ingest
	// stream: the first one at startup, and one more each time a mid-run
	// disconnect is detected. A steady climb here is the signal that a
	// collector or the network between here and it is flapping.
	Reconnects prometheus.Counter
}

// NewMetrics builds the tail source's metrics.
//
// A nil Registerer builds them without exporting, which is what unit tests
// want: no registry to construct, and no risk of a duplicate-registration
// panic when a test package creates several TailSources.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	m := &Metrics{
		RotationsDetected: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: agentSubsystem,
			Name:      "rotations_detected_total",
			Help:      "Rename-and-recreate rotations detected and drained without a gap.",
		}),
		TruncationsDetected: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: agentSubsystem,
			Name:      "truncations_detected_total",
			Help:      "In-place truncations detected, by size shrink or by a head fingerprint mismatch.",
		}),
		GenerationsMissed: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: agentSubsystem,
			Name:      "generations_missed_total",
			Help:      "Rotations discovered only at startup: the checkpointed file no longer exists at the path.",
		}),
		RecordsDropped: observability.RecordsDropped(reg),

		Acks: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: agentSubsystem,
			Name:      "acks_total",
			Help:      "Acknowledgements received from the collector, by code.",
		}, []string{"code"}),

		Reconnects: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: agentSubsystem,
			Name:      "reconnects_total",
			Help:      "Attempts to (re)open the ingest stream, including the first one at startup.",
		}),
	}

	// Pre-created at zero so an alert on this series can fire the first time
	// it is ever observed, instead of starting from "no data" — a counter
	// nobody has incremented yet and a counter that does not exist look
	// identical to a dashboard, but only one of them can page anyone.
	m.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonMissedGeneration)
	for _, reason := range []string{
		reasonNoLabels, reasonInvalidRecord, reasonRecordTooLarge,
		reasonCorruptSpoolEntry, reasonAckInvalid, reasonEncodeFailed, reasonSpoolAppendFailed,
	} {
		m.RecordsDropped.WithLabelValues(observability.ComponentAgent, reason)
	}
	for _, code := range shipperAckCodeNames {
		m.Acks.WithLabelValues(code)
	}

	return m
}
