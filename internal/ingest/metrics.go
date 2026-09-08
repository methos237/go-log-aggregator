package ingest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// ingestSubsystem prefixes metrics specific to this package.
const ingestSubsystem = "ingest"

// Drop reasons. A closed set of short constants, because a reason built from an
// error message would blow up label cardinality the first time a peer address
// appeared in one.
const (
	reasonInvalidLabels = "invalid_labels"
	reasonInvalidRecord = "invalid_record"
	reasonEncodeFailed  = "encode_failed"
	reasonQueueRefused  = "queue_refused"
	reasonQueueTooLarge = "queue_payload_too_large"
	reasonBufferFull    = "ingest_buffer_full"
	reasonShutdown      = "shutdown"
	reasonClientGone    = "client_gone"
)

// Metrics is the ingest layer's instrumentation.
type Metrics struct {
	// RecordsReceived counts records as they arrive, before validation, which makes
	// it the denominator for every rejection rate.
	RecordsReceived prometheus.Counter
	// RecordsAccepted counts records durably queued, i.e. the ones an agent was told
	// it may forget.
	RecordsAccepted prometheus.Counter
	// RecordsDropped is shared with the storage layer; see observability.RecordsDropped.
	RecordsDropped *prometheus.CounterVec
	// QueueDepth and QueueWait describe the bounded intake channel, in the same
	// families the writer's queue uses so one panel covers the whole pipeline.
	QueueDepth *prometheus.GaugeVec
	QueueWait  *prometheus.HistogramVec
	// Acks is keyed by the code returned to the agent. Watching the OVERLOADED rate
	// is how backpressure becomes visible from outside the process.
	Acks *prometheus.CounterVec
	// BatchDuration is receive-to-ack latency for one batch, which is what an agent
	// experiences as ingest latency.
	BatchDuration prometheus.Histogram
	// ActiveStreams is the number of agent streams currently connected.
	ActiveStreams prometheus.Gauge
}

// NewMetrics registers the ingest metrics and returns them.
//
// A nil Registerer builds them without exporting, which is what unit tests want.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	m := &Metrics{
		RecordsReceived: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: ingestSubsystem,
			Name:      "records_received_total",
			Help:      "Records received from agents, before validation.",
		}),

		RecordsAccepted: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: ingestSubsystem,
			Name:      "records_accepted_total",
			Help:      "Records durably published to the queue and acknowledged to the agent.",
		}),

		RecordsDropped: observability.RecordsDropped(reg),
		QueueDepth:     observability.QueueDepth(reg),
		QueueWait:      observability.QueueWait(reg),

		Acks: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: ingestSubsystem,
			Name:      "acks_total",
			Help:      "Acknowledgements sent to agents, by code.",
		}, []string{"code"}),

		BatchDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: observability.Namespace,
			Subsystem: ingestSubsystem,
			Name:      "batch_duration_seconds",
			Help:      "Time from receiving a batch to acknowledging it, including the queue publish.",
			// The publish dominates this, so the buckets mirror the queue's own
			// histogram and stretch past the publish timeout.
			Buckets: prometheus.ExponentialBuckets(0.0005, 2.5, 10),
		}),

		ActiveStreams: f.NewGauge(prometheus.GaugeOpts{
			Namespace: observability.Namespace,
			Subsystem: ingestSubsystem,
			Name:      "active_streams",
			Help:      "Agent streams currently connected.",
		}),
	}

	// Pre-created so a dashboard reads 0 rather than "no data" before the first
	// rejection, and so an alert on these series can fire at all.
	for _, reason := range []string{
		reasonInvalidLabels, reasonInvalidRecord, reasonEncodeFailed,
		reasonQueueRefused, reasonQueueTooLarge, reasonBufferFull, reasonShutdown,
		reasonClientGone,
	} {
		m.RecordsDropped.WithLabelValues(observability.ComponentIngest, reason)
	}
	for _, code := range ackCodeNames {
		m.Acks.WithLabelValues(code)
	}
	m.QueueDepth.WithLabelValues(observability.QueueIngest)
	m.QueueWait.WithLabelValues(observability.QueueIngest)
	return m
}
