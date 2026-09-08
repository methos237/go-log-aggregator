package storage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// Drop and retry reasons. These are metric label values, so they are a closed set
// of short constants rather than formatted strings: a reason derived from an error
// message would blow up cardinality the first time a DSN appeared in one.
const (
	reasonQueueFull    = "writer_queue_full"
	reasonInvalid      = "invalid_record"
	reasonWriteFailed  = "write_failed"
	reasonShutdown     = "shutdown"
	reasonRetryable    = "retryable_db_error"
	reasonNonRetryable = "non_retryable_db_error"
)

// storageSubsystem prefixes metrics that are specific to this package. The
// queue/batch/write families deliberately do not use it: they are named in §6 of
// the roadmap and shared with the ingest and queue layers, so a dashboard can plot
// every bounded queue in the system on one panel.
const storageSubsystem = "storage"

// Metrics is the storage subsystem's instrumentation.
//
// Passed in rather than package-global so tests can register against a throwaway
// registry, and so the node label that observability.Metrics applies is not
// bypassed. A nil Registerer is accepted and means "build the metrics but do not
// export them", which is what unit tests want.
type Metrics struct {
	// Bounded-queue instrumentation, per the roadmap rule that every bounded queue
	// exposes both a depth gauge and a wait-time histogram. Depth alone cannot
	// distinguish "full but draining fast" from "full and stuck".
	QueueDepth *prometheus.GaugeVec
	QueueWait  *prometheus.HistogramVec

	BatchSize     prometheus.Histogram
	WriteDuration *prometheus.HistogramVec

	RowsCopied     prometheus.Counter
	RowsInserted   prometheus.Counter
	RowsDeduped    prometheus.Counter
	RecordsDropped *prometheus.CounterVec
	WriteRetries   *prometheus.CounterVec

	StreamUpserts       prometheus.Counter
	StreamCacheHits     prometheus.Counter
	StreamCacheMisses   prometheus.Counter
	StreamCacheEntries  prometheus.Gauge
	StreamCacheCapacity prometheus.Gauge
}

// NewMetrics registers the storage metrics and returns them.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	m := &Metrics{
		// Shared with the ingest layer so one panel plots every bounded queue in the
		// pipeline; see observability.QueueDepth.
		QueueDepth: observability.QueueDepth(reg),
		QueueWait:  observability.QueueWait(reg),

		BatchSize: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: observability.Namespace,
			Name:      "batch_size",
			Help:      "Records per write batch.",
			// Powers of two from 16 to 32k: batch size is the main throughput knob
			// and phase 8 sweeps it, so the buckets must span that whole sweep.
			Buckets: prometheus.ExponentialBuckets(16, 2, 12),
		}),

		WriteDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: observability.Namespace,
			Name:      "write_duration_seconds",
			Help:      "Wall time to persist one batch, including stream upserts and retries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.5, 10),
		}, []string{"outcome"}),

		RowsCopied: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "rows_copied_total",
			Help:      "Rows COPYed into the staging table.",
		}),

		RowsInserted: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "rows_inserted_total",
			Help:      "Rows that reached the logs hypertable.",
		}),

		// The gap between copied and inserted is the redelivery rate that
		// at-least-once delivery produces. Its own counter makes "what is
		// redelivery costing" a query rather than an arithmetic exercise.
		RowsDeduped: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "rows_deduplicated_total",
			Help:      "Rows discarded by the dedup index on insert, i.e. redeliveries.",
		}),

		// Shared with the ingest layer: one family, one panel, distinguished by the
		// component label. See observability.RecordsDropped.
		RecordsDropped: observability.RecordsDropped(reg),

		WriteRetries: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "write_retries_total",
			Help:      "Batch write attempts that failed and were retried.",
		}, []string{"reason"}),

		StreamUpserts: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "stream_upserts_total",
			Help:      "Streams written to the dimension table.",
		}),

		StreamCacheHits: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "stream_cache_hits_total",
			Help:      "Stream lookups served from the in-memory cache.",
		}),

		StreamCacheMisses: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "stream_cache_misses_total",
			Help:      "Stream lookups that required an upsert.",
		}),

		StreamCacheEntries: f.NewGauge(prometheus.GaugeOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "stream_cache_entries",
			Help:      "Streams currently cached.",
		}),

		StreamCacheCapacity: f.NewGauge(prometheus.GaugeOpts{
			Namespace: observability.Namespace,
			Subsystem: storageSubsystem,
			Name:      "stream_cache_capacity_entries",
			Help:      "Configured stream cache capacity, in entries.",
		}),
	}

	// Pre-create the label combinations that matter so a dashboard shows 0 rather
	// than a gap before the first drop or retry. An alert on a series that does not
	// exist yet does not fire.
	for _, reason := range []string{reasonQueueFull, reasonInvalid, reasonWriteFailed, reasonShutdown} {
		m.RecordsDropped.WithLabelValues(observability.ComponentWriter, reason)
	}
	for _, reason := range []string{reasonRetryable, reasonNonRetryable} {
		m.WriteRetries.WithLabelValues(reason)
	}
	for _, outcome := range []string{outcomeSuccess, outcomeFailure} {
		m.WriteDuration.WithLabelValues(outcome)
	}
	m.QueueDepth.WithLabelValues(observability.QueueWriter)
	m.QueueWait.WithLabelValues(observability.QueueWriter)

	return m
}

const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
)
