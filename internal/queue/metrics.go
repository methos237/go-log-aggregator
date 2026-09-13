package queue

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// queueSubsystem prefixes metrics specific to this package.
const queueSubsystem = "queue"

const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
)

// Metrics is the queue layer's instrumentation.
//
// Passed in rather than package-global for the same reason as storage.Metrics: a
// test registers against a throwaway registry, and a nil Registerer means "build
// them but do not export them".
type Metrics struct {
	// PublishDuration is the latency of a publish including the JetStream ack. It
	// is the ingest path's tail latency, because the agent's ack waits on it.
	PublishDuration *prometheus.HistogramVec
	// PublishBytes is payload bytes accepted by the queue, which is what sizes the
	// stream's disk ceiling against a given ingest rate.
	PublishBytes prometheus.Counter
	// Consumed counts deliveries to this node, including redeliveries.
	Consumed prometheus.Counter
	// Redeliveries counts deliveries that were not the first attempt. Its ratio to
	// Consumed is the cost of at-least-once delivery, and a climbing ratio means
	// batches are timing out before the writer finishes them.
	Redeliveries prometheus.Counter
	// Fanout counts batches copied to the live-tail subject. A failure here is a
	// tail reader missing a batch, never an agent losing one, which is why it is
	// a counter and not a drop reason.
	Fanout *prometheus.CounterVec
}

// NewMetrics registers the queue metrics and returns them.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	m := &Metrics{
		PublishDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: observability.Namespace,
			Subsystem: queueSubsystem,
			Name:      "publish_duration_seconds",
			Help:      "Wall time to publish one batch and receive the JetStream ack.",
			// From a sub-millisecond local broker up past the publish timeout, so a
			// saturated queue shows as a shifting shape rather than as +Inf.
			Buckets: prometheus.ExponentialBuckets(0.0005, 2.5, 10),
		}, []string{"outcome"}),

		PublishBytes: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: queueSubsystem,
			Name:      "published_bytes_total",
			Help:      "Payload bytes accepted by the queue.",
		}),

		Consumed: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: queueSubsystem,
			Name:      "consumed_total",
			Help:      "Messages delivered to this node, including redeliveries.",
		}),

		Redeliveries: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: queueSubsystem,
			Name:      "redeliveries_total",
			Help:      "Messages delivered more than once.",
		}),

		Fanout: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: queueSubsystem,
			Name:      "fanout_total",
			Help:      "Batches copied to the live-tail subject, by outcome.",
		}, []string{"outcome"}),
	}

	// Pre-created so a dashboard shows 0 rather than a gap before the first
	// failure; an alert on a series that does not exist yet does not fire.
	for _, outcome := range []string{outcomeSuccess, outcomeFailure} {
		m.PublishDuration.WithLabelValues(outcome)
		m.Fanout.WithLabelValues(outcome)
	}
	return m
}
