package tail

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

const tailSubsystem = "tail"

// Metrics is the tail layer's instrumentation.
//
// Dropped is its own counter rather than a reason under the shared
// records_dropped_total family on purpose: that family answers "is this node
// losing records", and a tail drop loses nothing — the record is durable and a
// query finds it. Folding it in would make the loss panel lie every time a
// browser tab stalled. The per-client buffer has no depth gauge for the same
// reason the other bounded queues do: a series per client is unbounded
// cardinality, and Dropped moving is the signal depth would have given.
type Metrics struct {
	// Subscriptions is the number of live tail clients on this node.
	Subscriptions prometheus.Gauge
	// Delivered counts matched records handed to a client's buffer.
	Delivered prometheus.Counter
	// Dropped counts matched records a client was too slow to take.
	Dropped prometheus.Counter
}

// NewMetrics registers the tail metrics. A nil Registerer builds them
// unexported, which is what unit tests want.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Subscriptions: f.NewGauge(prometheus.GaugeOpts{
			Namespace: observability.Namespace,
			Subsystem: tailSubsystem,
			Name:      "subscriptions",
			Help:      "Live tail clients on this node.",
		}),
		Delivered: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: tailSubsystem,
			Name:      "records_delivered_total",
			Help:      "Matched records handed to a tail client's buffer.",
		}),
		Dropped: f.NewCounter(prometheus.CounterOpts{
			Namespace: observability.Namespace,
			Subsystem: tailSubsystem,
			Name:      "records_dropped_total",
			Help:      "Matched records dropped because the tail client's buffer was full.",
		}),
	}
}
