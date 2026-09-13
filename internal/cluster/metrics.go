package cluster

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the cluster layer's gauges and counters.
type Metrics struct {
	// Members is the number of alive members this node knows about, itself
	// included.
	Members prometheus.Gauge
	// OwnershipChanges counts key ranges that changed owner across every ring
	// rebuild. One join or leave contributes about vnodes of them.
	OwnershipChanges prometheus.Counter
}

// NewMetrics registers the cluster metrics with reg; nil registers nothing,
// which is what tests want.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Members: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "logagg_cluster_members",
			Help: "Alive cluster members known to this node, including itself.",
		}),
		OwnershipChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logagg_ring_ownership_changes_total",
			Help: "Hash ring key ranges that changed owner on membership changes.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.Members, m.OwnershipChanges)
	}
	return m
}
