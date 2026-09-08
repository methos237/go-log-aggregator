package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Components that can drop a record, used as the component label below.
const (
	ComponentIngest = "ingest"
	ComponentWriter = "writer"
)

// RecordsDropped returns the shared drop counter, registering it on first use.
//
// One family across layers rather than one per package, because the question an
// operator asks is "is this node losing records", not "is the writer losing
// records" — and two counters with different names cannot be summed on one panel
// without knowing both. The component label is what keeps the layers
// distinguishable inside that single family.
//
// Sharing goes through shared(), which hands the second caller a handle to the
// collector the first one registered. A nil Registerer returns an unregistered
// counter, which is what unit tests want.
func RecordsDropped(reg prometheus.Registerer) *prometheus.CounterVec {
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "records_dropped_total",
		Help:      "Records this node refused or gave up on, by the layer that dropped them.",
	}, []string{"component", "reason"})

	return shared(reg, vec)
}
