package observability

import (
	"errors"

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
// Sharing needs the AlreadyRegisteredError dance: ingest and storage each build
// their own metrics from the same registry and neither can be made to depend on
// the other, so whichever registers first wins and the other gets a handle to the
// same collector. A nil Registerer returns an unregistered counter, which is what
// unit tests want.
func RecordsDropped(reg prometheus.Registerer) *prometheus.CounterVec {
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "records_dropped_total",
		Help:      "Records this node refused or gave up on, by the layer that dropped them.",
	}, []string{"component", "reason"})

	if reg == nil {
		return vec
	}
	err := reg.Register(vec)
	if err == nil {
		return vec
	}

	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
			return existing
		}
	}
	// A different collector under the same name is a programming error, and the
	// alternative to panicking is silently dropping every one of these counts.
	panic(err)
}
