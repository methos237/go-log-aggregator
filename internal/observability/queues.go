package observability

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Bounded queues in the record pipeline, used as the queue label below.
//
// Agent-side and collector-side queues share this one family rather than getting
// a logagg_agent_* family of their own, because the operator's question spans
// both processes: "where in the pipeline is the backlog". A separate family would
// make that ungraphable on one panel, which is the whole reason this family is
// shared in the first place.
const (
	QueueIngest = "ingest"
	QueueWriter = "writer"
	// QueueAgentLines is the agent's source-to-shipper channel.
	QueueAgentLines = "agent_lines"
)

// QueueDepth returns the shared depth gauge for bounded in-process queues,
// registering it on first use.
//
// Every bounded queue in this project exposes a depth gauge and a wait
// histogram, and they are one family each rather than one per layer so a single
// dashboard panel can plot the whole pipeline. Depth alone cannot distinguish
// "full but draining fast" from "full and stuck", which is why the wait histogram
// below is not optional.
func QueueDepth(reg prometheus.Registerer) *prometheus.GaugeVec {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "queue_depth",
		Help:      "Records currently waiting in a bounded in-process queue.",
	}, []string{"queue"})
	return shared(reg, vec)
}

// QueueWait returns the shared wait-time histogram for bounded in-process queues.
func QueueWait(reg prometheus.Registerer) *prometheus.HistogramVec {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Name:      "queue_wait_seconds",
		Help:      "Time a record spent waiting in a bounded in-process queue.",
		// Sub-millisecond at the low end because a healthy queue is nearly empty;
		// the top buckets are seconds wide, because that is the regime where
		// backpressure is doing its job and the shape is what matters.
		Buckets: []float64{0.0001, 0.001, 0.005, 0.025, 0.1, 0.5, 1, 5, 30},
	}, []string{"queue"})
	return shared(reg, vec)
}

// shared registers c, or returns the equivalent collector already registered.
//
// Two layers build their own metrics from the same registry and neither can be
// made to depend on the other, so whichever registers first wins and the second
// gets a handle to the same collector. A nil Registerer returns c unregistered,
// which is what unit tests want.
func shared[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	if reg == nil {
		return c
	}
	err := reg.Register(c)
	if err == nil {
		return c
	}

	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(C); ok {
			return existing
		}
	}
	// A different collector under the same name is a programming error, and the
	// alternative to panicking is silently discarding these observations.
	panic(err)
}
