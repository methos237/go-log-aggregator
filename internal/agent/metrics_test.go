package agent

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// TestNewMetrics_NilRegistererDoesNotPanic pins the house convention every
// package's metrics constructor follows: a nil Registerer builds working,
// unregistered collectors rather than panicking, so unit tests never need a
// real registry.
func TestNewMetrics_NilRegistererDoesNotPanic(t *testing.T) {
	m := NewMetrics(nil)
	if m == nil {
		t.Fatal("NewMetrics(nil) = nil")
	}

	// The counters must be usable even though nothing is registered.
	m.RotationsDetected.Inc()
	m.TruncationsDetected.Inc()
	m.GenerationsMissed.Inc()
	m.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonMissedGeneration).Inc()
}

// TestNewMetrics_PreCreatesMissedGenerationReason checks the house rule that
// every label combination for a closed-set reason is created at zero: an
// alert on a series that has never been observed cannot fire.
func TestNewMetrics_PreCreatesMissedGenerationReason(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	found := false
	for _, fam := range families {
		if fam.GetName() != "logagg_records_dropped_total" {
			continue
		}
		for _, metric := range fam.GetMetric() {
			if labelsMatch(metric, observability.ComponentAgent, reasonMissedGeneration) {
				found = true
				if metric.GetCounter().GetValue() != 0 {
					t.Errorf("pre-created missed_generation counter = %v, want 0", metric.GetCounter().GetValue())
				}
			}
		}
	}
	if !found {
		t.Error("records_dropped_total{component=agent,reason=missed_generation} was not pre-created")
	}
}

// TestNewMetrics_SharesRecordsDroppedAcrossLayers checks the one collector
// that is meant to survive being built twice against the same registry:
// observability.RecordsDropped is shared across every layer, so a second
// caller (a different package's Metrics constructor, or in this test a
// second NewMetrics) must get a handle to the same collector rather than
// panic on duplicate registration. Package-specific counters like
// RotationsDetected are not shared this way — one Metrics per process is the
// house convention there, same as internal/ingest.
func TestNewMetrics_SharesRecordsDroppedAcrossLayers(t *testing.T) {
	reg := prometheus.NewRegistry()
	first := NewMetrics(reg)

	// A second layer's shared drop counter, built independently against the
	// same registry, is exactly what observability.RecordsDropped is for.
	second := observability.RecordsDropped(reg)

	first.RecordsDropped.WithLabelValues(observability.ComponentAgent, reasonMissedGeneration).Inc()
	second.WithLabelValues(observability.ComponentAgent, reasonMissedGeneration).Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "logagg_records_dropped_total" {
			continue
		}
		for _, metric := range fam.GetMetric() {
			if labelsMatch(metric, observability.ComponentAgent, reasonMissedGeneration) {
				if got := metric.GetCounter().GetValue(); got != 2 {
					t.Errorf("records_dropped_total{missed_generation} = %v, want 2 (both handles share one collector)", got)
				}
			}
		}
	}
}

func labelsMatch(metric *dto.Metric, component, reason string) bool {
	var gotComponent, gotReason string
	for _, lp := range metric.GetLabel() {
		switch lp.GetName() {
		case "component":
			gotComponent = lp.GetValue()
		case "reason":
			gotReason = lp.GetValue()
		}
	}
	return gotComponent == component && gotReason == reason
}
