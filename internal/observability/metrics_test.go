package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func newTestCounter() prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "test_total",
		Help:      "Test counter.",
	})
}

func TestNewMetricsIsIsolatedPerCall(t *testing.T) {
	// Each registry must be independent, otherwise registering the same collector
	// in two tests panics with a duplicate-registration error.
	a, b := NewMetrics("node-a"), NewMetrics("node-b")

	if err := a.Registerer.Register(newTestCounter()); err != nil {
		t.Fatalf("register on first registry: %v", err)
	}
	if err := b.Registerer.Register(newTestCounter()); err != nil {
		t.Fatalf("register on second registry: %v", err)
	}
}

func TestNewMetricsRejectsDuplicateWithinRegistry(t *testing.T) {
	m := NewMetrics("node-a")
	if err := m.Registerer.Register(newTestCounter()); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := m.Registerer.Register(newTestCounter()); err == nil {
		t.Error("second register of the same metric name should fail")
	}
}
