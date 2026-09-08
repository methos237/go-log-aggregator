package queue

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetricsRegistersEverything(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	NewMetrics(reg)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	seen := make(map[string]bool, len(families))
	for _, f := range families {
		seen[f.GetName()] = true
	}

	// Named explicitly rather than counted, so renaming a metric fails the test
	// instead of quietly breaking a Grafana panel that queries the old name.
	want := []string{
		"logagg_queue_publish_duration_seconds",
		"logagg_queue_published_bytes_total",
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("metric %s was not registered", name)
		}
	}
}

// A dashboard should read 0 rather than "no data" before the first failure, and an
// alert on a series that does not exist yet never fires.
func TestNewMetricsPreCreatesOutcomes(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	NewMetrics(reg)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	for _, f := range families {
		if f.GetName() != "logagg_queue_publish_duration_seconds" {
			continue
		}
		outcomes := make(map[string]bool)
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "outcome" {
					outcomes[l.GetValue()] = true
				}
			}
		}
		for _, want := range []string{outcomeSuccess, outcomeFailure} {
			if !outcomes[want] {
				t.Errorf("outcome %q was not pre-created", want)
			}
		}
		return
	}
	t.Fatal("publish duration histogram was not registered")
}

func TestNewMetricsWithNilRegistererIsUsable(t *testing.T) {
	t.Parallel()

	// Unit tests that only care about behavior should not have to build a registry.
	m := NewMetrics(nil)
	m.PublishDuration.WithLabelValues(outcomeSuccess).Observe(0.01)
	m.PublishBytes.Add(128)
}

func TestMetricNamesAreNamespaced(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	NewMetrics(reg)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "logagg_") {
			t.Errorf("metric %s is not namespaced", f.GetName())
		}
	}
}
