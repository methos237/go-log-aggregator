package storage

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
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
		"logagg_queue_depth",
		"logagg_queue_wait_seconds",
		"logagg_batch_size",
		"logagg_write_duration_seconds",
		"logagg_records_dropped_total",
		"logagg_storage_rows_copied_total",
		"logagg_storage_rows_inserted_total",
		"logagg_storage_rows_deduplicated_total",
		"logagg_storage_write_retries_total",
		"logagg_storage_stream_upserts_total",
		"logagg_storage_stream_cache_hits_total",
		"logagg_storage_stream_cache_misses_total",
		"logagg_storage_stream_cache_entries",
		"logagg_storage_stream_cache_capacity_entries",
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("metric %s was not registered", name)
		}
	}
}

// TestNewMetricsPreCreatesLabelValues covers the reason those loops at the end of
// NewMetrics exist: an alert on a series that has never been observed does not fire,
// so every drop reason must be present at zero from process start.
func TestNewMetricsPreCreatesLabelValues(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	NewMetrics(reg)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	reasons := make(map[string]bool)
	for _, f := range families {
		if f.GetName() != "logagg_records_dropped_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" {
					reasons[l.GetValue()] = true
				}
			}
		}
	}

	for _, reason := range []string{reasonQueueFull, reasonInvalid, reasonWriteFailed, reasonShutdown} {
		if !reasons[reason] {
			t.Errorf("drop reason %q is missing at startup", reason)
		}
	}
}

// TestNewMetricsWithNilRegistererIsUsable is what lets unit tests and the loadgen
// build a writer without standing up a registry.
func TestNewMetricsWithNilRegistererIsUsable(t *testing.T) {
	t.Parallel()

	m := NewMetrics(nil)
	m.RowsCopied.Add(1)
	m.RecordsDropped.WithLabelValues(observability.ComponentWriter, reasonInvalid).Inc()
	m.QueueDepth.WithLabelValues(queueWriter).Set(3)
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
			t.Errorf("metric %s is missing the logagg_ namespace", f.GetName())
		}
		if f.GetHelp() == "" {
			t.Errorf("metric %s has no help text", f.GetName())
		}
	}
}
