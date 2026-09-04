package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/jamespolk/go-log-aggregator/internal/version"
)

// Namespace prefixes every metric this project defines.
const Namespace = "logagg"

// Metrics owns the process metric registry.
//
// A dedicated registry rather than prometheus.DefaultRegisterer keeps test runs
// isolated (no cross-test duplicate registration panics) and keeps metrics
// pulled in by dependencies from appearing unannounced.
type Metrics struct {
	// Registry is what the /metrics handler serves.
	Registry *prometheus.Registry
	// Registerer wraps Registry so every metric registered through it gains the
	// node label automatically. Collectors should use this, not Registry.
	Registerer prometheus.Registerer
}

// NewMetrics builds the registry, attaches the standard Go and process
// collectors, and records build info.
func NewMetrics(node string) *Metrics {
	reg := prometheus.NewRegistry()

	// Unwrapped: the Go and process collectors already carry no node label, and
	// wrapping them would be fine too — but keeping them raw makes it obvious in
	// /metrics output which series are ours.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "build_info",
			Help:      "Build metadata, always 1. Labels carry version, commit and Go version.",
		},
		[]string{"version", "commit", "build_date", "go_version"},
	)
	info := version.Info()
	buildInfo.WithLabelValues(
		info["version"], info["commit"], info["build_date"], info["go_version"],
	).Set(1)
	reg.MustRegister(buildInfo)

	return &Metrics{
		Registry:   reg,
		Registerer: prometheus.WrapRegistererWith(prometheus.Labels{"node": node}, reg),
	}
}
