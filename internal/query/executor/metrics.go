package executor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/query"
)

const querySubsystem = "query"

// Metrics are the read path's counters, observed once per API query whether
// it ran on this node alone or was fanned out across the cluster.
//
// Passed in rather than package-global for the same reason as the other
// packages: a nil Registerer builds them without exporting them.
type Metrics struct {
	// Duration is wall time from plan to result, by the relation the planner
	// read: "logs" or a continuous aggregate. Snapping to an aggregate is the
	// planner's main optimisation, and this is where its payoff shows.
	Duration *prometheus.HistogramVec
	// Rows is records or points returned per query, after the row cap. What the
	// database scanned to produce them is not visible from here; EXPLAIN is.
	Rows prometheus.Histogram
}

// NewMetrics registers the query metrics and returns them.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: observability.Namespace,
			Subsystem: querySubsystem,
			Name:      "duration_seconds",
			Help:      "Wall time to plan and execute one query, by source relation.",
			// From an index-only point lookup up past a typical query timeout.
			Buckets: prometheus.ExponentialBuckets(0.001, 2.5, 12),
		}, []string{"source"}),
		Rows: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: observability.Namespace,
			Subsystem: querySubsystem,
			Name:      "rows_returned",
			Help:      "Records or aggregate points returned per query, after the row cap.",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 10),
		}),
	}
}

// Instrumented is a Runner that observes every successful run of the Runner
// it wraps, so the HTTP API stays ignorant of metrics and a single-node and a
// clustered query are measured the same way.
type Instrumented struct {
	Runner  Runner
	Metrics *Metrics
}

// Run implements Runner.
func (i Instrumented) Run(ctx context.Context, q *query.Query, r query.Request) (*Result, error) {
	res, err := i.Runner.Run(ctx, q, r)
	if err != nil {
		return nil, err
	}
	i.Metrics.Duration.WithLabelValues(res.Source).Observe(res.Elapsed.Seconds())
	i.Metrics.Rows.Observe(float64(len(res.Records) + len(res.Points)))
	return res, nil
}
