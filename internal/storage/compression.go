package storage

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// compressionStatsSQL reads the logs hypertable's columnstore totals. The
// function aggregates the catalog's per-chunk sizes; it does not touch the
// chunks themselves, which is what makes running it every scrape acceptable.
// Summed so a hypertable with no chunks yet yields one row of zeros rather
// than no row at all.
const compressionStatsSQL = `SELECT coalesce(sum(before_compression_total_bytes), 0)::bigint, coalesce(sum(after_compression_total_bytes), 0)::bigint
FROM hypertable_compression_stats('logs')`

// compressionCollector exports how much the columnstore is saving.
type compressionCollector struct {
	pool  *pgxpool.Pool
	log   *slog.Logger
	ratio *prometheus.Desc
	raw   *prometheus.Desc
	comp  *prometheus.Desc
}

// NewCompressionCollector returns a collector for the logs hypertable's
// compression ratio and the byte totals behind it. The ratio reads 0 until the
// first chunk has been compressed, so a dashboard shows a number rather than a
// gap while the policy has not run yet.
func NewCompressionCollector(pool *pgxpool.Pool, log *slog.Logger) prometheus.Collector {
	name := func(n string) string {
		return prometheus.BuildFQName(observability.Namespace, storageSubsystem, n)
	}
	return &compressionCollector{
		pool:  pool,
		log:   log,
		ratio: prometheus.NewDesc(name("compression_ratio"), "Uncompressed over compressed bytes for the logs hypertable's compressed chunks; 0 until one exists.", nil, nil),
		raw:   prometheus.NewDesc(name("uncompressed_bytes"), "Bytes the compressed chunks of the logs hypertable took before compression.", nil, nil),
		comp:  prometheus.NewDesc(name("compressed_bytes"), "Bytes the compressed chunks of the logs hypertable take now.", nil, nil),
	}
}

func (c *compressionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ratio
	ch <- c.raw
	ch <- c.comp
}

func (c *compressionCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var before, after int64
	if err := c.pool.QueryRow(ctx, compressionStatsSQL).Scan(&before, &after); err != nil {
		// A scrape must never fail because the database is slow; the pool
		// collector's own gauges say so already. Logged at debug because a
		// restarting database would otherwise log this every five seconds.
		c.log.Debug("compression stats unavailable", slog.Any("error", err))
		return
	}
	ratio := 0.0
	if after > 0 {
		ratio = float64(before) / float64(after)
	}
	ch <- prometheus.MustNewConstMetric(c.ratio, prometheus.GaugeValue, ratio)
	ch <- prometheus.MustNewConstMetric(c.raw, prometheus.GaugeValue, float64(before))
	ch <- prometheus.MustNewConstMetric(c.comp, prometheus.GaugeValue, float64(after))
}
