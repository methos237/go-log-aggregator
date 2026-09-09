// Package storage owns everything that touches TimescaleDB: the connection pool,
// the schema migrations, the stream dimension table and the writer pool that turns
// a stream of records into COPY batches.
//
// The write path is the part worth reading. It is built around three facts:
//
//  1. Delivery is at-least-once, so a record can arrive twice and the schema
//     deduplicates it (see migrations/0001, index logs_dedup).
//  2. COPY is roughly an order of magnitude faster than multi-row INSERT, but it
//     has no ON CONFLICT, so a replayed batch would abort the whole copy on the
//     unique index. The writer therefore COPYs into a session-local staging table
//     and moves rows across with INSERT ... SELECT ... ON CONFLICT DO NOTHING.
//  3. Every log row has a foreign key to streams, so a batch must upsert the
//     streams it references in the same transaction, and an in-memory cache keeps
//     that from being a round trip per record.
package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// Open builds a connection pool from configuration and verifies it can reach the
// database before returning.
//
// Verifying up front is deliberate: pgxpool.New is lazy, so without a ping a
// misconfigured DSN would surface as a failed write minutes later instead of as a
// startup error.
func Open(ctx context.Context, cfg config.DB, appName string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse db dsn: %w", err)
	}

	poolCfg.MaxConns = int32(cfg.MaxConns) //nolint:gosec // bounded by config validation
	poolCfg.MinConns = int32(cfg.MinConns) //nolint:gosec // bounded by config validation
	poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime
	// Without jitter every connection opened at startup expires at the same
	// instant, and the pool reconnects all of them at once against a database that
	// is also serving writes. 10% is enough to smear that out.
	poolCfg.MaxConnLifetimeJitter = cfg.ConnMaxLifetime / 10
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	// Shows up in pg_stat_activity, which is how you find out which collector is
	// responsible for a long-running query.
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolCfg.ConnConfig.RuntimeParams["application_name"] = appName

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create db pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return pool, nil
}

// poolCollector exports pgxpool's own statistics.
//
// Implemented as a Collector reading Stat() on scrape rather than as gauges
// updated on a ticker: the numbers are already maintained by the pool, so
// mirroring them into a background goroutine would only add a way for them to go
// stale.
type poolCollector struct {
	pool *pgxpool.Pool

	acquiredConns    *prometheus.Desc
	idleConns        *prometheus.Desc
	totalConns       *prometheus.Desc
	maxConns         *prometheus.Desc
	acquireCount     *prometheus.Desc
	acquireWaitCount *prometheus.Desc
	acquireWaitTime  *prometheus.Desc
}

// NewPoolCollector returns a Prometheus collector for pool statistics.
//
// Saturation shows up here first: empty_acquire_total climbing means callers are
// queueing for connections, which is the signal to raise MaxConns or lower the
// writer's concurrency.
func NewPoolCollector(pool *pgxpool.Pool) prometheus.Collector {
	const sub = "db_pool"
	name := func(n string) string {
		return prometheus.BuildFQName(observability.Namespace, sub, n)
	}
	return &poolCollector{
		pool:             pool,
		acquiredConns:    prometheus.NewDesc(name("acquired_connections"), "Connections currently checked out.", nil, nil),
		idleConns:        prometheus.NewDesc(name("idle_connections"), "Connections open and idle.", nil, nil),
		totalConns:       prometheus.NewDesc(name("total_connections"), "Connections currently open.", nil, nil),
		maxConns:         prometheus.NewDesc(name("max_connections"), "Configured connection ceiling.", nil, nil),
		acquireCount:     prometheus.NewDesc(name("acquires_total"), "Successful connection acquisitions.", nil, nil),
		acquireWaitCount: prometheus.NewDesc(name("empty_acquires_total"), "Acquisitions that had to wait for a free connection.", nil, nil),
		acquireWaitTime:  prometheus.NewDesc(name("acquire_wait_seconds_total"), "Cumulative time spent waiting for a connection.", nil, nil),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquiredConns
	ch <- c.idleConns
	ch <- c.totalConns
	ch <- c.maxConns
	ch <- c.acquireCount
	ch <- c.acquireWaitCount
	ch <- c.acquireWaitTime
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	gauge(c.acquiredConns, float64(s.AcquiredConns()))
	gauge(c.idleConns, float64(s.IdleConns()))
	gauge(c.totalConns, float64(s.TotalConns()))
	gauge(c.maxConns, float64(s.MaxConns()))
	counter(c.acquireCount, float64(s.AcquireCount()))
	counter(c.acquireWaitCount, float64(s.EmptyAcquireCount()))
	counter(c.acquireWaitTime, s.AcquireDuration().Seconds())
}
