-- Per-minute and per-hour record counts, maintained incrementally by Timescale.
--
-- These exist so that a query like `rate(5m) by (level)` over a week does not
-- scan a week of raw rows. The query planner (phase 4) chooses between logs,
-- logs_rate_1m and logs_rate_1h from the requested range and bucket width, and
-- reports the choice in the API response.

-- WITH NO DATA is mandatory here, not an optimization: golang-migrate sends each
-- migration file as a single multi-statement query, which Postgres wraps in an
-- implicit transaction, and Timescale rejects
-- "CREATE MATERIALIZED VIEW ... WITH DATA cannot run inside a transaction block".
-- It is also the right behaviour -- the refresh policy below backfills.
CREATE MATERIALIZED VIEW logs_rate_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       stream_id,
       level,
       count(*) AS n
FROM logs
GROUP BY bucket, stream_id, level
WITH NO DATA;

-- Rolled up from the 1 minute view rather than from logs. A hierarchical
-- aggregate re-reads a few thousand pre-aggregated rows per hour instead of
-- millions of raw ones, so the hourly refresh stays cheap as the table grows.
CREATE MATERIALIZED VIEW logs_rate_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket) AS bucket,
       stream_id,
       level,
       sum(n) AS n
FROM logs_rate_1m
GROUP BY 1, 2, 3
WITH NO DATA;

-- Real-time aggregation on. Since Timescale 2.13 continuous aggregates default to
-- materialized_only = true, which means a query against the view silently omits
-- everything newer than the last refresh -- for logs_rate_1m that is a
-- minutes-wide hole at the head of the time range, exactly where a log query
-- usually looks. Trading some query cost for a correct answer is the only
-- defensible choice when the planner may substitute the view for the raw table.
ALTER MATERIALIZED VIEW logs_rate_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW logs_rate_1h SET (timescaledb.materialized_only = false);

-- start_offset must exceed end_offset plus the refresh interval, or a bucket can
-- be skipped. end_offset of 1 minute keeps the refresh off the bucket currently
-- being written, which would otherwise be materialized while still incomplete.
SELECT add_continuous_aggregate_policy('logs_rate_1m',
    start_offset      => INTERVAL '10 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

-- Known limitation, verified against Timescale 2.22.1: add_continuous_aggregate_policy
-- accepts a policy on both levels of a hierarchy, but a later
-- alter_job() on this one is rejected with "multiple refresh policies are not
-- supported for hierarchical continuous aggregates". Refreshing works; only in-place
-- retuning does not. To change this schedule, remove the policy and add it again --
-- which is what a migration would do anyway.
SELECT add_continuous_aggregate_policy('logs_rate_1h',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '5 minutes');
