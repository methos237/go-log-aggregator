-- Refresh policies first, mirroring 0002. Dropping the view would remove its policy
-- anyway, but doing it explicitly first narrows the window in which Timescale's job
-- scheduler is refreshing an aggregate that this transaction is trying to drop --
-- which shows up as a deadlock between the scheduler's advisory lock and this
-- statement's relation lock, not as a clean error.
SELECT remove_continuous_aggregate_policy('logs_rate_1h', if_not_exists => true);
SELECT remove_continuous_aggregate_policy('logs_rate_1m', if_not_exists => true);

-- The 1 hour view reads from the 1 minute view, so it goes first.
DROP MATERIALIZED VIEW IF EXISTS logs_rate_1h;
DROP MATERIALIZED VIEW IF EXISTS logs_rate_1m;
