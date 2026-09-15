-- Recreate what create_hypertable would have made: a plain (time DESC) index on
-- the hypertable, which Timescale propagates to every chunk.
CREATE INDEX IF NOT EXISTS logs_time_idx ON logs (time DESC);
