-- Dropping the hypertable takes its chunks and indexes with it.
DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS streams;

-- The extension is deliberately left in place. Another database in the same
-- cluster may be using it, and re-creating it is cheap while dropping it is not
-- something a schema rollback should decide.
