-- Columnar compression and retention for the logs hypertable.
--
-- Compression converts a chunk from row storage to a per-column, per-segment
-- layout. For log data the win is large -- the same service, host and level repeat
-- for thousands of consecutive rows -- and phase 8 publishes the measured ratio.

ALTER TABLE logs SET (
    timescaledb.compress,
    -- Segment by stream_id so every compressed batch holds one stream's rows.
    -- Selectors resolve to a stream_id set first, so this lets a query read only
    -- the segments it needs and keeps stream_id itself stored once per segment
    -- rather than once per row.
    timescaledb.compress_segmentby = 'stream_id',
    -- Order within a segment by time DESC to match the query direction, with seq
    -- as a tiebreaker so records sharing a timestamp keep a stable order. Ordered
    -- columns also compress better: adjacent timestamps delta-encode well.
    timescaledb.compress_orderby = 'time DESC, seq'
);

-- 2 hours is two chunk intervals: the current chunk and the one before it stay
-- uncompressed and cheap to insert into, everything older is compressed. Setting
-- this below the chunk interval would mean compressing chunks that are still
-- receiving writes.
SELECT add_compression_policy('logs', INTERVAL '2 hours');

-- Retention drops whole chunks rather than deleting rows, so it costs almost
-- nothing. The continuous aggregates in 0003 are not affected: they keep their own
-- materialized data, which is the point of downsampling.
SELECT add_retention_policy('logs', INTERVAL '30 days');
