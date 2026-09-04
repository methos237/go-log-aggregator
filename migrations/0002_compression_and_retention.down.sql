-- Policies first: a running compression job would race a decompress below.
SELECT remove_retention_policy('logs', if_exists => true);
SELECT remove_compression_policy('logs', if_exists => true);

-- Compression cannot be disabled while compressed chunks exist, so every chunk
-- has to be expanded back to row storage first. if_compressed => false makes
-- already-uncompressed chunks a no-op instead of an error.
SELECT decompress_chunk(chunk, if_compressed => false)
FROM show_chunks('logs') AS chunk;

ALTER TABLE logs SET (timescaledb.compress = false);
