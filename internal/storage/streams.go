package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// upsertStreamsSQL writes a set of streams in one statement.
//
// unnest over parallel arrays rather than a generated VALUES list: the SQL text is
// then a constant, which means one prepared-statement cache entry instead of one
// per distinct batch size, and there is no string building anywhere near a query.
//
// GREATEST on last_seen keeps the column monotonic. Without it, two collectors
// writing the same stream out of order would make last_seen jitter backwards.
// Nothing reads the column yet; it is kept current so a stream
// retirement or ranking feature has the data when it wants it.
//
// first_seen is intentionally not updated: it records when the stream was first
// observed by the cluster, and EXCLUDED.first_seen from a later batch is newer.
const upsertStreamsSQL = `
INSERT INTO streams (stream_id, service, host, env, labels, first_seen, last_seen)
SELECT * FROM unnest(
    $1::bigint[], $2::text[], $3::text[], $4::text[],
    $5::jsonb[], $6::timestamptz[], $7::timestamptz[]
)
ON CONFLICT (stream_id) DO UPDATE
   SET last_seen = GREATEST(streams.last_seen, EXCLUDED.last_seen)`

// streamCache remembers which streams are already in the dimension table.
//
// Without it every batch would upsert every stream it touches, which for a
// thousand-record batch from one stream is a wasted round trip, and at cluster
// scale is a hot row that every node contends on.
//
// The cached value is when this process last refreshed last_seen for that stream,
// not the stream itself: nothing here needs the labels back, only the answer to
// "must I write this row again".
type streamCache struct {
	entries *lru[model.StreamID, time.Time]
	// refreshAfter bounds how stale last_seen may get. The tradeoff is explicit:
	// smaller values keep /v1/labels accurate, larger values cut writes. Because
	// GREATEST guards the column, a stale refresh can never move it backwards.
	refreshAfter time.Duration
	metrics      *Metrics
}

func newStreamCache(capacity int, refreshAfter time.Duration, metrics *Metrics) *streamCache {
	c := &streamCache{
		entries:      newLRU[model.StreamID, time.Time](capacity),
		refreshAfter: refreshAfter,
		metrics:      metrics,
	}
	if metrics != nil {
		metrics.StreamCacheCapacity.Set(float64(capacity))
	}
	return c
}

// needsUpsert reports whether the stream must be written, as of now.
func (c *streamCache) needsUpsert(id model.StreamID, now time.Time) bool {
	refreshed, ok := c.entries.Get(id)
	if !ok {
		if c.metrics != nil {
			c.metrics.StreamCacheMisses.Inc()
		}
		return true
	}
	if now.Sub(refreshed) >= c.refreshAfter {
		if c.metrics != nil {
			c.metrics.StreamCacheMisses.Inc()
		}
		return true
	}
	if c.metrics != nil {
		c.metrics.StreamCacheHits.Inc()
	}
	return false
}

// markUpserted records that the stream is present as of at.
//
// Called only after the transaction commits. Caching on send instead would mean a
// rolled-back batch left the cache claiming a stream exists when it does not, and
// the next batch's log rows would fail their foreign key.
func (c *streamCache) markUpserted(id model.StreamID, at time.Time) {
	c.entries.Put(id, at)
	if c.metrics != nil {
		c.metrics.StreamCacheEntries.Set(float64(c.entries.Len()))
	}
}

// upsertStreams writes the given streams inside tx.
//
// streams must not contain duplicate IDs: ON CONFLICT DO UPDATE cannot touch the
// same row twice in one command, and Postgres reports that as an error rather than
// merging. The caller collects streams into a map keyed by ID, which is what makes
// that hold.
func upsertStreams(ctx context.Context, tx pgx.Tx, streams []model.Stream) error {
	if len(streams) == 0 {
		return nil
	}

	var (
		ids       = make([]int64, len(streams))
		services  = make([]string, len(streams))
		hosts     = make([]string, len(streams))
		envs      = make([]string, len(streams))
		labels    = make([]string, len(streams))
		firstSeen = make([]time.Time, len(streams))
		lastSeen  = make([]time.Time, len(streams))
	)

	for i, s := range streams {
		ids[i] = int64(s.ID)
		services[i] = s.Labels.Service
		hosts[i] = s.Labels.Host
		envs[i] = s.Labels.Env
		labels[i] = encodeLabels(s.Labels)
		firstSeen[i] = s.FirstSeen
		lastSeen[i] = s.LastSeen
	}

	if _, err := tx.Exec(ctx, upsertStreamsSQL,
		ids, services, hosts, envs, labels, firstSeen, lastSeen,
	); err != nil {
		return fmt.Errorf("upsert %d streams: %w", len(streams), err)
	}
	return nil
}

// encodeLabels renders the extra labels as a JSON object for the JSONB column.
//
// Always an object, never null: the column is NOT NULL, and `labels @> '{...}'`
// containment in generated queries is simpler when there is no null case.
// encoding/json sorts map keys, so the same label set always produces identical
// bytes, which keeps the stored value stable across rewrites.
func encodeLabels(ls model.LabelSet) string {
	if len(ls.Extra) == 0 {
		return "{}"
	}
	return string(marshalFields(ls.Extra))
}

// marshalFields renders a string map as a JSON object for a JSONB column.
//
// Shared by the streams upsert and the per-record fields column so both produce
// byte-identical encodings for the same map. encoding/json sorts map keys, which is
// what makes that guarantee hold. Marshaling a map[string]string cannot fail
// (invalid UTF-8 is coerced, not rejected), so the error is discarded.
func marshalFields(m map[string]string) []byte {
	encoded, _ := json.Marshal(m)
	return encoded
}
