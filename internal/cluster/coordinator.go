package cluster

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
)

// Coordinator answers a query by fanning its logs statement out to the
// members that own the matching streams and merging what comes back. It is
// the executor.Runner the HTTP API uses when clustering is on.
//
// Every collector writes to the same database, so ownership here is about
// dividing the scan, not about where the data is: the coordinator resolves
// the stream set once, gives each owner its share of the ids, and each owner
// scans only those. A shard whose owner is unreachable is reported as a
// warning and the rest of the result stands; degrading with a note beats
// failing the whole query.
type Coordinator struct {
	db      executor.Querier
	cluster *Cluster
	peers   *Peers
	log     *slog.Logger
}

// NewCoordinator wires the local database, the membership view and the peer
// client pool.
func NewCoordinator(db executor.Querier, c *Cluster, peers *Peers, log *slog.Logger) *Coordinator {
	return &Coordinator{db: db, cluster: c, peers: peers, log: log.With(slog.String("component", "coordinator"))}
}

// shard is one owner's share of a query.
type shard struct {
	member Member
	local  bool
	ids    []int64
}

// Run implements executor.Runner.
func (c *Coordinator) Run(ctx context.Context, q *query.Query, r query.Request) (*executor.Result, error) {
	plan, limit, err := executor.Plan(q, r)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	ids, err := executor.ResolveStreams(ctx, c.db, plan)
	if err != nil {
		return nil, err
	}
	res := &executor.Result{Source: plan.Source, Start: plan.Start, End: plan.End, Streams: len(ids)}
	if len(ids) == 0 {
		res.Elapsed = time.Since(started)
		return res, nil
	}

	shards := c.shards(ids)
	shape := executor.ShapeOf(q)
	results := make([]shardResult, len(shards))
	var wg sync.WaitGroup
	for i, s := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = c.execute(ctx, plan.Logs, &s, shape)
		}()
	}
	wg.Wait()

	var records [][]model.LogRecord
	var points [][]executor.Point
	for i, sr := range results {
		if sr.err != nil {
			// A failed remote shard is a warning. A failed local shard is the
			// query failing, as it would on one node, since it is this node's
			// own database that refused; and a canceled query is the caller's
			// doing, not a partial result. The error text itself stays in the
			// log, where execute put it: a database message does not belong
			// in a response, which is the single-node handler's policy too.
			if ctx.Err() != nil || shards[i].local {
				return nil, sr.err
			}
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s unreachable: %d streams not searched", shards[i].member.Name, len(shards[i].ids)))
			continue
		}
		records = append(records, sr.records)
		points = append(points, sr.points)
		// Each shard applied the row cap itself, so a shard at the cap means
		// the merged result is a prefix even before the final cut.
		if len(sr.records) > limit || len(sr.points) > limit {
			res.Truncated = true
		}
	}
	if shape.Aggregate {
		res.Points = mergePoints(points, r.Direction, shape.By)
	} else {
		res.Records = mergeRecords(records, r.Direction, limit+1)
	}
	res.Truncate(limit)
	res.Elapsed = time.Since(started)
	return res, nil
}

// shards groups the ids by owner. Ids whose owner is not ready, or not in the
// ring at all, are scanned locally: the database is shared, so this node can
// answer for a member that is starting or stopping without a warning.
func (c *Coordinator) shards(ids []int64) []shard {
	self := c.cluster.Self()
	byOwner := make(map[string]*shard)
	order := []string{}
	for _, id := range ids {
		m, ok := c.cluster.Owner(model.StreamID(id))
		if !ok || !m.Ready || m.Name == self {
			m = Member{Name: self}
		}
		s, seen := byOwner[m.Name]
		if !seen {
			s = &shard{member: m, local: m.Name == self}
			byOwner[m.Name] = s
			order = append(order, m.Name)
		}
		s.ids = append(s.ids, id)
	}
	out := make([]shard, 0, len(order))
	for _, name := range order {
		out = append(out, *byOwner[name])
	}
	return out
}

type shardResult struct {
	records []model.LogRecord
	points  []executor.Point
	err     error
}

// execute runs one shard, here or on its owner. The statement is copied so
// concurrent shards each bind their own ids.
func (c *Coordinator) execute(ctx context.Context, logs query.Stmt, s *shard, shape executor.Shape) shardResult {
	stmt := query.Stmt{SQL: logs.SQL, Args: slices.Clone(logs.Args)}
	stmt.Args[0] = s.ids
	var sr shardResult
	if s.local {
		sr.records, sr.points, sr.err = executor.ExecLogs(ctx, c.db, stmt, shape)
	} else {
		sr.records, sr.points, sr.err = c.peers.Execute(ctx, s.member.PeerAddr(), stmt, shape)
	}
	if sr.err != nil && ctx.Err() == nil {
		c.log.Warn("shard failed", slog.String("owner", s.member.Name), slog.Int("streams", len(s.ids)), slog.Any("error", sr.err))
	}
	return sr
}

// mergeRecords k-way merges per-shard record lists, each already in the
// direction's order, and stops after limit rows. Records order by time then
// seq, newest first for Backward.
func mergeRecords(shards [][]model.LogRecord, dir query.Direction, limit int) []model.LogRecord {
	h := &recordHeap{less: recordLess(dir)}
	for i, s := range shards {
		if len(s) > 0 {
			h.items = append(h.items, cursor{shard: i, rec: s[0]})
		}
	}
	heap.Init(h)
	pos := make([]int, len(shards))
	var out []model.LogRecord
	for h.Len() > 0 && len(out) < limit {
		top := heap.Pop(h).(cursor) //nolint:errcheck // heap of cursors only
		out = append(out, top.rec)
		pos[top.shard]++
		if next := pos[top.shard]; next < len(shards[top.shard]) {
			heap.Push(h, cursor{shard: top.shard, rec: shards[top.shard][next]})
		}
	}
	return out
}

func recordLess(dir query.Direction) func(a, b model.LogRecord) bool {
	return func(a, b model.LogRecord) bool {
		if !a.Time.Equal(b.Time) {
			if dir == query.Forward {
				return a.Time.Before(b.Time)
			}
			return a.Time.After(b.Time)
		}
		if dir == query.Forward {
			return a.Seq < b.Seq
		}
		return a.Seq > b.Seq
	}
}

type cursor struct {
	shard int
	rec   model.LogRecord
}

type recordHeap struct {
	items []cursor
	less  func(a, b model.LogRecord) bool
}

func (h *recordHeap) Len() int           { return len(h.items) }
func (h *recordHeap) Less(i, j int) bool { return h.less(h.items[i].rec, h.items[j].rec) }
func (h *recordHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *recordHeap) Push(x any)         { h.items = append(h.items, x.(cursor)) } //nolint:errcheck // heap of cursors only
func (h *recordHeap) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

// mergePoints combines per-shard series. Every aggregation is a count or a
// sum, so the same bucket and group from two shards add. The merged points
// are ordered like a single node's: bucket in the direction's order, then the
// by values ascending with a missing value last.
func mergePoints(shards [][]executor.Point, dir query.Direction, by []string) []executor.Point {
	merged := make(map[string]*executor.Point)
	var keys []string
	for _, s := range shards {
		for _, p := range s {
			k := pointKey(p, by)
			if m, ok := merged[k]; ok {
				m.Value += p.Value
				continue
			}
			cp := p
			merged[k] = &cp
			keys = append(keys, k)
		}
	}
	out := make([]executor.Point, 0, len(keys))
	for _, k := range keys {
		out = append(out, *merged[k])
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if !a.Bucket.Equal(b.Bucket) {
			if dir == query.Forward {
				return a.Bucket.Before(b.Bucket)
			}
			return a.Bucket.After(b.Bucket)
		}
		for _, name := range by {
			av, aok := a.Labels[name]
			bv, bok := b.Labels[name]
			switch {
			case aok && !bok:
				return true
			case !aok && bok:
				return false
			case av != bv:
				return av < bv
			}
		}
		return false
	})
	return out
}

// pointKey identifies a bucket and group; a missing label is distinct from
// an empty one, matching the NULL the database returned.
func pointKey(p executor.Point, by []string) string {
	var b strings.Builder
	b.WriteString(p.Bucket.UTC().Format(time.RFC3339Nano))
	for _, name := range by {
		v, ok := p.Labels[name]
		if ok {
			b.WriteString("\x01" + v)
		} else {
			b.WriteString("\x00")
		}
	}
	return b.String()
}
