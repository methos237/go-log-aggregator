// Package cluster is the membership and ownership layer: a consistent hash
// ring over the collector nodes, kept current from memberlist events, used to
// route query fan-out. Writes never consult it, and neither does live tail,
// which fans out over core NATS; every node writes through JetStream.
package cluster

import (
	"slices"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// DefaultVNodes is the virtual-node count per member when none is configured.
// 128 keeps the largest and smallest shares within roughly 10% of the mean
// for the cluster sizes this project targets; phase 8 measures the variance.
const DefaultVNodes = 128

// Ring is a consistent hash ring with virtual nodes, keyed by StreamID. A
// stream belongs to the member owning the first token at or after its hash,
// wrapping past the top. Rebuilt whole on every membership change: a few
// thousand tokens sort in microseconds, and one immutable value per epoch is
// simpler to reason about than incremental edits under a lock.
//
// A Ring is immutable after construction and safe for concurrent reads.
type Ring struct {
	tokens []token
	nodes  []string
}

type token struct {
	hash uint64
	node string
}

// NewRing builds a ring over nodes with vnodes tokens each. Node names must be
// unique; duplicates are collapsed. vnodes below 1 uses DefaultVNodes.
func NewRing(nodes []string, vnodes int) *Ring {
	if vnodes < 1 {
		vnodes = DefaultVNodes
	}
	nodes = slices.Clone(nodes)
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)

	r := &Ring{nodes: nodes, tokens: make([]token, 0, len(nodes)*vnodes)}
	for _, n := range nodes {
		for i := 0; i < vnodes; i++ {
			r.tokens = append(r.tokens, token{hash: tokenHash(n, i), node: n})
		}
	}
	// Sort by hash, then by node so two nodes colliding on a token resolve
	// the same way on every member.
	sort.Slice(r.tokens, func(i, j int) bool {
		if r.tokens[i].hash != r.tokens[j].hash {
			return r.tokens[i].hash < r.tokens[j].hash
		}
		return r.tokens[i].node < r.tokens[j].node
	})
	return r
}

// tokenHash places virtual node i of node on the ring. The separator keeps
// "a"+"10" and "a1"+"0" apart.
func tokenHash(node string, i int) uint64 {
	return xxhash.Sum64String(node + "\x00" + strconv.Itoa(i))
}

// Owner returns the member owning id, or "" on an empty ring. StreamID is
// already an xxhash64 of the label set, so it is used as the key directly.
func (r *Ring) Owner(id model.StreamID) string {
	if len(r.tokens) == 0 {
		return ""
	}
	return r.tokens[r.slot(uint64(id))].node //nolint:gosec // StreamID is a reinterpreted uint64 hash
}

// slot is the index of the first token at or after h, wrapping to 0.
func (r *Ring) slot(h uint64) int {
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i].hash >= h })
	if i == len(r.tokens) {
		return 0
	}
	return i
}

// Nodes returns the members, sorted.
func (r *Ring) Nodes() []string { return slices.Clone(r.nodes) }

// Len is the member count.
func (r *Ring) Len() int { return len(r.nodes) }

// Tokens returns node's positions on the ring, sorted; nil for a non-member.
func (r *Ring) Tokens(node string) []uint64 {
	var out []uint64
	for _, t := range r.tokens {
		if t.node == node {
			out = append(out, t.hash)
		}
	}
	return out
}

// Arc is a half-open range of the key space (Lo, Hi] owned by one member. The
// arc that wraps past the top has Hi < Lo.
type Arc struct {
	Lo, Hi uint64
	Owner  string
}

// Arcs returns the ring as owned ranges in token order, one per token. The
// first arc starts after the last token, so the arcs tile the whole key space.
func (r *Ring) Arcs() []Arc {
	if len(r.tokens) == 0 {
		return nil
	}
	arcs := make([]Arc, len(r.tokens))
	prev := r.tokens[len(r.tokens)-1].hash
	for i, t := range r.tokens {
		arcs[i] = Arc{Lo: prev, Hi: t.hash, Owner: t.node}
		prev = t.hash
	}
	return arcs
}

// Shares returns each member's fraction of the key space, summing to 1 on a
// non-empty ring. It is the number /v1/cluster shows next to each member so
// an uneven ring is visible without reading tokens.
func (r *Ring) Shares() map[string]float64 {
	shares := make(map[string]float64, len(r.nodes))
	for _, a := range r.Arcs() {
		// Hi - Lo wraps correctly in uint64 arithmetic for the arc that
		// crosses the top; a single token owns the whole space.
		span := a.Hi - a.Lo
		if len(r.tokens) == 1 {
			span = ^uint64(0)
		}
		shares[a.Owner] += float64(span) / (1 << 64)
	}
	return shares
}

// Transfer is a key range whose owner differs between two rings.
type Transfer struct {
	Lo, Hi   uint64
	From, To string
}

// Diff lists the key ranges that change owner going from old to cur, in ring
// order. Membership changes log these so an ownership move is visible with
// its before and after, and the count feeds the ownership-changes metric.
func Diff(old, cur *Ring) []Transfer {
	// Every boundary from either ring; between two consecutive boundaries the
	// owner is constant in both rings, so one lookup each decides the range.
	bounds := make([]uint64, 0, len(old.tokens)+len(cur.tokens))
	for _, t := range old.tokens {
		bounds = append(bounds, t.hash)
	}
	for _, t := range cur.tokens {
		bounds = append(bounds, t.hash)
	}
	slices.Sort(bounds)
	bounds = slices.Compact(bounds)
	if len(bounds) == 0 {
		return nil
	}

	var out []Transfer
	prev := bounds[len(bounds)-1]
	for _, b := range bounds {
		// Any key in (prev, b] resolves to the token at b in whichever ring
		// has one there, and to the next token up otherwise: b itself is the
		// representative key.
		from, to := old.ownerOf(b), cur.ownerOf(b)
		if from != to {
			// Merge with the previous transfer when it is the same move and
			// ends where this one starts, so a run of tokens is one range.
			if n := len(out); n > 0 && out[n-1].Hi == prev && out[n-1].From == from && out[n-1].To == to {
				out[n-1].Hi = b
			} else {
				out = append(out, Transfer{Lo: prev, Hi: b, From: from, To: to})
			}
		}
		prev = b
	}
	return out
}

func (r *Ring) ownerOf(h uint64) string {
	if len(r.tokens) == 0 {
		return ""
	}
	return r.tokens[r.slot(h)].node
}
