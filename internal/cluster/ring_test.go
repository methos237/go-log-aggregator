package cluster

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
	"pgregory.net/rapid"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

func nodeNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("collector-%d", i)
	}
	return names
}

func TestRingEmpty(t *testing.T) {
	r := NewRing(nil, 0)
	if r.Owner(42) != "" || r.Len() != 0 || r.Arcs() != nil {
		t.Errorf("empty ring should own nothing: owner %q, len %d", r.Owner(42), r.Len())
	}
	if d := Diff(r, r); d != nil {
		t.Errorf("Diff(empty, empty) = %v", d)
	}
}

func TestRingSingleNodeOwnsEverything(t *testing.T) {
	r := NewRing([]string{"only"}, 3)
	for _, id := range []model.StreamID{0, 1, -1, math.MaxInt64, math.MinInt64} {
		if got := r.Owner(id); got != "only" {
			t.Errorf("Owner(%d) = %q", id, got)
		}
	}
	if got := r.Tokens("only"); len(got) != 3 || !slices.IsSorted(got) {
		t.Errorf("Tokens = %v, want 3 sorted", got)
	}
	if got := r.Tokens("nope"); got != nil {
		t.Errorf("Tokens(non-member) = %v", got)
	}
}

func TestRingDeduplicatesNodes(t *testing.T) {
	r := NewRing([]string{"b", "a", "b"}, 4)
	if got := r.Nodes(); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("Nodes = %v", got)
	}
	if len(r.tokens) != 8 {
		t.Errorf("token count = %d, want 8", len(r.tokens))
	}
}

func TestRingArcsTileKeySpace(t *testing.T) {
	r := NewRing(nodeNames(3), 16)
	arcs := r.Arcs()
	if len(arcs) != 48 {
		t.Fatalf("arcs = %d, want 48", len(arcs))
	}
	for i := 1; i < len(arcs); i++ {
		if arcs[i].Lo != arcs[i-1].Hi {
			t.Errorf("arc %d starts at %d, previous ended at %d", i, arcs[i].Lo, arcs[i-1].Hi)
		}
	}
	if arcs[0].Lo != arcs[len(arcs)-1].Hi {
		t.Error("arcs do not wrap")
	}
	// The owner of an arc's Hi token is the arc's owner.
	for _, a := range arcs {
		if got := r.Owner(model.StreamID(a.Hi)); got != a.Owner { //nolint:gosec // test key
			t.Errorf("Owner(%d) = %q, arc says %q", a.Hi, got, a.Owner)
		}
	}
}

// TestRingProperties holds for any membership and any keys: ownership is a
// member and is deterministic; removing a node moves exactly that node's keys
// and nothing else; adding a node takes about 1/(N+1) of the keys, and only
// ever takes them, never shuffles keys between existing members; and Diff
// agrees with Owner about which keys moved.
func TestRingProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 8).Draw(t, "nodes")
		vnodes := rapid.SampledFrom([]int{16, 64, 128}).Draw(t, "vnodes")
		nodes := nodeNames(n)
		// Keys are real stream ids in spirit: uniform hashes, all distinct.
		// Drawing them raw lets rapid shrink to 500 zeros, which says nothing
		// about distribution.
		seed := rapid.Uint64().Draw(t, "seed")
		count := rapid.IntRange(300, 800).Draw(t, "keys")
		keys := make([]int64, count)
		for i := range keys {
			keys[i] = int64(xxhash.Sum64String(strconv.FormatUint(seed+uint64(i), 10))) //nolint:gosec // uniform key
		}

		base := NewRing(nodes, vnodes)
		owners := make(map[int64]string, len(keys))
		for _, k := range keys {
			o := base.Owner(model.StreamID(k))
			if !slices.Contains(nodes, o) {
				t.Fatalf("Owner(%d) = %q, not a member", k, o)
			}
			if again := NewRing(nodes, vnodes).Owner(model.StreamID(k)); again != o {
				t.Fatalf("Owner(%d) differs between identical rings: %q vs %q", k, o, again)
			}
			owners[k] = o
		}

		// Remove one node: its keys move, everyone else's stay.
		if n > 1 {
			gone := nodes[rapid.IntRange(0, n-1).Draw(t, "removed")]
			smaller := NewRing(slices.DeleteFunc(slices.Clone(nodes), func(s string) bool { return s == gone }), vnodes)
			for _, k := range keys {
				now := smaller.Owner(model.StreamID(k))
				switch {
				case owners[k] == gone && now == gone:
					t.Fatalf("key %d still owned by removed node", k)
				case owners[k] != gone && now != owners[k]:
					t.Fatalf("key %d moved from %q to %q though %q was removed", k, owners[k], now, gone)
				}
			}
			checkDiff(t, base, smaller, keys)
		}

		// Add one node: only it gains keys, and about 1/(N+1) of them.
		added := "collector-new"
		bigger := NewRing(append(slices.Clone(nodes), added), vnodes)
		moved := 0
		for _, k := range keys {
			now := bigger.Owner(model.StreamID(k))
			if now != owners[k] {
				if now != added {
					t.Fatalf("key %d moved from %q to existing member %q on add", k, owners[k], now)
				}
				moved++
			}
		}
		want := float64(len(keys)) / float64(n+1)
		if f := float64(moved); f < want*0.4 || f > want*2.2 {
			t.Fatalf("adding a node to %d moved %d of %d keys, want about %.0f", n, moved, len(keys), want)
		}
		checkDiff(t, base, bigger, keys)
	})
}

// checkDiff asserts Diff(old, cur) explains exactly the keys whose Owner
// changed: a key is inside some Transfer iff its owner differs, and the
// Transfer names the right pair.
func checkDiff(t *rapid.T, old, cur *Ring, keys []int64) {
	t.Helper()
	moves := Diff(old, cur)
	for _, k := range keys {
		h := uint64(k) //nolint:gosec // test key
		from, to := old.Owner(model.StreamID(k)), cur.Owner(model.StreamID(k))
		var hit *Transfer
		for i := range moves {
			if inArc(moves[i].Lo, moves[i].Hi, h) {
				hit = &moves[i]
				break
			}
		}
		switch {
		case from == to && hit != nil:
			t.Fatalf("key %d did not move but Diff lists %+v", k, *hit)
		case from != to && hit == nil:
			t.Fatalf("key %d moved %q -> %q but Diff has no range for it", k, from, to)
		case hit != nil && (hit.From != from || hit.To != to):
			t.Fatalf("key %d moved %q -> %q but Diff says %+v", k, from, to, *hit)
		}
	}
}

// inArc reports whether h is in the half-open range (lo, hi], which wraps
// when hi < lo. A range with lo == hi covers the whole space.
func inArc(lo, hi, h uint64) bool {
	switch {
	case lo == hi:
		return true
	case lo < hi:
		return h > lo && h <= hi
	default:
		return h > lo || h <= hi
	}
}

func TestRingDistribution(t *testing.T) {
	const keys = 200_000
	r := NewRing(nodeNames(5), DefaultVNodes)
	counts := map[string]int{}
	for i := int64(0); i < keys; i++ {
		counts[r.Owner(model.StreamID(i*0x9E3779B97F4A7C1))]++ // odd multiplier spreads sequential keys
	}
	mean := float64(keys) / 5
	for n, c := range counts {
		if dev := math.Abs(float64(c)-mean) / mean; dev > 0.15 {
			t.Errorf("%s owns %d keys, %.0f%% from the mean of %.0f", n, c, dev*100, mean)
		}
	}
}
