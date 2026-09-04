package storage

import (
	"strconv"
	"sync"
	"testing"
)

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	t.Parallel()

	c := newLRU[string, int](3)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("c", 3)

	// Touch "a" so "b" becomes the eviction candidate rather than "a".
	if got, ok := c.Get("a"); !ok || got != 1 {
		t.Fatalf(`Get("a") = %d, %t`, got, ok)
	}

	c.Put("d", 4)

	if _, ok := c.Get("b"); ok {
		t.Error(`"b" should have been evicted`)
	}
	for _, key := range []string{"a", "c", "d"} {
		if _, ok := c.Get(key); !ok {
			t.Errorf("%q should still be cached", key)
		}
	}
	if got := c.Len(); got != 3 {
		t.Errorf("Len() = %d, want 3", got)
	}
}

func TestLRUPutUpdatesWithoutGrowing(t *testing.T) {
	t.Parallel()

	c := newLRU[string, int](2)
	c.Put("a", 1)
	c.Put("a", 2)

	if got, _ := c.Get("a"); got != 2 {
		t.Errorf(`Get("a") = %d, want 2`, got)
	}
	if got := c.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestLRUCapacityFloor(t *testing.T) {
	t.Parallel()

	// A cache built with capacity 0 must still cache one entry. Silently accepting
	// zero would make every lookup a miss while still looking like a working cache,
	// which in the writer means an upsert per batch forever.
	for _, capacity := range []int{0, -1} {
		c := newLRU[int, int](capacity)
		c.Put(1, 1)
		if _, ok := c.Get(1); !ok {
			t.Errorf("capacity %d: entry was not retained", capacity)
		}
	}
}

func TestLRUEvictionOrderAfterManyOperations(t *testing.T) {
	t.Parallel()

	const capacity = 8
	c := newLRU[int, int](capacity)
	for i := 0; i < 100; i++ {
		c.Put(i, i)
		if got := c.Len(); got > capacity {
			t.Fatalf("Len() = %d exceeds capacity %d after %d puts", got, capacity, i+1)
		}
	}

	// Only the last `capacity` keys survive a purely sequential insert pattern.
	for i := 0; i < 100-capacity; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("key %d should have been evicted", i)
		}
	}
	for i := 100 - capacity; i < 100; i++ {
		if _, ok := c.Get(i); !ok {
			t.Fatalf("key %d should still be cached", i)
		}
	}
}

// TestLRUIsConcurrencySafe exists for the race detector; the assertions are weak on
// purpose because with concurrent eviction almost nothing is deterministic. Every
// writer goroutine shares one cache, so a data race here would be a real bug.
func TestLRUIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	c := newLRU[string, int](64)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := strconv.Itoa((worker*i)%128) + ":" + strconv.Itoa(worker)
				c.Put(key, i)
				c.Get(key)
				c.Len()
			}
		}(w)
	}
	wg.Wait()

	if got := c.Len(); got > 64 {
		t.Fatalf("Len() = %d exceeds capacity", got)
	}
}

func BenchmarkLRUGetHit(b *testing.B) {
	c := newLRU[int, int](1024)
	for i := 0; i < 1024; i++ {
		c.Put(i, i)
	}
	i := 0
	for b.Loop() {
		c.Get(i & 1023)
		i++
	}
}
