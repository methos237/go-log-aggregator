package storage

import "sync"

// lru is a fixed-capacity, least-recently-used cache.
//
// Hand-rolled rather than pulled from a dependency: it is one file, it is on the
// hot write path, and this project's premise is that the interesting code should be
// readable. container/list would have worked but stores elements as `any`, which
// costs a type assertion and an interface box per lookup.
//
// Safe for concurrent use. Every writer goroutine shares one instance on purpose:
// the cache exists to avoid a database round trip, so a per-worker cache would
// multiply the miss rate by the worker count.
type lru[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	index    map[K]*lruNode[K, V]
	// head is the most recently used node, tail the least. Both nil when empty.
	head *lruNode[K, V]
	tail *lruNode[K, V]
}

type lruNode[K comparable, V any] struct {
	key        K
	value      V
	prev, next *lruNode[K, V]
}

// newLRU returns a cache holding at most capacity entries. A capacity below 1 is
// raised to 1: a zero-capacity cache would evict everything it stores and turn
// every lookup into a miss while still looking like a cache.
func newLRU[K comparable, V any](capacity int) *lru[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &lru[K, V]{
		capacity: capacity,
		index:    make(map[K]*lruNode[K, V], capacity),
	}
}

// Get returns the value for key and marks it most recently used.
func (c *lru[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.index[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.moveToFront(n)
	return n.value, true
}

// Put inserts or updates key, evicting the least recently used entry when full.
func (c *lru[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n, ok := c.index[key]; ok {
		n.value = value
		c.moveToFront(n)
		return
	}

	n := &lruNode[K, V]{key: key, value: value}
	c.index[key] = n
	c.pushFront(n)

	if len(c.index) > c.capacity {
		if oldest := c.tail; oldest != nil {
			c.unlink(oldest)
			delete(c.index, oldest.key)
		}
	}
}

// Len reports the number of cached entries.
func (c *lru[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.index)
}

func (c *lru[K, V]) pushFront(n *lruNode[K, V]) {
	n.prev = nil
	n.next = c.head
	if c.head != nil {
		c.head.prev = n
	}
	c.head = n
	if c.tail == nil {
		c.tail = n
	}
}

func (c *lru[K, V]) moveToFront(n *lruNode[K, V]) {
	if c.head == n {
		return
	}
	c.unlink(n)
	c.pushFront(n)
}

func (c *lru[K, V]) unlink(n *lruNode[K, V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else if c.head == n {
		c.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else if c.tail == n {
		c.tail = n.prev
	}
	n.prev, n.next = nil, nil
}
