// Package backoff is the one exponential-with-full-jitter delay shared by every
// retry loop in the tree.
//
// Full jitter rather than a fixed multiple: when a dependency briefly refuses
// connections, every client fails at once, and without jitter they all retry at
// the same instant and refuse it again.
package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

// Delay returns the wait before the given attempt (1-based; values below 1 are
// treated as 1), uniformly random in [1ns, min(base<<(attempt-1), maxDelay)].
// The floor of one nanosecond keeps a tight failure loop from becoming a busy one.
func Delay(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base << min(attempt-1, 16)
	if delay <= 0 || delay > maxDelay {
		delay = maxDelay
	}
	return time.Duration(rand.Int64N(int64(delay)) + 1) //nolint:gosec // jitter, not a secret
}

// Sleep waits for d, reporting false if ctx was canceled first.
func Sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
