package backoff

import (
	"context"
	"testing"
	"time"
)

func TestDelayBounded(t *testing.T) {
	const base, maxDelay = 100 * time.Millisecond, time.Second
	for attempt := -1; attempt < 40; attempt++ {
		for range 50 {
			got := Delay(attempt, base, maxDelay)
			cap := min(base<<max(attempt-1, 0), maxDelay)
			if attempt > 16 || cap <= 0 {
				cap = maxDelay
			}
			if got <= 0 || got > cap {
				t.Fatalf("Delay(%d) = %s, want in (0, %s]", attempt, got, cap)
			}
		}
	}
}

func TestSleepHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) {
		t.Fatal("Sleep returned true on canceled ctx")
	}
	if !Sleep(context.Background(), time.Millisecond) {
		t.Fatal("Sleep returned false without cancellation")
	}
}
