//go:build integration

package integration

import (
	"log/slog"
	"testing"
)

// testLogger routes component logs into the test's own output, so a failure shows
// the migration or writer log lines that led to it instead of nothing.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	// Trailing newline trimmed: t.Log adds its own.
	w.t.Log(string(p[:max(0, len(p)-1)]))
	return len(p), nil
}
