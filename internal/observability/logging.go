package observability

import (
	"io"
	"log/slog"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

// NewLogger builds the process logger. Every line carries the node name and
// build version, because the first question about any log line in a five-replica
// cluster is which replica emitted it.
func NewLogger(cfg config.Log, w io.Writer, node string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:       cfg.Level,
		AddSource:   cfg.AddSource,
		ReplaceAttr: readableDurations,
	}

	var h slog.Handler
	if cfg.Format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}

	return slog.New(h).With(
		slog.String("node", node),
		slog.String("version", version.Version),
	)
}

// readableDurations renders durations as "1.5s" instead of a raw nanosecond
// integer. The JSON handler's default turns a 20s timeout into 20000000000,
// which nobody reads correctly at a glance during an incident.
func readableDurations(_ []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindDuration {
		return slog.String(a.Key, a.Value.Duration().String())
	}
	return a
}
