package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

func TestNewLoggerJSONCarriesNodeAndVersion(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(config.Log{Level: slog.LevelInfo, Format: "json"}, &buf, "collector-3")
	log.Info("hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not JSON: %q", buf.String())
	}
	if got := rec["node"]; got != "collector-3" {
		t.Errorf("node = %v, want collector-3", got)
	}
	if _, ok := rec["version"]; !ok {
		t.Error("version attribute missing")
	}
}

func TestNewLoggerRendersDurationsReadably(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(config.Log{Level: slog.LevelInfo, Format: "json"}, &buf, "n1")
	log.Info("shutting down", slog.Duration("timeout", 20*time.Second))

	// The default JSON encoding would emit 20000000000, which reads as garbage.
	if !strings.Contains(buf.String(), `"timeout":"20s"`) {
		t.Errorf("duration not rendered as a string:\n%s", buf.String())
	}
}

func TestNewLoggerTextFormat(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(config.Log{Level: slog.LevelInfo, Format: "text"}, &buf, "n1")
	log.Info("hello")

	if strings.HasPrefix(strings.TrimSpace(buf.String()), "{") {
		t.Errorf("text format produced JSON:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "node=n1") {
		t.Errorf("node attribute missing:\n%s", buf.String())
	}
}

func TestNewLoggerRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(config.Log{Level: slog.LevelWarn, Format: "json"}, &buf, "n1")

	log.Info("suppressed")
	if buf.Len() != 0 {
		t.Errorf("info logged at warn level:\n%s", buf.String())
	}

	log.Warn("emitted")
	if buf.Len() == 0 {
		t.Error("warn not logged at warn level")
	}
}
