package config

import (
	"strings"
	"testing"
	"time"
)

func validWriter() Writer {
	return Writer{
		Workers:               4,
		BatchSize:             5000,
		FlushInterval:         250 * time.Millisecond,
		QueueDepth:            1024,
		WriteTimeout:          30 * time.Second,
		MaxAttempts:           5,
		RetryBaseDelay:        50 * time.Millisecond,
		RetryMaxDelay:         5 * time.Second,
		StreamCacheSize:       8192,
		StreamRefreshInterval: 5 * time.Minute,
	}
}

func TestWriterDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got, want := cfg.Writer, validWriter(); got != want {
		t.Fatalf("writer defaults = %+v, want %+v", got, want)
	}
	// The default must satisfy its own validation, or `make dev` fails on a clean
	// clone.
	if err := cfg.Writer.Validate(); err != nil {
		t.Fatalf("default writer config is invalid: %v", err)
	}
}

func TestWriterOverrides(t *testing.T) {
	t.Setenv("LOGAGG_WRITER_WORKERS", "8")
	t.Setenv("LOGAGG_WRITER_BATCH_SIZE", "1000")
	t.Setenv("LOGAGG_WRITER_FLUSH_INTERVAL", "1s")
	t.Setenv("LOGAGG_WRITER_QUEUE_DEPTH", "32")
	t.Setenv("LOGAGG_WRITER_MAX_ATTEMPTS", "2")
	t.Setenv("LOGAGG_WRITER_STREAM_REFRESH_INTERVAL", "90s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Writer.Workers != 8 {
		t.Errorf("Workers = %d, want 8", cfg.Writer.Workers)
	}
	if cfg.Writer.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", cfg.Writer.BatchSize)
	}
	if cfg.Writer.FlushInterval != time.Second {
		t.Errorf("FlushInterval = %s, want 1s", cfg.Writer.FlushInterval)
	}
	if cfg.Writer.QueueDepth != 32 {
		t.Errorf("QueueDepth = %d, want 32", cfg.Writer.QueueDepth)
	}
	if cfg.Writer.MaxAttempts != 2 {
		t.Errorf("MaxAttempts = %d, want 2", cfg.Writer.MaxAttempts)
	}
	if cfg.Writer.StreamRefreshInterval != 90*time.Second {
		t.Errorf("StreamRefreshInterval = %s, want 90s", cfg.Writer.StreamRefreshInterval)
	}
}

func TestWriterValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Writer)
		want   string
	}{
		{name: "valid", mutate: func(*Writer) {}},
		{name: "no workers", mutate: func(w *Writer) { w.Workers = 0 }, want: "workers must be at least 1"},
		{name: "no batch", mutate: func(w *Writer) { w.BatchSize = 0 }, want: "batch size must be at least 1"},
		{name: "no flush", mutate: func(w *Writer) { w.FlushInterval = 0 }, want: "flush interval must be positive"},
		{name: "no queue", mutate: func(w *Writer) { w.QueueDepth = 0 }, want: "queue depth must be at least 1"},
		{name: "no timeout", mutate: func(w *Writer) { w.WriteTimeout = 0 }, want: "write timeout must be positive"},
		{name: "no attempts", mutate: func(w *Writer) { w.MaxAttempts = 0 }, want: "max attempts must be at least 1"},
		{name: "no base delay", mutate: func(w *Writer) { w.RetryBaseDelay = 0 }, want: "retry base delay must be positive"},
		{
			name:   "max below base",
			mutate: func(w *Writer) { w.RetryMaxDelay = time.Nanosecond },
			want:   "retry max delay",
		},
		{name: "no cache", mutate: func(w *Writer) { w.StreamCacheSize = 0 }, want: "stream cache size must be at least 1"},
		{
			name:   "no refresh interval",
			mutate: func(w *Writer) { w.StreamRefreshInterval = 0 },
			want:   "stream refresh interval must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := validWriter()
			tc.mutate(&w)

			err := w.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want == "":
			case err == nil:
				t.Fatalf("expected an error mentioning %q, got nil", tc.want)
			case !strings.Contains(err.Error(), tc.want):
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestWriterValidateReportsEveryProblemAtOnce(t *testing.T) {
	w := Writer{}
	err := w.Validate()
	if err == nil {
		t.Fatal("expected an error for a zero-value writer config")
	}
	// Accumulating rather than failing fast: an operator fixing configuration should
	// see every mistake in one restart, not one per restart.
	if got := strings.Count(err.Error(), "\n") + 1; got < 8 {
		t.Fatalf("expected at least 8 problems reported, got %d:\n%v", got, err)
	}
}

func TestConfigRejectsMoreWorkersThanConnections(t *testing.T) {
	t.Setenv("LOGAGG_WRITER_WORKERS", "32")
	t.Setenv("LOGAGG_DB_MAX_CONNS", "8")

	_, err := Load()
	if err == nil {
		t.Fatal("expected an error when workers exceed db max conns")
	}
	if !strings.Contains(err.Error(), "exceeds db max conns") {
		t.Fatalf("error does not explain the problem: %v", err)
	}
}
