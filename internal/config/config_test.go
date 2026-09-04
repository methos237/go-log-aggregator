package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with no environment overrides: %v", err)
	}

	if cfg.Node.Name == "" {
		t.Error("node name defaulted to empty")
	}
	if got, want := cfg.HTTP.Addr, ":8080"; got != want {
		t.Errorf("HTTP.Addr = %q, want %q", got, want)
	}
	if got, want := cfg.Admin.Addr, ":9090"; got != want {
		t.Errorf("Admin.Addr = %q, want %q", got, want)
	}
	if got, want := cfg.Log.Level, slog.LevelInfo; got != want {
		t.Errorf("Log.Level = %v, want %v", got, want)
	}
	if cfg.Cluster.Enabled {
		t.Error("clustering should default to off so a single node needs no configuration")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv(EnvPrefix+"NODE_NAME", "collector-7")
	t.Setenv(EnvPrefix+"HTTP_ADDR", ":18080")
	t.Setenv(EnvPrefix+"INGEST_MAX_RECV_BYTES", "8MB")
	t.Setenv(EnvPrefix+"SHUTDOWN_TIMEOUT", "45s")
	t.Setenv(EnvPrefix+"CLUSTER_ENABLED", "true")
	t.Setenv(EnvPrefix+"CLUSTER_PEERS", "a:7946, b:7946 ,")
	t.Setenv(EnvPrefix+"LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got, want := cfg.Node.Name, "collector-7"; got != want {
		t.Errorf("Node.Name = %q, want %q", got, want)
	}
	if got, want := cfg.HTTP.Addr, ":18080"; got != want {
		t.Errorf("HTTP.Addr = %q, want %q", got, want)
	}
	if got, want := cfg.Ingest.MaxRecvMsgBytes, 8<<20; got != want {
		t.Errorf("Ingest.MaxRecvMsgBytes = %d, want %d", got, want)
	}
	if got, want := cfg.Node.ShutdownTimeout, 45*time.Second; got != want {
		t.Errorf("Node.ShutdownTimeout = %s, want %s", got, want)
	}
	if got, want := len(cfg.Cluster.Peers), 2; got != want {
		t.Errorf("Cluster.Peers = %v, want %d entries (blanks trimmed)", cfg.Cluster.Peers, want)
	}
	if got, want := cfg.Log.Level, slog.LevelDebug; got != want {
		t.Errorf("Log.Level = %v, want %v", got, want)
	}
}

func TestLoadReportsEveryParseFailure(t *testing.T) {
	t.Setenv(EnvPrefix+"HTTP_READ_TIMEOUT", "soon")
	t.Setenv(EnvPrefix+"DB_MAX_CONNS", "lots")
	t.Setenv(EnvPrefix+"ADMIN_ENABLE_PPROF", "maybe")

	_, err := Load()
	if err == nil {
		t.Fatal("Load succeeded with three malformed values")
	}
	for _, key := range []string{"HTTP_READ_TIMEOUT", "DB_MAX_CONNS", "ADMIN_ENABLE_PPROF"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s; a single restart should surface every problem:\n%v", key, err)
		}
	}
}

func TestLoadEmptyValueFallsBackToDefault(t *testing.T) {
	// Compose interpolation of an unset variable yields an empty string. Treating
	// that as "unset" avoids a container that fails to boot over a blank line.
	t.Setenv(EnvPrefix+"HTTP_ADDR", "   ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.HTTP.Addr, ":8080"; got != want {
		t.Errorf("HTTP.Addr = %q, want default %q", got, want)
	}
}

func TestValidate(t *testing.T) {
	base := func() *Config {
		t.Helper()
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		return cfg
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "admin shares the public port",
			mutate: func(c *Config) { c.Admin.Addr = c.HTTP.Addr },
			want:   "must not be publicly reachable",
		},
		{
			name:   "tls cert without key",
			mutate: func(c *Config) { c.Ingest.TLSCertFile = "server.pem" },
			want:   "needs both cert and key",
		},
		{
			name:   "client CA without server tls",
			mutate: func(c *Config) { c.Ingest.TLSClientCAFile = "ca.pem" },
			want:   "server TLS is not enabled",
		},
		{
			name:   "min conns above max",
			mutate: func(c *Config) { c.DB.MinConns, c.DB.MaxConns = 10, 4 },
			want:   "exceeds max conns",
		},
		{
			name:   "zero virtual nodes with clustering on",
			mutate: func(c *Config) { c.Cluster.Enabled, c.Cluster.VirtualNodes = true, 0 },
			want:   "virtual nodes must be at least 1",
		},
		{
			name:   "unknown log format",
			mutate: func(c *Config) { c.Log.Format = "xml" },
			want:   "log format must be json or text",
		},
		{
			name:   "zero shutdown timeout",
			mutate: func(c *Config) { c.Node.ShutdownTimeout = 0 },
			want:   "shutdown timeout must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			tt.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Validate error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestValidateAcceptsDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("defaults must be valid, got: %v", err)
	}
}

func TestBytesSuffixes(t *testing.T) {
	tests := map[string]int{
		"1024":  1024,
		"4KB":   4 << 10,
		"8mb":   8 << 20,
		"1GB":   1 << 30,
		" 2MB ": 2 << 20,
	}
	for raw, want := range tests {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(EnvPrefix+"INGEST_MAX_RECV_BYTES", raw)
			e := &env{}
			got := e.bytes("INGEST_MAX_RECV_BYTES", -1)
			if err := e.err(); err != nil {
				t.Fatalf("bytes(%q): %v", raw, err)
			}
			if got != want {
				t.Errorf("bytes(%q) = %d, want %d", raw, got, want)
			}
		})
	}
}
