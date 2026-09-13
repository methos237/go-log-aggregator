package config

import (
	"log/slog"
	"slices"
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
	// Agent defaults must make a bare `go run ./cmd/agent` reach the compose
	// stack's collector with no configuration beyond a source, and must
	// leave a collector process's own defaults untouched by anything
	// agent-specific.
	if got, want := cfg.Agent.IngestAddr, "127.0.0.1:9095"; got != want {
		t.Errorf("Agent.IngestAddr = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.Env, cfg.Node.Env; got != want {
		t.Errorf("Agent.Env = %q, want it to default to Node.Env %q", got, want)
	}
	if cfg.Agent.Host == "" {
		t.Error("Agent.Host defaulted to empty")
	}
	if got, want := cfg.Agent.CheckpointPath, "/var/lib/logagg/checkpoint.json"; got != want {
		t.Errorf("Agent.CheckpointPath = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.SpoolDir, "/var/lib/logagg/spool"; got != want {
		t.Errorf("Agent.SpoolDir = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.SpoolMaxBytes, int64(256<<20); got != want {
		t.Errorf("Agent.SpoolMaxBytes = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.SpoolSegmentBytes, int64(8<<20); got != want {
		t.Errorf("Agent.SpoolSegmentBytes = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.QueueCapacity, 4096; got != want {
		t.Errorf("Agent.QueueCapacity = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.PollInterval, 250*time.Millisecond; got != want {
		t.Errorf("Agent.PollInterval = %s, want %s", got, want)
	}
	if got, want := cfg.Agent.MultilineTimeout, 5*time.Second; got != want {
		t.Errorf("Agent.MultilineTimeout = %s, want %s", got, want)
	}
	if got, want := cfg.Agent.BatchRecords, 500; got != want {
		t.Errorf("Agent.BatchRecords = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.BatchBytes, 512<<10; got != want {
		t.Errorf("Agent.BatchBytes = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.BatchDelay, time.Second; got != want {
		t.Errorf("Agent.BatchDelay = %s, want %s", got, want)
	}
	if got, want := cfg.Agent.AckWindow, 64; got != want {
		t.Errorf("Agent.AckWindow = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.MinBackoff, 250*time.Millisecond; got != want {
		t.Errorf("Agent.MinBackoff = %s, want %s", got, want)
	}
	if got, want := cfg.Agent.MaxBackoff, 30*time.Second; got != want {
		t.Errorf("Agent.MaxBackoff = %s, want %s", got, want)
	}
	// A collector's own default config has no sources configured at all —
	// see TestAgentValidate's "no sources" case — and that must not stop
	// the collector's Load from succeeding: Agent.Validate is deliberately
	// not part of this Validate chain (see Agent.Validate's doc comment).
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config (no agent sources) must still validate for a collector process: %v", err)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv(EnvPrefix+"NODE_NAME", "collector-7")
	t.Setenv(EnvPrefix+"HTTP_ADDR", ":18080")
	t.Setenv(EnvPrefix+"INGEST_MAX_RECV_BYTES", "8MB")
	t.Setenv(EnvPrefix+"SHUTDOWN_TIMEOUT", "45s")
	t.Setenv(EnvPrefix+"LOG_LEVEL", "debug")
	t.Setenv(EnvPrefix+"AGENT_FILES", "/var/log/app.log, /var/log/other.log ,")
	t.Setenv(EnvPrefix+"AGENT_CONTAINERS", "web, worker")
	t.Setenv(EnvPrefix+"AGENT_STDIN", "true")
	t.Setenv(EnvPrefix+"AGENT_SERVICE", "everything")
	t.Setenv(EnvPrefix+"AGENT_ENV", "staging")
	t.Setenv(EnvPrefix+"AGENT_HOST", "agent-host-7")
	t.Setenv(EnvPrefix+"AGENT_INGEST_ADDR", "collector:9095")
	t.Setenv(EnvPrefix+"AGENT_SPOOL_MAX_BYTES", "64MB")
	t.Setenv(EnvPrefix+"AGENT_QUEUE_CAPACITY", "128")
	t.Setenv(EnvPrefix+"AGENT_MULTILINE_PATTERN", `^\s`)
	t.Setenv(EnvPrefix+"AGENT_EXTRACT_JSON", "true")
	t.Setenv(EnvPrefix+"AGENT_BATCH_RECORDS", "50")
	t.Setenv(EnvPrefix+"AGENT_ACK_WINDOW", "8")

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
	if got, want := cfg.Log.Level, slog.LevelDebug; got != want {
		t.Errorf("Log.Level = %v, want %v", got, want)
	}
	if got, want := cfg.Agent.Files, []string{"/var/log/app.log", "/var/log/other.log"}; !slices.Equal(got, want) {
		t.Errorf("Agent.Files = %v, want %v (blanks trimmed)", got, want)
	}
	if got, want := cfg.Agent.Containers, []string{"web", "worker"}; !slices.Equal(got, want) {
		t.Errorf("Agent.Containers = %v, want %v", got, want)
	}
	if !cfg.Agent.Stdin {
		t.Error("Agent.Stdin = false, want true")
	}
	if got, want := cfg.Agent.Service, "everything"; got != want {
		t.Errorf("Agent.Service = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.Env, "staging"; got != want {
		t.Errorf("Agent.Env = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.Host, "agent-host-7"; got != want {
		t.Errorf("Agent.Host = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.IngestAddr, "collector:9095"; got != want {
		t.Errorf("Agent.IngestAddr = %q, want %q", got, want)
	}
	if got, want := cfg.Agent.SpoolMaxBytes, int64(64<<20); got != want {
		t.Errorf("Agent.SpoolMaxBytes = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.QueueCapacity, 128; got != want {
		t.Errorf("Agent.QueueCapacity = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.MultilinePattern, `^\s`; got != want {
		t.Errorf("Agent.MultilinePattern = %q, want %q", got, want)
	}
	if !cfg.Agent.ExtractJSON {
		t.Error("Agent.ExtractJSON = false, want true")
	}
	if got, want := cfg.Agent.BatchRecords, 50; got != want {
		t.Errorf("Agent.BatchRecords = %d, want %d", got, want)
	}
	if got, want := cfg.Agent.AckWindow, 8; got != want {
		t.Errorf("Agent.AckWindow = %d, want %d", got, want)
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
			name:   "zero query timeout",
			mutate: func(c *Config) { c.HTTP.QueryTimeout = 0 },
			want:   "query timeout must be positive",
		},
		{
			name:   "query timeout not under write timeout",
			mutate: func(c *Config) { c.HTTP.QueryTimeout = c.HTTP.WriteTimeout },
			want:   "shorter than the write timeout",
		},
		{
			name:   "cluster bind addr without port",
			mutate: func(c *Config) { c.Cluster.Enabled, c.Cluster.BindAddr = true, "10.0.0.1" },
			want:   "cluster bind addr",
		},
		{
			name:   "cluster advertise addr without host",
			mutate: func(c *Config) { c.Cluster.Enabled, c.Cluster.AdvertiseAddr = true, ":7946" },
			want:   "needs a host",
		},
		{
			name: "cluster loopback peer port behind routable gossip",
			mutate: func(c *Config) {
				c.Cluster.Enabled, c.Cluster.BindAddr, c.Cluster.PeerAllowPlaintext = true, ":7946", true
			},
			want: "unreachable port",
		},
		{
			name:   "cluster peer plaintext on routable addr",
			mutate: func(c *Config) { c.Cluster.Enabled, c.Cluster.PeerAddr = true, ":9096" },
			want:   "CLUSTER_PEER_ALLOW_PLAINTEXT",
		},
		{
			name:   "cluster peer tls partial",
			mutate: func(c *Config) { c.Cluster.Enabled, c.Cluster.PeerTLSCertFile = true, "x.pem" },
			want:   "all of cert, key and CA",
		},
		{
			name:   "cluster vnodes zero",
			mutate: func(c *Config) { c.Cluster.Enabled, c.Cluster.VNodes = true, 0 },
			want:   "cluster vnodes",
		},
		{
			name:   "zero query row cap",
			mutate: func(c *Config) { c.HTTP.QueryMaxRows = 0 },
			want:   "query max rows must be positive",
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
			name:   "unknown log format",
			mutate: func(c *Config) { c.Log.Format = "xml" },
			want:   "log format must be json or text",
		},
		{
			name:   "zero shutdown timeout",
			mutate: func(c *Config) { c.Node.ShutdownTimeout = 0 },
			want:   "shutdown timeout must be positive",
		},
		{
			name:   "zero tail buffer",
			mutate: func(c *Config) { c.HTTP.TailBuffer = 0 },
			want:   "tail buffer must be positive",
		},
		{
			name:   "zero tail ping interval",
			mutate: func(c *Config) { c.HTTP.TailPingInterval = 0 },
			want:   "tail ping interval must be positive",
		},
		{
			name:   "empty tail subject prefix",
			mutate: func(c *Config) { c.Queue.TailSubjectPrefix = "" },
			want:   "tail subject prefix must not be empty",
		},
		{
			// The durable stream would capture the copy and store every record twice.
			name:   "tail subject prefix under the durable stream",
			mutate: func(c *Config) { c.Queue.TailSubjectPrefix = c.Queue.SubjectPrefix + ".tail" },
			want:   "inside the durable stream",
		},
		{
			name:   "tail subject prefix equal to the durable prefix",
			mutate: func(c *Config) { c.Queue.TailSubjectPrefix = c.Queue.SubjectPrefix },
			want:   "inside the durable stream",
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

// validAgent returns an Agent config with one source (Stdin) and every
// other field at the value Load produces by default, so each test below
// mutates exactly one thing away from "valid" instead of constructing a
// config from scratch.
func validAgent() Agent {
	return Agent{
		Stdin:             true,
		IngestAddr:        "127.0.0.1:9095",
		CheckpointPath:    "/var/lib/logagg/checkpoint.json",
		SpoolDir:          "/var/lib/logagg/spool",
		SpoolMaxBytes:     256 << 20,
		SpoolSegmentBytes: 8 << 20,
		QueueCapacity:     4096,
		PollInterval:      250 * time.Millisecond,
		MultilineTimeout:  5 * time.Second,
		BatchRecords:      500,
		BatchBytes:        512 << 10,
		BatchDelay:        time.Second,
		AckWindow:         64,
		MinBackoff:        250 * time.Millisecond,
		MaxBackoff:        30 * time.Second,
	}
}

func TestAgentValidateAcceptsSensibleConfig(t *testing.T) {
	a := validAgent()
	if err := a.Validate(); err != nil {
		t.Fatalf("valid agent config rejected: %v", err)
	}
}

func TestAgentValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Agent)
		want   string
	}{
		{
			name:   "no sources at all",
			mutate: func(a *Agent) { a.Stdin = false },
			want:   "no sources configured",
		},
		{
			name:   "files alone is a source",
			mutate: func(a *Agent) { a.Stdin = false; a.Files = []string{"/var/log/app.log"} },
			want:   "",
		},
		{
			name:   "containers alone is a source",
			mutate: func(a *Agent) { a.Stdin = false; a.Containers = []string{"web"} },
			want:   "",
		},
		{
			name:   "bad multiline pattern",
			mutate: func(a *Agent) { a.MultilinePattern = "(" },
			want:   "multiline pattern",
		},
		{
			name:   "bad extract pattern",
			mutate: func(a *Agent) { a.ExtractPattern = "(" },
			want:   "extract pattern",
		},
		{
			name:   "cert without key or ca",
			mutate: func(a *Agent) { a.CertFile = "client.pem" },
			want:   "needs all three",
		},
		{
			name:   "cert and key without ca",
			mutate: func(a *Agent) { a.CertFile, a.KeyFile = "client.pem", "client.key" },
			want:   "needs all three",
		},
		{
			name: "all three tls files is fine",
			mutate: func(a *Agent) {
				a.CertFile, a.KeyFile, a.CAFile = "client.pem", "client.key", "ca.pem"
			},
			want: "",
		},
		{name: "non-positive spool max bytes", mutate: func(a *Agent) { a.SpoolMaxBytes = 0 }, want: "spool max bytes"},
		{name: "non-positive spool segment bytes", mutate: func(a *Agent) { a.SpoolSegmentBytes = -1 }, want: "spool segment bytes"},
		{name: "non-positive queue capacity", mutate: func(a *Agent) { a.QueueCapacity = 0 }, want: "queue capacity"},
		{name: "non-positive poll interval", mutate: func(a *Agent) { a.PollInterval = 0 }, want: "poll interval"},
		{name: "non-positive multiline timeout", mutate: func(a *Agent) { a.MultilineTimeout = 0 }, want: "multiline timeout"},
		{name: "non-positive batch records", mutate: func(a *Agent) { a.BatchRecords = 0 }, want: "batch records"},
		{name: "non-positive batch bytes", mutate: func(a *Agent) { a.BatchBytes = 0 }, want: "batch bytes"},
		{name: "non-positive batch delay", mutate: func(a *Agent) { a.BatchDelay = 0 }, want: "batch delay"},
		{name: "non-positive ack window", mutate: func(a *Agent) { a.AckWindow = 0 }, want: "ack window"},
		{name: "non-positive min backoff", mutate: func(a *Agent) { a.MinBackoff = 0 }, want: "min backoff"},
		{name: "non-positive max backoff", mutate: func(a *Agent) { a.MaxBackoff = 0 }, want: "max backoff"},
		{
			name:   "max backoff below min backoff",
			mutate: func(a *Agent) { a.MinBackoff, a.MaxBackoff = time.Second, 500*time.Millisecond },
			want:   "below min backoff",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := validAgent()
			tc.mutate(&a)

			err := a.Validate()
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

func TestAgentValidateReportsEveryProblemAtOnce(t *testing.T) {
	a := Agent{}
	err := a.Validate()
	if err == nil {
		t.Fatal("expected an error for a zero-value agent config")
	}
	// Accumulating rather than failing fast: an operator fixing configuration should
	// see every mistake in one restart, not one per restart.
	if got := strings.Count(err.Error(), "\n") + 1; got < 8 {
		t.Fatalf("expected at least 8 problems reported, got %d:\n%v", got, err)
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
