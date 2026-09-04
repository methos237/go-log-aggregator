// Package config loads and validates process configuration from the environment.
//
// Every setting has a default that makes the docker-compose stack work with no
// explicit configuration, so `make dev` needs zero setup. Overrides use the
// LOGAGG_ prefix. Loading collects every problem before returning, so a broken
// deployment reports all of its mistakes in one shot instead of one per restart.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix is prepended to every environment variable name.
const EnvPrefix = "LOGAGG_"

// Config is the fully resolved configuration for a collector process.
type Config struct {
	Node    Node
	HTTP    HTTP
	Admin   Admin
	Ingest  Ingest
	DB      DB
	Writer  Writer
	Queue   Queue
	Cluster Cluster
	Log     Log
}

// Node identifies this process within the cluster.
type Node struct {
	// Name must be unique per process. Defaults to the hostname, which is the
	// container ID under compose and therefore already unique.
	Name string
	// Env labels every record this node ingests (dev, staging, prod).
	Env string
	// ShutdownTimeout bounds graceful shutdown: in-flight batches get this long
	// to drain before the process exits anyway.
	ShutdownTimeout time.Duration
}

// HTTP is the public API listener: query, tail, health.
type HTTP struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// Admin is the internal listener: metrics and pprof. Never expose this port
// outside the cluster network; pprof is an unauthenticated profiling endpoint.
type Admin struct {
	Addr        string
	EnablePprof bool
}

// Ingest is the gRPC listener that agents stream into.
type Ingest struct {
	Addr string
	// MaxRecvMsgBytes caps a single gRPC message. Agents batch, so this bounds
	// batch size on the wire and is a first-line defense against memory abuse.
	MaxRecvMsgBytes int
	// BufferSize is the depth of the bounded channel between the gRPC handler
	// and the queue publisher. Full buffer means shed load, never grow.
	BufferSize int
	// TLS is off by default so `make dev` works without certificates. Enable it
	// (and require client certs) for anything reachable beyond localhost.
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
}

// DB is the TimescaleDB connection.
type DB struct {
	DSN             string
	MaxConns        int
	MinConns        int
	ConnMaxLifetime time.Duration
	ConnectTimeout  time.Duration
	// MigrateOnStart runs pending migrations at boot. Convenient for a
	// single-operator project; a real multi-replica deploy would run migrations
	// as a separate job to avoid concurrent DDL.
	MigrateOnStart bool
}

// Writer is the pool that batches records into TimescaleDB.
//
// These are the throughput knobs. Phase 8 sweeps BatchSize and Workers and
// publishes the numbers, so every one of them is an environment variable rather
// than a constant.
type Writer struct {
	// Workers is the number of concurrent batch-writing goroutines. Each one holds
	// one pooled connection while it writes, so this should stay below DB.MaxConns
	// or workers will simply queue on connection acquisition.
	Workers int
	// BatchSize is the record count that triggers a write. Larger batches amortize
	// the round trip and the staging-table insert; too large and a single failure
	// retries a lot of work.
	BatchSize int
	// FlushInterval writes a partial batch that has been waiting this long, which is
	// what bounds ingest-to-queryable latency on a quiet stream.
	FlushInterval time.Duration
	// QueueDepth is the capacity of the bounded channel feeding the workers, in
	// shipments. Full means Submit blocks, which is the intended backpressure:
	// JetStream is the buffer, not process memory.
	QueueDepth int
	// WriteTimeout bounds a single write attempt.
	WriteTimeout time.Duration
	// MaxAttempts includes the first try. Retries are safe because the insert is
	// idempotent through the dedup index.
	MaxAttempts int
	// RetryBaseDelay and RetryMaxDelay bound exponential backoff with full jitter.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// StreamCacheSize caps the in-memory set of streams known to exist in the
	// dimension table. Bounded because label cardinality is attacker- and
	// misconfiguration-controlled.
	StreamCacheSize int
	// StreamRefreshInterval is how often a stream's last_seen is rewritten. Trades
	// freshness of /v1/labels against write volume.
	StreamRefreshInterval time.Duration
}

// Queue is the NATS JetStream connection.
type Queue struct {
	URL            string
	StreamName     string
	SubjectPrefix  string
	ConnectTimeout time.Duration
	// MaxAckPending bounds unacknowledged messages per consumer, which is how
	// storage-side slowness propagates back into ingest as backpressure.
	MaxAckPending int
}

// Cluster is the gossip membership and hash ring.
type Cluster struct {
	// Enabled off means single-node mode: no gossip, ring of one.
	Enabled bool
	// BindAddr is the memberlist gossip listener (TCP and UDP on the same port).
	BindAddr string
	// AdvertiseAddr is what peers are told to reach this node on. Required when
	// the bind address is not routable from peers.
	AdvertiseAddr string
	// Peers is the seed list for joining. Under compose, the service name
	// resolves to every replica, so one entry is enough.
	Peers []string
	// PeerAddr is the internal gRPC listener used for query fan-out.
	PeerAddr string
	// VirtualNodes per member on the hash ring. Higher means more even key
	// distribution and a larger ring; 128 is the starting point to benchmark.
	VirtualNodes int
}

// Log configures the structured logger.
type Log struct {
	Level     slog.Level
	Format    string // "json" or "text"
	AddSource bool
}

// Load reads configuration from the environment and validates it.
func Load() (*Config, error) {
	e := &env{}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "collector"
	}

	cfg := &Config{
		Node: Node{
			Name:            e.str("NODE_NAME", hostname),
			Env:             e.str("ENV", "dev"),
			ShutdownTimeout: e.dur("SHUTDOWN_TIMEOUT", 20*time.Second),
		},
		HTTP: HTTP{
			Addr:            e.str("HTTP_ADDR", ":8080"),
			ReadTimeout:     e.dur("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:    e.dur("HTTP_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:     e.dur("HTTP_IDLE_TIMEOUT", 120*time.Second),
			ShutdownTimeout: e.dur("HTTP_SHUTDOWN_TIMEOUT", 10*time.Second),
		},
		Admin: Admin{
			Addr:        e.str("ADMIN_ADDR", ":9090"),
			EnablePprof: e.bool("ADMIN_ENABLE_PPROF", true),
		},
		Ingest: Ingest{
			Addr:            e.str("INGEST_ADDR", ":9095"),
			MaxRecvMsgBytes: e.bytes("INGEST_MAX_RECV_BYTES", 4<<20),
			BufferSize:      e.int("INGEST_BUFFER_SIZE", 8192),
			TLSCertFile:     e.str("INGEST_TLS_CERT_FILE", ""),
			TLSKeyFile:      e.str("INGEST_TLS_KEY_FILE", ""),
			TLSClientCAFile: e.str("INGEST_TLS_CLIENT_CA_FILE", ""),
		},
		DB: DB{
			DSN:             e.str("DB_DSN", "postgres://logagg:logagg@timescaledb:5432/logagg?sslmode=disable"),
			MaxConns:        e.int("DB_MAX_CONNS", 16),
			MinConns:        e.int("DB_MIN_CONNS", 2),
			ConnMaxLifetime: e.dur("DB_CONN_MAX_LIFETIME", time.Hour),
			ConnectTimeout:  e.dur("DB_CONNECT_TIMEOUT", 10*time.Second),
			MigrateOnStart:  e.bool("DB_MIGRATE_ON_START", true),
		},
		Writer: Writer{
			Workers:       e.int("WRITER_WORKERS", 4),
			BatchSize:     e.int("WRITER_BATCH_SIZE", 5000),
			FlushInterval: e.dur("WRITER_FLUSH_INTERVAL", 250*time.Millisecond),
			QueueDepth:    e.int("WRITER_QUEUE_DEPTH", 1024),
			WriteTimeout:  e.dur("WRITER_WRITE_TIMEOUT", 30*time.Second),
			MaxAttempts:   e.int("WRITER_MAX_ATTEMPTS", 5),
			// 50ms doubling to a 5s ceiling: five attempts span a few seconds, which
			// covers a leader failover or a brief connection storm without holding a
			// batch long enough for JetStream to redeliver it.
			RetryBaseDelay:        e.dur("WRITER_RETRY_BASE_DELAY", 50*time.Millisecond),
			RetryMaxDelay:         e.dur("WRITER_RETRY_MAX_DELAY", 5*time.Second),
			StreamCacheSize:       e.int("WRITER_STREAM_CACHE_SIZE", 8192),
			StreamRefreshInterval: e.dur("WRITER_STREAM_REFRESH_INTERVAL", 5*time.Minute),
		},
		Queue: Queue{
			URL:            e.str("QUEUE_URL", "nats://nats:4222"),
			StreamName:     e.str("QUEUE_STREAM", "LOGS"),
			SubjectPrefix:  e.str("QUEUE_SUBJECT_PREFIX", "logs"),
			ConnectTimeout: e.dur("QUEUE_CONNECT_TIMEOUT", 10*time.Second),
			MaxAckPending:  e.int("QUEUE_MAX_ACK_PENDING", 4096),
		},
		Cluster: Cluster{
			Enabled:       e.bool("CLUSTER_ENABLED", false),
			BindAddr:      e.str("CLUSTER_BIND_ADDR", ":7946"),
			AdvertiseAddr: e.str("CLUSTER_ADVERTISE_ADDR", ""),
			Peers:         e.list("CLUSTER_PEERS", nil),
			PeerAddr:      e.str("CLUSTER_PEER_ADDR", ":9096"),
			VirtualNodes:  e.int("CLUSTER_VIRTUAL_NODES", 128),
		},
		Log: Log{
			Level:     e.level("LOG_LEVEL", slog.LevelInfo),
			Format:    e.str("LOG_FORMAT", "json"),
			AddSource: e.bool("LOG_ADD_SOURCE", false),
		},
	}

	if err := e.err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate reports every invalid setting at once.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.Node.Name == "" {
		bad("node name must not be empty")
	}
	if c.Node.Env == "" {
		bad("node env must not be empty")
	}
	if c.Node.ShutdownTimeout <= 0 {
		bad("shutdown timeout must be positive, got %s", c.Node.ShutdownTimeout)
	}
	if c.HTTP.Addr == "" {
		bad("http addr must not be empty")
	}
	if c.Admin.Addr == "" {
		bad("admin addr must not be empty")
	}
	if c.Admin.Addr == c.HTTP.Addr {
		bad("admin addr %q must differ from http addr: pprof must not be publicly reachable", c.Admin.Addr)
	}
	if c.Ingest.MaxRecvMsgBytes <= 0 {
		bad("ingest max recv bytes must be positive, got %d", c.Ingest.MaxRecvMsgBytes)
	}
	if c.Ingest.BufferSize <= 0 {
		bad("ingest buffer size must be positive, got %d", c.Ingest.BufferSize)
	}

	// Partial TLS configuration is worse than none: it silently serves plaintext.
	certSet, keySet := c.Ingest.TLSCertFile != "", c.Ingest.TLSKeyFile != ""
	switch {
	case certSet != keySet:
		bad("ingest TLS needs both cert and key files, got cert=%q key=%q", c.Ingest.TLSCertFile, c.Ingest.TLSKeyFile)
	case !certSet && c.Ingest.TLSClientCAFile != "":
		bad("ingest client CA is set but server TLS is not enabled")
	}

	if c.DB.DSN == "" {
		bad("db dsn must not be empty")
	}
	if c.DB.MinConns < 0 {
		bad("db min conns must not be negative, got %d", c.DB.MinConns)
	}
	if c.DB.MaxConns < 1 {
		bad("db max conns must be at least 1, got %d", c.DB.MaxConns)
	}
	if c.DB.MinConns > c.DB.MaxConns {
		bad("db min conns %d exceeds max conns %d", c.DB.MinConns, c.DB.MaxConns)
	}
	if err := c.Writer.Validate(); err != nil {
		errs = append(errs, err)
	}
	// A worker holds a pooled connection for the whole of its write transaction, so
	// more workers than connections just moves the queue from the channel to the
	// pool's acquire path, where it is harder to see.
	if c.Writer.Workers > c.DB.MaxConns {
		bad("writer workers %d exceeds db max conns %d", c.Writer.Workers, c.DB.MaxConns)
	}
	if c.Queue.URL == "" {
		bad("queue url must not be empty")
	}
	if c.Queue.StreamName == "" {
		bad("queue stream name must not be empty")
	}
	if c.Queue.SubjectPrefix == "" {
		bad("queue subject prefix must not be empty")
	}
	if c.Queue.MaxAckPending < 1 {
		bad("queue max ack pending must be at least 1, got %d", c.Queue.MaxAckPending)
	}
	if c.Cluster.Enabled {
		if c.Cluster.BindAddr == "" {
			bad("cluster bind addr must not be empty when clustering is enabled")
		}
		if c.Cluster.PeerAddr == "" {
			bad("cluster peer addr must not be empty when clustering is enabled")
		}
		if c.Cluster.VirtualNodes < 1 {
			bad("cluster virtual nodes must be at least 1, got %d", c.Cluster.VirtualNodes)
		}
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		bad("log format must be json or text, got %q", c.Log.Format)
	}

	return errors.Join(errs...)
}

// Validate reports every invalid writer setting at once.
//
// A method on Writer rather than inline in Config.Validate so storage.NewWriter can
// check a hand-built config in tests without constructing a whole Config.
func (w *Writer) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if w.Workers < 1 {
		bad("writer workers must be at least 1, got %d", w.Workers)
	}
	if w.BatchSize < 1 {
		bad("writer batch size must be at least 1, got %d", w.BatchSize)
	}
	if w.FlushInterval <= 0 {
		bad("writer flush interval must be positive, got %s", w.FlushInterval)
	}
	if w.QueueDepth < 1 {
		bad("writer queue depth must be at least 1, got %d", w.QueueDepth)
	}
	if w.WriteTimeout <= 0 {
		bad("writer write timeout must be positive, got %s", w.WriteTimeout)
	}
	if w.MaxAttempts < 1 {
		bad("writer max attempts must be at least 1, got %d", w.MaxAttempts)
	}
	if w.RetryBaseDelay <= 0 {
		bad("writer retry base delay must be positive, got %s", w.RetryBaseDelay)
	}
	if w.RetryMaxDelay < w.RetryBaseDelay {
		bad("writer retry max delay %s is below base delay %s", w.RetryMaxDelay, w.RetryBaseDelay)
	}
	if w.StreamCacheSize < 1 {
		bad("writer stream cache size must be at least 1, got %d", w.StreamCacheSize)
	}
	if w.StreamRefreshInterval <= 0 {
		bad("writer stream refresh interval must be positive, got %s", w.StreamRefreshInterval)
	}

	return errors.Join(errs...)
}

// env reads prefixed variables, accumulating parse failures.
type env struct {
	errs []error
}

func (e *env) err() error { return errors.Join(e.errs...) }

func (e *env) lookup(key string) (string, bool) {
	v, ok := os.LookupEnv(EnvPrefix + key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func (e *env) fail(key, raw string, err error) {
	e.errs = append(e.errs, fmt.Errorf("%s%s=%q: %w", EnvPrefix, key, raw, err))
}

func (e *env) str(key, def string) string {
	if v, ok := e.lookup(key); ok {
		return v
	}
	return def
}

func (e *env) int(key string, def int) int {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		e.fail(key, raw, errors.New("not an integer"))
		return def
	}
	return v
}

// bytes accepts a plain byte count or a KB/MB/GB suffix, so operators can write
// 8MB instead of counting zeros.
func (e *env) bytes(key string, def int) int {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	mult := 1
	num := raw
	switch {
	case strings.HasSuffix(strings.ToUpper(raw), "GB"):
		mult, num = 1<<30, raw[:len(raw)-2]
	case strings.HasSuffix(strings.ToUpper(raw), "MB"):
		mult, num = 1<<20, raw[:len(raw)-2]
	case strings.HasSuffix(strings.ToUpper(raw), "KB"):
		mult, num = 1<<10, raw[:len(raw)-2]
	}
	v, err := strconv.Atoi(strings.TrimSpace(num))
	if err != nil {
		e.fail(key, raw, errors.New("not a byte size (e.g. 4194304, 4MB)"))
		return def
	}
	return v * mult
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		e.fail(key, raw, errors.New("not a duration (e.g. 500ms, 10s, 2m)"))
		return def
	}
	return v
}

func (e *env) bool(key string, def bool) bool {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		e.fail(key, raw, errors.New("not a boolean (true/false/1/0)"))
		return def
	}
	return v
}

func (e *env) list(key string, def []string) []string {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *env) level(key string, def slog.Level) slog.Level {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(raw)); err != nil {
		e.fail(key, raw, errors.New("not a log level (debug/info/warn/error)"))
		return def
	}
	return lvl
}
