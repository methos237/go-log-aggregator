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
	"math"
	"net"
	"os"
	"regexp"
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
	Tracing Tracing
	Agent   Agent
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
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	// AuthToken is the static bearer token the /v1 endpoints require. Empty
	// means the query API is disabled: every /v1 request is refused, so an
	// unconfigured node fails closed rather than serving logs to anyone.
	AuthToken string
	// QueryTimeout bounds one /v1/query round trip, database time included.
	// It must be shorter than WriteTimeout, or the 504 it produces would be
	// written after net/http has already closed the response.
	QueryTimeout time.Duration
	// QueryMaxRows caps the limit a request may ask for; larger and absent
	// limits are clamped to it.
	QueryMaxRows int
	// TailBuffer is how many matched records one /v1/tail client may have
	// waiting before further matches are dropped for it. It is what keeps a
	// stalled client from holding memory or slowing anyone else.
	TailBuffer int
	// TailPingInterval is how often a tail connection is pinged. Under
	// deploy/nginx.conf's 300s proxy_read_timeout it keeps an idle tail open;
	// a client that does not answer within the interval is disconnected.
	TailPingInterval time.Duration
}

// Admin is the internal listener: metrics and pprof. Never expose this port
// outside the cluster network; pprof is an unauthenticated profiling endpoint.
type Admin struct {
	Addr        string
	EnablePprof bool
	// BlockProfileRate and MutexProfileFraction feed runtime.SetBlockProfileRate
	// and runtime.SetMutexProfileFraction. Both default to off: sampling costs
	// throughput, so the benchmark harness turns them on for a profiled run
	// rather than every deployment paying for a profile nobody reads.
	BlockProfileRate     int
	MutexProfileFraction int
}

// QueuePublishHeaderBytes is the room a queue publish's headers take out of
// the broker's max_payload, which counts headers and payload together. A
// traceparent header block is about 85 bytes; this leaves room for a
// tracestate and the broker's own headers. The ingest ceiling's default and
// the collector's startup check both subtract it.
const QueuePublishHeaderBytes = 512

// Ingest is the gRPC listener that agents stream into.
type Ingest struct {
	Addr string
	// MaxRecvMsgBytes caps a single gRPC message. Agents batch, so this bounds
	// batch size on the wire and is a first-line defense against memory abuse.
	//
	// It must not exceed the broker's max_payload less QueuePublishHeaderBytes,
	// since the broker counts a message's headers against the same limit. A
	// batch above that is accepted here, validated, re-marshaled and then
	// refused by NATS as unsendable — a well-formed batch permanently dropped.
	// cmd/collector checks the two against each other at startup, and the
	// default is the NATS server default (1MB) less the header room, rather
	// than gRPC's 4MB, so a stock stack is coherent.
	MaxRecvMsgBytes int
	// BufferSize is the depth of the bounded channel between the gRPC handler
	// and the queue publisher, in batches. Full buffer means shed load, never grow.
	BufferSize int
	// PublishWorkers is how many batches may be in flight to the queue at once.
	// Publishing is a network round trip, so this is what keeps one slow ack from
	// serializing every agent behind it.
	PublishWorkers int
	// TLS is off by default so `make dev` works without certificates. Enable it
	// (and require client certs) for anything reachable beyond localhost.
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	// AllowPlaintext permits serving ingest without TLS on an address that is not
	// loopback. Without it that combination refuses to start, because it is an
	// unauthenticated write path into the log store reachable by anything that can
	// route to the port. Containers legitimately need it — inside a container the
	// listener must bind every interface for the runtime to forward to it — so it is
	// a setting rather than a prohibition, but it has to be chosen deliberately.
	AllowPlaintext bool
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
	// Durable is the name of the shared pull consumer the writers bind to. Shared
	// rather than per-node: replicas competing on one durable consumer is what
	// distributes the backlog instead of duplicating it.
	Durable string
	// MaxAckPending bounds unacknowledged messages per consumer, which is how
	// storage-side slowness propagates back into ingest as backpressure.
	MaxAckPending int
	// AckWait is how long the broker waits for an ack before redelivering. It must
	// exceed the writer's worst case -- WriteTimeout times MaxAttempts plus backoff
	// -- or a batch that is merely slow gets redelivered while it is still being
	// written, which costs a duplicate the dedup index then has to absorb.
	AckWait time.Duration
	// PublishTimeout bounds one publish, including waiting for the JetStream ack.
	// The ingest handler acks the agent only after that ack arrives, so this is
	// also the ceiling on how long a batch can hold a gRPC handler.
	PublishTimeout time.Duration
	// StreamMaxBytes caps the stream on disk. Reaching it rejects new publishes
	// rather than discarding queued records, which pushes backpressure back to the
	// agent instead of losing data (see queue.streamConfig).
	StreamMaxBytes int64
	// StreamMaxAge is the age at which an unconsumed record is dropped. This is a
	// safety valve for a writer that has been down long enough that catching up is
	// hopeless, not a retention policy: TimescaleDB is the archive.
	StreamMaxAge time.Duration
	// TailSubjectPrefix is the core NATS subject prefix accepted batches are
	// copied to for live tail. It must not sit under SubjectPrefix, or the
	// durable stream would capture the copy and store every record twice.
	TailSubjectPrefix string
}

// Cluster is gossip membership and the consistent hash ring over the
// collectors. Off by default so a single node needs no configuration; when on,
// the node binds a memberlist port, joins the seed peers and serves the peer
// gRPC service that query fan-out uses.
type Cluster struct {
	Enabled bool
	// BindAddr is the gossip listener, host:port. Memberlist speaks UDP and TCP
	// on the same port.
	BindAddr string
	// AdvertiseAddr is what other members are told to dial; empty lets
	// memberlist pick the interface it bound to, or a private address when
	// bound to every interface.
	AdvertiseAddr string
	// Peers are seed addresses to join at startup. A hostname that resolves to
	// several addresses, as a compose service name does, seeds every replica.
	Peers []string
	// PeerAddr is the internal gRPC listener other collectors call for query
	// fan-out. Its port is gossiped in the node metadata.
	PeerAddr string
	// Peer TLS. One key pair serves both ways: it is this node's server
	// certificate for incoming fan-out and its client certificate when calling
	// peers, and the CA is what signs every collector. All three or none, with
	// the same plaintext rule as ingest: without them the peer port may bind
	// only loopback unless PeerAllowPlaintext says otherwise, since a peer
	// executes planned statements for anyone who can reach it.
	PeerTLSCertFile    string
	PeerTLSKeyFile     string
	PeerTLSCAFile      string
	PeerAllowPlaintext bool
	// VNodes is the virtual-node count per member on the ring. Every member
	// must agree, since each computes the ring from names alone.
	VNodes int
	// JoinTimeout bounds how long startup keeps retrying the seeds before the
	// node carries on alone and waits to be found.
	JoinTimeout time.Duration
}

// Log configures the structured logger.
type Log struct {
	Level     slog.Level
	Format    string // "json" or "text"
	AddSource bool
}

// Tracing configures OpenTelemetry trace export. Off by default: a process with
// nothing listening on the endpoint would otherwise log an export failure
// every batch interval.
type Tracing struct {
	Enabled bool
	// Endpoint is the OTLP/gRPC collector address, host:port. Jaeger accepts
	// OTLP natively, so this is what the compose stack's Jaeger listens on.
	Endpoint string
	// Insecure sends spans over plaintext gRPC. Fine inside a compose network,
	// wrong anywhere the endpoint is reached over a real wire.
	Insecure bool
	// SampleRatio is the fraction of new traces recorded, 0 to 1. A child span
	// always follows its parent's decision, so one ratio covers the whole
	// agent-to-write trace.
	SampleRatio float64
}

// Agent configures the log-shipping agent's pipeline: which sources to
// read, how to join and extract structure from their lines, and how to
// reach the collector. It has no default that assumes any particular
// source — the compose stack's own agent service is what supplies FILES,
// CONTAINERS or STDIN — but every other setting defaults to something that
// works once at least one source is.
type Agent struct {
	// Files are file paths to tail, one source each.
	Files []string
	// Containers are Docker container names to follow. Each yields two
	// sources — stdout and stderr — checkpointed and labeled independently;
	// see the agent package's DockerSource.
	Containers []string
	// Stdin tails os.Stdin as an additional source.
	Stdin bool
	// Service, when set, overrides the per-source service label for every
	// configured source. Left empty (the default), each source derives its
	// own service label instead — a file's base name with its extension
	// stripped, or "<container>-<stream>" — so every file and every
	// container stream becomes its own stream. Setting this deliberately
	// collapses every source into one stream, which is occasionally what an
	// operator wants (one label for a fleet of otherwise-identical
	// sidecars) and is why the override exists, but two sources sharing a
	// label set are indistinguishable downstream, so it should be a
	// deliberate choice.
	Service string
	// Env labels every record this agent ships. Defaults to Node.Env, so an
	// agent in the same compose stack as its collector needs no separate
	// setting.
	Env string
	// Host labels every record this agent ships. Defaults to the machine's
	// hostname.
	Host string

	// IngestAddr is the collector's ingest listener.
	IngestAddr string
	// CertFile, KeyFile and CAFile are this agent's client TLS material,
	// all three or none — the same rule ingest.ClientConfig enforces at
	// dial time (see ingest.ErrPartialClientTLS), which Validate defers to
	// rather than restating.
	CertFile string
	KeyFile  string
	CAFile   string

	// CheckpointPath persists acked progress per source across restarts.
	CheckpointPath string
	// SpoolDir, SpoolMaxBytes and SpoolSegmentBytes configure the bounded
	// on-disk buffer used while the collector is unreachable; see the agent
	// package's Spool.
	SpoolDir          string
	SpoolMaxBytes     int64
	SpoolSegmentBytes int64

	// QueueCapacity bounds the lines channel between the sources and the
	// multiline joiner: how many lines may be read ahead of the rest of the
	// pipeline before a source's Run blocks.
	QueueCapacity int
	// PollInterval is how often a tailed file is checked for new data once
	// caught up to EOF.
	PollInterval time.Duration

	// MultilinePattern is a continuation-line regex: a line matching it is
	// folded into the record before it. Empty disables joining, which is
	// the right default for a source that already emits one line per
	// record.
	MultilinePattern string
	// MultilineTimeout flushes a held record when no further continuation
	// line arrives within it.
	MultilineTimeout time.Duration

	// ExtractJSON parses each line as a JSON object into fields.
	ExtractJSON bool
	// ExtractPattern extracts fields from named capture groups. Empty
	// disables regex extraction.
	ExtractPattern string

	// BatchRecords and BatchBytes bound one batch shipped to the collector.
	// BatchDelay bounds how long a partially-filled batch waits before
	// shipping anyway.
	BatchRecords int
	BatchBytes   int
	BatchDelay   time.Duration
	// AckWindow bounds how many batches may be outstanding — sent but not
	// yet acknowledged — before the shipper stops sending and spools
	// instead.
	AckWindow int
	// MinBackoff and MaxBackoff bound the full-jitter backoff used both to
	// reopen a dropped ingest stream and to pause after an OVERLOADED ack.
	MinBackoff time.Duration
	MaxBackoff time.Duration
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
			Addr:         e.str("HTTP_ADDR", ":8080"),
			ReadTimeout:  e.dur("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: e.dur("HTTP_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:  e.dur("HTTP_IDLE_TIMEOUT", 120*time.Second),
			AuthToken:    e.str("HTTP_AUTH_TOKEN", ""),
			QueryTimeout: e.dur("HTTP_QUERY_TIMEOUT", 20*time.Second),
			QueryMaxRows: e.int("HTTP_QUERY_MAX_ROWS", 5000),
			// A second or so of a busy service at the default; a client that
			// falls further behind than that is stalled, not slow.
			TailBuffer:       e.int("HTTP_TAIL_BUFFER", 1024),
			TailPingInterval: e.dur("HTTP_TAIL_PING_INTERVAL", 30*time.Second),
		},
		Admin: Admin{
			Addr:                 e.str("ADMIN_ADDR", ":9090"),
			EnablePprof:          e.bool("ADMIN_ENABLE_PPROF", true),
			BlockProfileRate:     e.int("ADMIN_BLOCK_PROFILE_RATE", 0),
			MutexProfileFraction: e.int("ADMIN_MUTEX_PROFILE_FRACTION", 0),
		},
		Ingest: Ingest{
			// Loopback by default, unlike the HTTP and admin listeners. Those serve
			// reads; this one accepts writes with no application-level auth, so the
			// default must not be reachable from off-box. A container overrides it to
			// ":9095" and opts in below, because inside a container the listener has to
			// bind every interface for the runtime to forward to it.
			Addr:            e.str("INGEST_ADDR", "127.0.0.1:9095"),
			MaxRecvMsgBytes: e.bytes("INGEST_MAX_RECV_BYTES", 1<<20-QueuePublishHeaderBytes),
			BufferSize:      e.int("INGEST_BUFFER_SIZE", 8192),
			PublishWorkers:  e.int("INGEST_PUBLISH_WORKERS", 8),
			TLSCertFile:     e.str("INGEST_TLS_CERT_FILE", ""),
			TLSKeyFile:      e.str("INGEST_TLS_KEY_FILE", ""),
			TLSClientCAFile: e.str("INGEST_TLS_CLIENT_CA_FILE", ""),
			AllowPlaintext:  e.bool("INGEST_ALLOW_PLAINTEXT", false),
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
			Durable:        e.str("QUEUE_DURABLE", "writer"),
			MaxAckPending:  e.int("QUEUE_MAX_ACK_PENDING", 4096),
			// Five minutes clears the writer's worst case (30s x 5 attempts plus
			// backoff) with room to spare. Redelivering earlier than that would
			// duplicate work the writer is still doing.
			AckWait: e.dur("QUEUE_ACK_WAIT", 5*time.Minute),
			// Well under the gRPC handler's patience: a publish that has not been
			// acked in five seconds means JetStream is unhealthy, and telling the
			// agent to retry beats holding its stream open.
			PublishTimeout: e.dur("QUEUE_PUBLISH_TIMEOUT", 5*time.Second),
			StreamMaxBytes: e.bytes64("QUEUE_STREAM_MAX_BYTES", 8<<30),
			StreamMaxAge:   e.dur("QUEUE_STREAM_MAX_AGE", 24*time.Hour),
			// Outside the durable stream's logs.> filter on purpose; see the field.
			TailSubjectPrefix: e.str("QUEUE_TAIL_SUBJECT_PREFIX", "tail"),
		},
		Cluster: Cluster{
			Enabled:            e.bool("CLUSTER_ENABLED", false),
			BindAddr:           e.str("CLUSTER_BIND_ADDR", "127.0.0.1:7946"),
			AdvertiseAddr:      e.str("CLUSTER_ADVERTISE_ADDR", ""),
			Peers:              e.list("CLUSTER_PEERS", nil),
			PeerAddr:           e.str("CLUSTER_PEER_ADDR", "127.0.0.1:9096"),
			PeerTLSCertFile:    e.str("CLUSTER_PEER_TLS_CERT_FILE", ""),
			PeerTLSKeyFile:     e.str("CLUSTER_PEER_TLS_KEY_FILE", ""),
			PeerTLSCAFile:      e.str("CLUSTER_PEER_TLS_CA_FILE", ""),
			PeerAllowPlaintext: e.bool("CLUSTER_PEER_ALLOW_PLAINTEXT", false),
			VNodes:             e.int("CLUSTER_VNODES", 128),
			JoinTimeout:        e.dur("CLUSTER_JOIN_TIMEOUT", 30*time.Second),
		},
		Log: Log{
			Level:     e.level("LOG_LEVEL", slog.LevelInfo),
			Format:    e.str("LOG_FORMAT", "json"),
			AddSource: e.bool("LOG_ADD_SOURCE", false),
		},
		Tracing: Tracing{
			Enabled:     e.bool("TRACING_ENABLED", false),
			Endpoint:    e.str("TRACING_ENDPOINT", "localhost:4317"),
			Insecure:    e.bool("TRACING_INSECURE", true),
			SampleRatio: e.float("TRACING_SAMPLE_RATIO", 1.0),
		},
	}

	// Agent.Env and Agent.Host default from values Load has already resolved
	// above, not from a literal, so an agent sharing a compose stack with its
	// collector needs no separate setting for either.
	cfg.Agent = Agent{
		Files:      e.list("AGENT_FILES", nil),
		Containers: e.list("AGENT_CONTAINERS", nil),
		Stdin:      e.bool("AGENT_STDIN", false),
		Service:    e.str("AGENT_SERVICE", ""),
		Env:        e.str("AGENT_ENV", cfg.Node.Env),
		Host:       e.str("AGENT_HOST", hostname),

		// Loopback, matching Ingest.Addr's own default: this is where a bare
		// `go run ./cmd/agent` on the same host as `make dev` finds its
		// collector. A container overrides it to the collector's compose
		// service name.
		IngestAddr: e.str("AGENT_INGEST_ADDR", "127.0.0.1:9095"),
		CertFile:   e.str("AGENT_CERT_FILE", ""),
		KeyFile:    e.str("AGENT_KEY_FILE", ""),
		CAFile:     e.str("AGENT_CA_FILE", ""),

		CheckpointPath:    e.str("AGENT_CHECKPOINT_PATH", "/var/lib/logagg/checkpoint.json"),
		SpoolDir:          e.str("AGENT_SPOOL_DIR", "/var/lib/logagg/spool"),
		SpoolMaxBytes:     e.bytes64("AGENT_SPOOL_MAX_BYTES", 256<<20),
		SpoolSegmentBytes: e.bytes64("AGENT_SPOOL_SEGMENT_BYTES", 8<<20),

		QueueCapacity: e.int("AGENT_QUEUE_CAPACITY", 4096),
		PollInterval:  e.dur("AGENT_POLL_INTERVAL", 250*time.Millisecond),

		MultilinePattern: e.str("AGENT_MULTILINE_PATTERN", ""),
		MultilineTimeout: e.dur("AGENT_MULTILINE_TIMEOUT", 5*time.Second),

		ExtractJSON:    e.bool("AGENT_EXTRACT_JSON", false),
		ExtractPattern: e.str("AGENT_EXTRACT_PATTERN", ""),

		BatchRecords: e.int("AGENT_BATCH_RECORDS", 500),
		BatchBytes:   e.bytes("AGENT_BATCH_BYTES", 512<<10),
		BatchDelay:   e.dur("AGENT_BATCH_DELAY", time.Second),
		AckWindow:    e.int("AGENT_ACK_WINDOW", 64),
		MinBackoff:   e.dur("AGENT_MIN_BACKOFF", 250*time.Millisecond),
		MaxBackoff:   e.dur("AGENT_MAX_BACKOFF", 30*time.Second),
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
	if c.HTTP.QueryTimeout <= 0 {
		bad("http query timeout must be positive, got %s", c.HTTP.QueryTimeout)
	}
	if c.Cluster.Enabled {
		for name, addr := range map[string]string{"bind": c.Cluster.BindAddr, "peer": c.Cluster.PeerAddr} {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				bad("cluster %s addr %q must be host:port: %v", name, addr, err)
			}
		}
		if c.Cluster.AdvertiseAddr != "" {
			host, _, err := net.SplitHostPort(c.Cluster.AdvertiseAddr)
			switch {
			case err != nil:
				bad("cluster advertise addr %q must be host:port: %v", c.Cluster.AdvertiseAddr, err)
			case host == "":
				// An empty host would be advertised as 0.0.0.0 and every peer
				// would dial itself.
				bad("cluster advertise addr %q needs a host: peers dial what is advertised", c.Cluster.AdvertiseAddr)
			}
		}
		if loopbackAddr(c.Cluster.PeerAddr) && !loopbackAddr(c.Cluster.BindAddr) {
			// Peers dial the gossip address with the peer port, so a peer
			// listener on loopback is unreachable from any other host and every
			// fan-out to this node would degrade into a warning.
			bad("cluster peer addr %s binds loopback while gossip binds %s: peers would dial an unreachable port", c.Cluster.PeerAddr, c.Cluster.BindAddr)
		}
		if c.Cluster.VNodes < 1 {
			bad("cluster vnodes must be positive, got %d", c.Cluster.VNodes)
		}
		set := 0
		for _, f := range []string{c.Cluster.PeerTLSCertFile, c.Cluster.PeerTLSKeyFile, c.Cluster.PeerTLSCAFile} {
			if f != "" {
				set++
			}
		}
		switch {
		case set != 0 && set != 3:
			bad("cluster peer TLS needs all of cert, key and CA, or none")
		case set == 0 && !c.Cluster.PeerAllowPlaintext && !loopbackAddr(c.Cluster.PeerAddr):
			bad("cluster peer listens on %s without TLS: set the %sCLUSTER_PEER_TLS_* files, bind loopback, or set %sCLUSTER_PEER_ALLOW_PLAINTEXT=true to let anyone who reaches the port run planned queries",
				c.Cluster.PeerAddr, EnvPrefix, EnvPrefix)
		}
		if c.Cluster.JoinTimeout <= 0 {
			bad("cluster join timeout must be positive, got %s", c.Cluster.JoinTimeout)
		}
	}
	// Positive rather than net/http's "0 means none": the write timeout also
	// bounds each live-tail write, and a tail with no write bound would keep a
	// stalled client's goroutine forever.
	if c.HTTP.WriteTimeout <= 0 {
		bad("http write timeout must be positive, got %s", c.HTTP.WriteTimeout)
	}
	if c.HTTP.WriteTimeout > 0 && c.HTTP.QueryTimeout >= c.HTTP.WriteTimeout {
		bad("http query timeout %s must be shorter than the write timeout %s, or a timed-out query cannot be answered", c.HTTP.QueryTimeout, c.HTTP.WriteTimeout)
	}
	if c.HTTP.QueryMaxRows <= 0 {
		bad("http query max rows must be positive, got %d", c.HTTP.QueryMaxRows)
	}
	if c.HTTP.TailBuffer <= 0 {
		bad("http tail buffer must be positive, got %d", c.HTTP.TailBuffer)
	}
	if c.HTTP.TailPingInterval <= 0 {
		bad("http tail ping interval must be positive, got %s", c.HTTP.TailPingInterval)
	}
	if c.Admin.Addr == "" {
		bad("admin addr must not be empty")
	}
	if c.Admin.Addr == c.HTTP.Addr {
		bad("admin addr %q must differ from http addr: pprof must not be publicly reachable", c.Admin.Addr)
	}
	if c.Admin.BlockProfileRate < 0 || c.Admin.MutexProfileFraction < 0 {
		bad("admin profile rates must not be negative, got block=%d mutex=%d",
			c.Admin.BlockProfileRate, c.Admin.MutexProfileFraction)
	}
	if c.Ingest.MaxRecvMsgBytes <= 0 {
		bad("ingest max recv bytes must be positive, got %d", c.Ingest.MaxRecvMsgBytes)
	}
	if c.Ingest.BufferSize <= 0 {
		bad("ingest buffer size must be positive, got %d", c.Ingest.BufferSize)
	}
	if c.Ingest.PublishWorkers < 1 {
		bad("ingest publish workers must be at least 1, got %d", c.Ingest.PublishWorkers)
	}

	// Partial TLS configuration is worse than none: it silently serves plaintext, or
	// it serves one-way TLS that looks authenticated and is not.
	certSet, keySet := c.Ingest.TLSCertFile != "", c.Ingest.TLSKeyFile != ""
	switch {
	case certSet != keySet:
		bad("ingest TLS needs both cert and key files, got cert=%q key=%q", c.Ingest.TLSCertFile, c.Ingest.TLSKeyFile)
	case !certSet && c.Ingest.TLSClientCAFile != "":
		bad("ingest client CA is set but server TLS is not enabled")
	case certSet && c.Ingest.TLSClientCAFile == "":
		// mTLS is the baseline for this hop (roadmap §8). Server-only TLS would
		// encrypt the connection while letting anyone who can reach the port write
		// logs into the cluster.
		bad("ingest TLS is enabled without a client CA: mutual authentication is required")
	case !certSet && !c.Ingest.AllowPlaintext && !loopbackAddr(c.Ingest.Addr):
		// Ingest is a write path into the log store with no application-level auth,
		// so plaintext on a routable address means anyone who can reach the port can
		// forge records. Loopback is exempt because that is what `make dev` and the
		// tests use.
		bad("ingest listens on %s without TLS: set the TLS files, bind loopback, or set %sINGEST_ALLOW_PLAINTEXT=true to accept an unauthenticated write path",
			c.Ingest.Addr, EnvPrefix)
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
	if c.Queue.Durable == "" {
		bad("queue durable consumer name must not be empty")
	}
	if c.Queue.AckWait <= 0 {
		bad("queue ack wait must be positive, got %s", c.Queue.AckWait)
	}
	// Redelivering a batch the writer is still retrying wastes a write and leans on
	// the dedup index to clean up after it.
	if worst := c.Writer.WriteTimeout * time.Duration(c.Writer.MaxAttempts); c.Queue.AckWait < worst {
		bad("queue ack wait %s is below the writer's worst case %s (write timeout x max attempts)", c.Queue.AckWait, worst)
	}
	if c.Queue.MaxAckPending < 1 {
		bad("queue max ack pending must be at least 1, got %d", c.Queue.MaxAckPending)
	}
	if c.Queue.PublishTimeout <= 0 {
		bad("queue publish timeout must be positive, got %s", c.Queue.PublishTimeout)
	}
	if c.Queue.StreamMaxBytes < 1 {
		bad("queue stream max bytes must be positive, got %d", c.Queue.StreamMaxBytes)
	}
	if c.Queue.StreamMaxAge <= 0 {
		bad("queue stream max age must be positive, got %s", c.Queue.StreamMaxAge)
	}
	// The stream captures SubjectPrefix.>, so a tail prefix inside it would make
	// every fan-out copy durable: double the disk, and the writer storing each
	// record twice.
	if tail, durable := c.Queue.TailSubjectPrefix, c.Queue.SubjectPrefix; tail == "" {
		bad("queue tail subject prefix must not be empty")
	} else if tail == durable || strings.HasPrefix(tail, durable+".") {
		bad("queue tail subject prefix %q is inside the durable stream's %q.>", tail, durable)
	} else if strings.HasPrefix(durable, tail+".") {
		// The other nesting: a fan-out copy whose env token spells the rest of
		// the durable prefix would land inside the stream just the same.
		bad("queue subject prefix %q is inside the tail prefix's %q.>", durable, tail)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		bad("log format must be json or text, got %q", c.Log.Format)
	}
	if c.Tracing.Enabled && c.Tracing.Endpoint == "" {
		bad("tracing endpoint must not be empty when tracing is enabled")
	}
	if r := c.Tracing.SampleRatio; !(r >= 0 && r <= 1) { // also rejects NaN
		bad("tracing sample ratio must be between 0 and 1, got %v", r)
	}
	// Agent is deliberately not validated here: a collector process never
	// sets any LOGAGG_AGENT_* variable and must stay valid regardless, while
	// an agent process's own "no sources configured" rule would reject that
	// same all-defaults config. cmd/agent calls Agent.Validate directly,
	// the same way storage.NewWriter can call Writer.Validate directly on a
	// hand-built config without going through Config.Validate.

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

// Validate reports every invalid agent setting at once.
//
// A method on Agent rather than inline in Config.Validate, and deliberately
// not called from Config.Validate, for the same reason Writer.Validate is its
// own method: cmd/agent needs to enforce this on its own, without forcing a
// collector process — which never sets any LOGAGG_AGENT_* variable and has
// no sources to configure — to fail its own, otherwise-valid default
// config.
func (a *Agent) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	// An agent with nothing to read is a misconfiguration, not a no-op: it
	// would start, connect to nothing useful, and sit idle forever, which is
	// worth catching at startup rather than discovering as "why is this
	// agent shipping zero records".
	if len(a.Files) == 0 && len(a.Containers) == 0 && !a.Stdin {
		bad("agent has no sources configured: set %sAGENT_FILES, %sAGENT_CONTAINERS, or %sAGENT_STDIN",
			EnvPrefix, EnvPrefix, EnvPrefix)
	}

	if a.MultilinePattern != "" {
		if _, err := regexp.Compile(a.MultilinePattern); err != nil {
			bad("agent multiline pattern %q does not compile: %v", a.MultilinePattern, err)
		}
	}
	if a.ExtractPattern != "" {
		if _, err := regexp.Compile(a.ExtractPattern); err != nil {
			bad("agent extract pattern %q does not compile: %v", a.ExtractPattern, err)
		}
	}

	// Client TLS is all three files or none — the same rule
	// ingest.ClientConfig enforces at dial time (ErrPartialClientTLS) — so
	// this checks exactly that shape rather than restating ingest's own,
	// more elaborate server-side rules for the collector's Ingest section
	// above, which allow a cert+key pair with no client CA.
	certSet, keySet, caSet := a.CertFile != "", a.KeyFile != "", a.CAFile != ""
	if certSet != keySet || keySet != caSet {
		bad("agent client TLS needs all three of cert, key and CA files, or none, got cert=%q key=%q ca=%q",
			a.CertFile, a.KeyFile, a.CAFile)
	}

	if a.SpoolMaxBytes <= 0 {
		bad("agent spool max bytes must be positive, got %d", a.SpoolMaxBytes)
	}
	if a.SpoolSegmentBytes <= 0 {
		bad("agent spool segment bytes must be positive, got %d", a.SpoolSegmentBytes)
	}
	if a.QueueCapacity <= 0 {
		bad("agent queue capacity must be positive, got %d", a.QueueCapacity)
	}
	if a.PollInterval <= 0 {
		bad("agent poll interval must be positive, got %s", a.PollInterval)
	}
	if a.MultilineTimeout <= 0 {
		bad("agent multiline timeout must be positive, got %s", a.MultilineTimeout)
	}
	if a.BatchRecords <= 0 {
		bad("agent batch records must be positive, got %d", a.BatchRecords)
	}
	if a.BatchBytes <= 0 {
		bad("agent batch bytes must be positive, got %d", a.BatchBytes)
	}
	if a.BatchDelay <= 0 {
		bad("agent batch delay must be positive, got %s", a.BatchDelay)
	}
	if a.AckWindow <= 0 {
		bad("agent ack window must be positive, got %d", a.AckWindow)
	}
	if a.MinBackoff <= 0 {
		bad("agent min backoff must be positive, got %s", a.MinBackoff)
	}
	if a.MaxBackoff <= 0 {
		bad("agent max backoff must be positive, got %s", a.MaxBackoff)
	}
	if a.MinBackoff > 0 && a.MaxBackoff > 0 && a.MaxBackoff < a.MinBackoff {
		bad("agent max backoff %s is below min backoff %s", a.MaxBackoff, a.MinBackoff)
	}

	return errors.Join(errs...)
}

// loopbackAddr reports whether addr binds only the loopback interface.
//
// An empty or wildcard host means every interface, which is the case that matters:
// ":9095" and "0.0.0.0:9095" are reachable from off-box, "127.0.0.1:9095" is not.
// A hostname that is not an IP literal is treated as routable, because resolving it
// here would make validation depend on DNS.
func loopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Not host:port at all. Reported by the listener rather than guessed at here.
		return false
	}
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host == "localhost"
	}
	return ip.IsLoopback()
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
	return int(e.bytes64(key, int64(def)))
}

// bytes64 is bytes for the settings that are legitimately larger than a 32-bit
// int, such as an on-disk stream ceiling measured in gigabytes.
func (e *env) bytes64(key string, def int64) int64 {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	var mult int64 = 1
	num := raw
	switch {
	case strings.HasSuffix(strings.ToUpper(raw), "GB"):
		mult, num = 1<<30, raw[:len(raw)-2]
	case strings.HasSuffix(strings.ToUpper(raw), "MB"):
		mult, num = 1<<20, raw[:len(raw)-2]
	case strings.HasSuffix(strings.ToUpper(raw), "KB"):
		mult, num = 1<<10, raw[:len(raw)-2]
	}
	v, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil {
		e.fail(key, raw, errors.New("not a byte size (e.g. 4194304, 4MB)"))
		return def
	}
	// Checked rather than trusted: a suffixed value large enough to wrap would come
	// back negative and be reported as "must be positive", which sends an operator
	// looking in the wrong place.
	if mult > 1 && (v > math.MaxInt64/mult || v < math.MinInt64/mult) {
		e.fail(key, raw, errors.New("byte size overflows a 64-bit integer"))
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

func (e *env) float(key string, def float64) float64 {
	raw, ok := e.lookup(key)
	if !ok {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		e.fail(key, raw, errors.New("not a number (e.g. 0.1)"))
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
