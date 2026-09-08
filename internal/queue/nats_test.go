package queue

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

func testQueueConfig() config.Queue {
	return config.Queue{
		URL:            "nats://127.0.0.1:4222",
		StreamName:     "LOGS",
		SubjectPrefix:  "logs",
		ConnectTimeout: 2 * time.Second,
		MaxAckPending:  128,
		PublishTimeout: time.Second,
		StreamMaxBytes: 1 << 20,
		StreamMaxAge:   time.Hour,
	}
}

// The retention and discard policies are the two settings that decide whether a
// full queue sheds data or refuses new writes, so they are asserted rather than
// left to review.
func TestStreamConfigRefusesNewInsteadOfDiscardingQueued(t *testing.T) {
	t.Parallel()

	got := streamConfig(testQueueConfig())

	if got.Retention != jetstream.WorkQueuePolicy {
		t.Errorf("Retention = %v, want WorkQueuePolicy: acked records must leave the stream", got.Retention)
	}
	if got.Discard != jetstream.DiscardNew {
		t.Errorf("Discard = %v, want DiscardNew: a full stream must reject publishes, not drop queued records", got.Discard)
	}
	if got.Storage != jetstream.FileStorage {
		t.Errorf("Storage = %v, want FileStorage: the backlog must survive a broker restart", got.Storage)
	}
}

func TestStreamConfigCarriesLimitsFromConfig(t *testing.T) {
	t.Parallel()

	cfg := testQueueConfig()
	got := streamConfig(cfg)

	if got.Name != cfg.StreamName {
		t.Errorf("Name = %q, want %q", got.Name, cfg.StreamName)
	}
	if got.MaxBytes != cfg.StreamMaxBytes {
		t.Errorf("MaxBytes = %d, want %d", got.MaxBytes, cfg.StreamMaxBytes)
	}
	if got.MaxAge != cfg.StreamMaxAge {
		t.Errorf("MaxAge = %s, want %s", got.MaxAge, cfg.StreamMaxAge)
	}
	// One wildcard subject: enumerating services would mean a stream update every
	// time a new one appeared.
	if len(got.Subjects) != 1 || got.Subjects[0] != "logs.>" {
		t.Errorf("Subjects = %v, want [logs.>]", got.Subjects)
	}
	// Publish-side dedup cannot cover a redelivery to the writer, so storage owns
	// deduplication and enabling it here would only cost memory on the broker.
	if got.Duplicates != 0 {
		t.Errorf("Duplicates = %s, want 0: dedup belongs to the logs_dedup index", got.Duplicates)
	}
}

// A publish made while disconnected must fail rather than sit in a client-side
// buffer: that buffer is unbounded, and the agent's disk spool is the buffer this
// design intends to use.
func TestNatsOptionsDisablesTheReconnectBuffer(t *testing.T) {
	t.Parallel()

	var opts nats.Options
	for _, apply := range natsOptions(testQueueConfig(), slog.New(slog.DiscardHandler), new(atomic.Bool)) {
		if err := apply(&opts); err != nil {
			t.Fatalf("applying option: %v", err)
		}
	}

	if opts.ReconnectBufSize >= 0 {
		t.Errorf("ReconnectBufSize = %d, want negative (disabled)", opts.ReconnectBufSize)
	}
	if opts.MaxReconnect != -1 {
		t.Errorf("MaxReconnect = %d, want -1 (unlimited)", opts.MaxReconnect)
	}
	if opts.Timeout != testQueueConfig().ConnectTimeout {
		t.Errorf("Timeout = %s, want %s", opts.Timeout, testQueueConfig().ConnectTimeout)
	}
}
