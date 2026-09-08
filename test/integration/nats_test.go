//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// natsImage is pinned by digest to the image deploy/docker-compose.yml runs, for the
// same reason the TimescaleDB image is: a pass should say something about the stack
// that actually ships.
const natsImage = "nats@sha256:b270f5e2428354c0335612694d7dd2fb588148e567a5757fdff325ef9c9332e6"

// natsURL is the broker every test in this package shares, set once by TestMain.
var natsURL string

// streamCounter keeps per-test JetStream stream names unique.
var streamCounter atomic.Int64

// startNATS brings up one JetStream-enabled broker for the whole package.
//
// One container, many streams: a broker starts in well under a second but the image
// pull does not, and each test isolates itself with its own stream and durable
// consumer name instead (see queueConfig).
func startNATS(ctx context.Context) (func(), error) {
	if url := os.Getenv("LOGAGG_TEST_NATS_URL"); url != "" {
		// Same bypass convention as LOGAGG_TEST_DB_DSN: point the suite at the
		// `make dev` broker for a faster inner loop.
		natsURL = url
		return func() {}, nil
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        natsImage,
			ExposedPorts: []string{"4222/tcp"},
			// -js enables JetStream, which is off by default and is the only part of
			// NATS this project uses.
			Cmd:        []string{"-js"},
			WaitingFor: wait.ForLog("Server is ready"),
		},
	})
	stop := func() {
		if container == nil {
			return
		}
		if terr := testcontainers.TerminateContainer(container); terr != nil {
			fmt.Fprintf(os.Stderr, "terminating nats container: %v\n", terr)
		}
	}
	if err != nil {
		stop()
		return stop, fmt.Errorf("start nats container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		stop()
		return stop, fmt.Errorf("read nats host: %w", err)
	}
	port, err := container.MappedPort(ctx, "4222/tcp")
	if err != nil {
		stop()
		return stop, fmt.Errorf("read nats port: %w", err)
	}

	natsURL = fmt.Sprintf("nats://%s:%s", host, port.Port())
	return stop, nil
}

// uniqueStream returns a stream name, a durable consumer name and a subject prefix
// unique to one test.
//
// The subject prefix has to be unique too, not just the stream name: JetStream
// refuses a stream whose subjects overlap another's, so every test sharing the
// production "logs" prefix would collide with each other and with the `make dev`
// broker's own LOGS stream. Isolation by name rather than by broker also matters
// because work-queue retention means a leftover stream would silently steal this
// test's messages, and that failure looks like data loss rather than a test problem.
func uniqueStream(t *testing.T) (stream, durable, subjectPrefix string) {
	t.Helper()

	n := streamCounter.Add(1)
	return fmt.Sprintf("LOGS_%d_%d", os.Getpid(), n),
		fmt.Sprintf("writer_%d_%d", os.Getpid(), n),
		fmt.Sprintf("logs_%d_%d", os.Getpid(), n)
}

// waitFor polls until cond returns true, failing the test when it does not.
//
// Polling rather than a fixed sleep: the whole path is asynchronous, and a sleep long
// enough to be reliable on a loaded CI runner would make every local run slow.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.FailNowf(t, "timed out", "waiting up to %s for %s", timeout, what)
}
