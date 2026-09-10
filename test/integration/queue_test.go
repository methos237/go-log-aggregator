//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jamespolk/go-log-aggregator/internal/queue"
)

// A full stream must classify as ErrOverloaded, which is what turns into
// ACK_CODE_OVERLOADED and tells an agent to slow down.
//
// This is asserted against a real broker because the obvious implementation does not
// work: jetstream.ErrMaxBytesExceeded carries no APIError, so errors.Is can never match
// the *APIError a rejected publish returns, and the classification silently degrades to
// ErrUnavailable — reporting a collector fault for the one condition the backpressure
// design exists to signal. A unit test with a hand-built error would have agreed with
// the broken version.
func TestPublishToAFullStreamClassifiesAsOverloaded(t *testing.T) {
	t.Parallel()

	stream, durable, prefix := uniqueStream(t)
	cfg := queueConfig(&collectorOptions{stream: stream, durable: durable, subjectPrefix: prefix})
	// Small enough that a handful of publishes fill it, with DiscardNew so the broker
	// refuses rather than dropping what is queued.
	cfg.StreamMaxBytes = 8 << 10

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	q, err := queue.Connect(ctx, cfg, nil, testLogger(t))
	require.NoError(t, err, "connect queue")
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		require.NoError(t, q.Close(closeCtx))
	})

	subject := q.Subject("test", "overload")
	payload := make([]byte, 1024)

	var lastErr error
	for i := 0; i < 200 && lastErr == nil; i++ {
		lastErr = q.Publish(ctx, subject, payload)
	}

	require.Error(t, lastErr, "the stream never filled up, so nothing was classified")
	require.ErrorIs(t, lastErr, queue.ErrOverloaded,
		"a full stream must be ErrOverloaded, not %v", lastErr)
	// Overload and unavailability lead to different agent behavior -- slow down versus
	// retry unchanged -- so they must not be conflated.
	require.NotErrorIs(t, lastErr, queue.ErrUnavailable)
}

// A payload the broker will not accept at all is not retryable, so it must classify
// differently from a full stream.
func TestPublishOversizedPayloadClassifiesAsTooLarge(t *testing.T) {
	t.Parallel()

	stream, durable, prefix := uniqueStream(t)
	cfg := queueConfig(&collectorOptions{stream: stream, durable: durable, subjectPrefix: prefix})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	q, err := queue.Connect(ctx, cfg, nil, testLogger(t))
	require.NoError(t, err, "connect queue")
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		require.NoError(t, q.Close(closeCtx))
	})

	// Past whatever this broker advertised, which is also the limit cmd/collector
	// checks the ingest message ceiling against.
	require.Positive(t, q.MaxPayload(), "broker did not advertise a max payload")
	oversized := make([]byte, q.MaxPayload()+1)

	err = q.Publish(ctx, q.Subject("test", "oversized"), oversized)
	require.Error(t, err, "the broker accepted a payload above its own limit")
	require.ErrorIs(t, err, queue.ErrTooLarge, "got %v", err)
}

// The ingest message ceiling must not exceed the broker's, or a valid batch is
// accepted, validated, re-marshaled and then permanently dropped as unsendable. The
// default configuration has to satisfy that against a stock broker.
func TestDefaultIngestCeilingFitsTheBroker(t *testing.T) {
	t.Parallel()

	stream, durable, prefix := uniqueStream(t)
	cfg := queueConfig(&collectorOptions{stream: stream, durable: durable, subjectPrefix: prefix})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	q, err := queue.Connect(ctx, cfg, nil, testLogger(t))
	require.NoError(t, err, "connect queue")
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		require.NoError(t, q.Close(closeCtx))
	})

	defaults, err := defaultConfig()
	require.NoError(t, err)
	require.LessOrEqual(t, int64(defaults.Ingest.MaxRecvMsgBytes), q.MaxPayload(),
		"the default ingest ceiling is above this broker's max_payload, so an oversized batch would be dropped rather than refused")
	require.False(t, errors.Is(err, context.Canceled))
}
