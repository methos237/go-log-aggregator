//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/httpapi"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/tail"
)

// tailServer is the public API of a collector, mounted on an httptest server so
// the WebSocket handshake is real and the tail registry reads the real broker.
type tailServer struct {
	url     string
	metrics *tail.Metrics
	api     *httpapi.Server
}

func startTail(t *testing.T, c *collector, buffer int, writeTimeout time.Duration) *tailServer {
	t.Helper()
	m := tail.NewMetrics(nil)
	reg := tail.New(c.queue, c.opt.tailPrefix, buffer, m, testLogger(t))
	api := httpapi.New(&config.HTTP{
		Addr: ":0", AuthToken: "secret", QueryTimeout: time.Second, QueryMaxRows: 50,
		WriteTimeout: writeTimeout, TailPingInterval: 10 * time.Second,
	}, observability.NewHealth(time.Second), httpapi.Deps{Node: "solo", Tails: reg}, testLogger(t))
	ts := httptest.NewServer(api.Handler())
	t.Cleanup(ts.Close)
	return &tailServer{url: ts.URL, metrics: m, api: api}
}

func (s *tailServer) dial(t *testing.T, query string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer secret")
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.url, "http")+"/v1/tail?query="+query, &websocket.DialOptions{HTTPHeader: hdr})
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err, "dial tail")
	c.SetReadLimit(-1)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

// The headline of the phase: a record handed to the gRPC front door reaches a
// tail client through the real broker in well under a second.
func TestTailStreamsWithSubSecondLatency(t *testing.T) {
	t.Parallel()
	_, opt := migratedDBWithStream(t)
	c := startCollector(t, opt, nil)
	defer c.stop(t)
	ts := startTail(t, c, 64, 5*time.Second)

	const service = "tail-latency"
	ws := ts.dial(t, fmt.Sprintf(`{service=%q}%%20|=%%20"record%%203"`, service))
	client := dial(t, c.addr)
	ctx := testContext(t)

	sent := time.Now()
	ack, err := client.SendBatch(ctx, batchOf("b1", service, 0, 5))
	require.NoError(t, err)
	require.Equal(t, logaggv1.AckCode_ACK_CODE_ACCEPTED, ack.GetCode(), ack.GetDetail())

	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, data, err := ws.Read(rctx)
	require.NoError(t, err, "no tail message arrived")
	latency := time.Since(sent)

	var got struct {
		Message string            `json:"message"`
		Labels  map[string]string `json:"labels"`
		Dropped int64             `json:"dropped"`
	}
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, service+" record 3", got.Message, "the line filter picked the wrong record")
	require.Equal(t, service, got.Labels["service"])
	require.Zero(t, got.Dropped)
	require.Less(t, latency, time.Second, "send-to-tail latency")
	t.Logf("send-to-tail latency: %s", latency)
}

// The other exit criterion: a client that stops reading is dropped, its
// misses are counted, and the agents streaming into the same collector never
// notice. Ingest is the thing under protection, so its acks are what is timed.
func TestTailStalledClientDoesNotSlowIngest(t *testing.T) {
	t.Parallel()
	_, opt := migratedDBWithStream(t)
	reg := prometheus.NewRegistry()
	c := startCollector(t, opt, reg)
	defer c.stop(t)
	ts := startTail(t, c, 4, 500*time.Millisecond)

	const (
		service = "tail-stall"
		batches = 200
		perID   = 50
	)
	// Dialed and then never read from.
	ts.dial(t, fmt.Sprintf(`{service=%q}`, service))
	require.Equal(t, 1.0, testutil.ToFloat64(ts.metrics.Subscriptions))

	client := dial(t, c.addr)
	ctx := testContext(t)
	stream, err := client.Stream(ctx)
	require.NoError(t, err)

	// Big messages so the stalled socket fills within the run rather than
	// absorbing the whole test into kernel buffers.
	padding := strings.Repeat("x", 4<<10)
	var slowest time.Duration
	started := time.Now()
	for i := range batches {
		b := batchOf(fmt.Sprintf("b%d", i), service, int64(i*perID), perID)
		for _, r := range b.GetRecords() {
			r.Message += " " + padding
		}
		sent := time.Now()
		require.NoError(t, stream.Send(b))
		ack, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, logaggv1.AckCode_ACK_CODE_ACCEPTED, ack.GetCode(), "batch %d: %s", i, ack.GetDetail())
		slowest = max(slowest, time.Since(sent))
	}
	total := time.Since(started)
	require.NoError(t, stream.CloseSend())
	t.Logf("%d batches acked in %s, slowest %s", batches, total, slowest)

	// A publish that waited on the stalled client would show as the write
	// timeout, 500ms, landing in an ack.
	require.Less(t, slowest, 500*time.Millisecond, "an ack waited on the tail path")

	waitFor(t, 10*time.Second, "the stalled client to be dropped", func() bool {
		return testutil.ToFloat64(ts.metrics.Subscriptions) == 0
	})
	require.Positive(t, testutil.ToFloat64(ts.metrics.Dropped), "no matches were counted as dropped for a client that never read")
	require.Zero(t, testutil.ToFloat64(counterIn(t, reg, "logagg_records_dropped_total")), "ingest dropped records")
}

// counterIn sums a counter family from reg, 0 when it has no series yet.
func counterIn(t *testing.T, reg *prometheus.Registry, name string) prometheus.Counter {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	sum := prometheus.NewCounter(prometheus.CounterOpts{Name: "sum"})
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			sum.Add(m.GetCounter().GetValue())
		}
	}
	return sum
}
