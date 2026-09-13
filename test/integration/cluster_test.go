//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jamespolk/go-log-aggregator/internal/cluster"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
)

// TestPeerExecute runs a planned statement through the peer service over a
// real gRPC connection and a real database, and checks the rows match a local
// execution byte for byte. It also proves the peer refuses a statement the
// planner could not have written.
func TestPeerExecute(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedStream(ctx, t, pool, 1, "api", "web-1", "prod")
	seedStream(ctx, t, pool, 2, "web", "web-1", "prod")
	insertRows(ctx, t, pool, 1, start, 3)
	insertRows(ctx, t, pool, 2, start, 3)

	cfg := &config.Cluster{PeerAddr: "127.0.0.1:0"}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	srv, err := cluster.NewPeerServer(ctx, cfg, pool, log)
	require.NoError(t, err)
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	peers, err := cluster.NewPeers(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peers.Close() })

	req := query.Request{Start: start, End: start.Add(time.Hour), Limit: 100}
	for _, src := range []string{
		`{env="prod"}`,
		`{env="prod"} |= "row 1"`,
		`{env="prod"} | count_over_time(1m) by (service)`,
	} {
		q, perr := query.Parse(src)
		require.NoError(t, perr)
		plan, cerr := query.Compile(q, req)
		require.NoError(t, cerr)
		plan.Logs.Args[0] = []int64{1, 2}
		shape := executor.ShapeOf(q)

		wantRecords, wantPoints, lerr := executor.ExecLogs(ctx, pool, plan.Logs, shape)
		require.NoError(t, lerr, src)
		gotRecords, gotPoints, rerr := peers.Execute(ctx, srv.Addr(), plan.Logs, shape)
		require.NoError(t, rerr, src)
		require.Equal(t, wantRecords, gotRecords, src)
		require.Equal(t, wantPoints, gotPoints, src)
		require.True(t, len(gotRecords)+len(gotPoints) > 0, "%s returned nothing", src)
	}

	// A statement outside the planner's vocabulary is refused before it
	// reaches the database, with the offending word in the message.
	_, _, err = peers.Execute(ctx, srv.Addr(), query.Stmt{SQL: "SELECT pg_sleep(1)", Args: nil}, executor.Shape{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "pg_sleep")

	// An unreachable peer is an error the coordinator can turn into a warning.
	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, _, err = peers.Execute(short, "127.0.0.1:1", query.Stmt{SQL: "SELECT 1"}, executor.Shape{})
	require.Error(t, err)
}

// TestCoordinatorFanOut runs two collectors in one process over one database:
// two memberlists on loopback and two peer servers. Node a coordinates: its
// answer must equal a single-node answer for every query shape, and once b's
// peer server is gone the answer must degrade to a's own shard with a
// warning rather than fail.
func TestCoordinatorFanOut(t *testing.T) {
	t.Parallel()
	pool, _ := migratedDB(t)
	ctx := testContext(t)
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	// Twelve streams with ids spread over the key space like real hashes are,
	// so both members own some. Sequential small ids would all land in the
	// arc that wraps past the top and belong to one member.
	ids := make([]model.StreamID, 12)
	for i := range ids {
		ids[i] = model.StreamID(int64(i+1) * 0x9E3779B97F4A7C1)
		seedStream(ctx, t, pool, ids[i], "svc", "web-1", "prod")
		insertRows(ctx, t, pool, ids[i], start.Add(time.Duration(i)*time.Second), 3)
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	type node struct {
		cluster *cluster.Cluster
		peer    *cluster.PeerServer
		gossip  string
	}
	startNode := func(name string, seeds ...string) node {
		cfg := &config.Cluster{Enabled: true, BindAddr: "127.0.0.1:0", Peers: seeds, PeerAddr: "127.0.0.1:0", VNodes: 32, JoinTimeout: 5 * time.Second}
		peer, err := cluster.NewPeerServer(ctx, cfg, pool, log)
		require.NoError(t, err)
		go func() { _ = peer.Serve() }()
		t.Cleanup(func() { _ = peer.Shutdown(ctx) })
		cfg.PeerAddr = peer.Addr() // the port memberlist advertises is the one actually bound
		c, err := cluster.New(cfg, name, "127.0.0.1:8080", log, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Leave(time.Second) })
		require.NoError(t, c.Join(ctx))
		c.SetReady(true)
		return node{cluster: c, peer: peer, gossip: c.GossipAddr()}
	}
	a := startNode("a")
	b := startNode("b", a.gossip)
	require.Eventually(t, func() bool {
		ready := 0
		for _, m := range a.cluster.Members() {
			if m.Ready {
				ready++
			}
		}
		return ready == 2
	}, 10*time.Second, 20*time.Millisecond, "a should see both members ready")

	peers, err := cluster.NewPeers(&config.Cluster{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peers.Close() })
	coord := cluster.NewCoordinator(pool, a.cluster, peers, log)
	single := executor.Single{DB: pool}

	// Both members must own streams for the test to prove a fan-out.
	ring := a.cluster.Ring()
	owned := map[string]int{}
	for _, id := range ids {
		owned[ring.Owner(id)]++
	}
	require.Len(t, owned, 2, "ring gave every stream to one member: %v", owned)

	req := query.Request{Start: start, End: start.Add(time.Hour), Limit: 100}
	small := req
	small.Limit = 5
	for _, tc := range []struct {
		src string
		r   query.Request
	}{
		{`{env="prod"}`, req},
		{`{env="prod"}`, small},
		{`{env="prod"} |= "row 1"`, req},
		{`{env="prod"} | count_over_time(1m)`, req},
		{`{env="prod"} | count_over_time(1m) by (level)`, req},
	} {
		q, perr := query.Parse(tc.src)
		require.NoError(t, perr)
		want, serr := single.Run(ctx, q, tc.r)
		require.NoError(t, serr)
		got, cerr := coord.Run(ctx, q, tc.r)
		require.NoError(t, cerr, tc.src)
		require.Empty(t, got.Warnings, tc.src)
		require.Equal(t, want.Records, got.Records, tc.src)
		require.Equal(t, want.Points, got.Points, tc.src)
		require.Equal(t, want.Truncated, got.Truncated, tc.src)
		require.Equal(t, want.Streams, got.Streams, tc.src)
	}

	// Take b's peer server away while gossip still says b is ready: the
	// shard fails, the query does not.
	require.NoError(t, b.peer.Shutdown(context.Background()))
	q, err := query.Parse(`{env="prod"}`)
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	got, err := coord.Run(short, q, req)
	require.NoError(t, err)
	require.Len(t, got.Warnings, 1)
	require.Contains(t, got.Warnings[0], "b unreachable")
	require.Equal(t, 12, got.Streams)
	require.Len(t, got.Records, 3*owned["a"], "only a's shard should be answered")
}
