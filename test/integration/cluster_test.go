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
