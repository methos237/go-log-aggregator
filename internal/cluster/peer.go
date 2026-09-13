package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
	"github.com/jamespolk/go-log-aggregator/internal/tlsx"
)

// The peer service is how a coordinator has another collector answer one
// shard of a query. The request carries the query text and the request
// parameters, never SQL: the peer compiles them itself, so the only statements
// it executes are ones its own planner wrote, with the time bound and row cap
// the planner always injects. A vocabulary check on shipped SQL would bound
// what a caller could say; compiling here bounds it to what the language can
// mean. The listener is mTLS or loopback unless an operator opts into
// plaintext, and even then the port is a query endpoint, not a SQL one.

// maxRequestBytes caps an ExecuteRequest: the query text is bounded by the
// HTTP layer and the rest is stream ids, so 64 MiB is millions of streams.
const maxRequestBytes = 64 << 20

// PeerServer serves PeerService for the coordinators in the cluster.
type PeerServer struct {
	logaggv1.UnimplementedPeerServiceServer
	db   executor.Querier
	log  *slog.Logger
	grpc *grpc.Server
	lis  net.Listener
	mtls bool
}

// NewPeerServer binds cfg.PeerAddr and prepares to serve. Serve starts it.
func NewPeerServer(ctx context.Context, cfg *config.Cluster, db executor.Querier, log *slog.Logger) (*PeerServer, error) {
	creds, err := peerServerCredentials(cfg)
	if err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.PeerAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: listen on %s: %w", cfg.PeerAddr, err)
	}
	s := &PeerServer{
		db:   db,
		log:  log.With(slog.String("component", "peer")),
		grpc: grpc.NewServer(creds, grpc.MaxRecvMsgSize(maxRequestBytes)),
		lis:  lis,
		mtls: cfg.PeerTLSCertFile != "",
	}
	logaggv1.RegisterPeerServiceServer(s.grpc, s)
	return s, nil
}

// Addr is the address actually bound, for a caller that asked for port 0.
func (s *PeerServer) Addr() string { return s.lis.Addr().String() }

// Serve blocks until Shutdown. A clean stop returns nil.
func (s *PeerServer) Serve() error {
	if s.mtls {
		s.log.Info("peer service listening", slog.String("addr", s.Addr()), slog.String("tls", "mtls"))
	} else {
		s.log.Warn("peer service listening in plaintext: anything reaching it can run queries", slog.String("addr", s.Addr()))
	}
	if err := s.grpc.Serve(s.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// Shutdown stops accepting calls and waits for in-flight ones until ctx ends,
// then cuts them off and waits for the server to finish stopping.
func (s *PeerServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.grpc.Stop()
		<-done
		return ctx.Err()
	}
}

// Execute compiles the shard's query here and runs its logs statement over
// the shipped stream ids.
func (s *PeerServer) Execute(ctx context.Context, req *logaggv1.ExecuteRequest) (*logaggv1.ExecuteResponse, error) {
	q, err := query.Parse(req.GetQuery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	r := query.Request{
		Start: time.Unix(0, req.GetStartUnixNano()).UTC(),
		End:   time.Unix(0, req.GetEndUnixNano()).UTC(),
		Limit: int(req.GetLimit()),
	}
	if req.GetForward() {
		r.Direction = query.Forward
	}
	plan, err := query.Compile(q, r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(req.GetStreamIds()) == 0 {
		return &logaggv1.ExecuteResponse{}, nil
	}
	plan.Logs.Args[0] = req.GetStreamIds()
	records, points, err := executor.ExecLogs(ctx, s.db, plan.Logs, executor.ShapeOf(q))
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		// The coordinator gets a generic code; the detail stays in this
		// node's log, where a database error message belongs.
		s.log.Error("peer execution failed", slog.String("query", req.GetQuery()), slog.Any("error", err))
		return nil, status.Error(codes.Internal, "execution failed")
	}
	return &logaggv1.ExecuteResponse{Records: recordsToProto(records), Points: pointsToProto(points)}, nil
}

func peerServerCredentials(cfg *config.Cluster) (grpc.ServerOption, error) {
	if cfg.PeerTLSCertFile == "" {
		return grpc.EmptyServerOption{}, nil
	}
	creds, err := tlsx.Server(cfg.PeerTLSCertFile, cfg.PeerTLSKeyFile, cfg.PeerTLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("cluster: peer tls: %w", err)
	}
	return grpc.Creds(creds), nil
}

// Peers is the coordinator's side: one lazily dialed connection per peer
// address, reused across queries and closed together.
type Peers struct {
	creds    credentials.TransportCredentials
	maxReply int
	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn
}

// NewPeers builds the client pool. The same key pair that serves the peer
// port is presented to other peers, so one CA signs every collector.
// maxReplyBytes caps a shard's response; size it from the row cap and the
// largest record, since gRPC's default of 4 MiB is a few thousand records.
func NewPeers(cfg *config.Cluster, maxReplyBytes int) (*Peers, error) {
	creds := insecure.NewCredentials()
	if cfg.PeerTLSCertFile != "" {
		var err error
		if creds, err = tlsx.Client(cfg.PeerTLSCertFile, cfg.PeerTLSKeyFile, cfg.PeerTLSCAFile); err != nil {
			return nil, fmt.Errorf("cluster: peer client tls: %w", err)
		}
	}
	if maxReplyBytes <= 0 {
		maxReplyBytes = 4 << 20
	}
	return &Peers{creds: creds, maxReply: maxReplyBytes, conns: make(map[string]*grpc.ClientConn)}, nil
}

// Execute has the peer at addr answer q for r over ids.
func (p *Peers) Execute(ctx context.Context, addr string, q *query.Query, r query.Request, ids []int64) ([]model.LogRecord, []executor.Point, error) {
	conn, err := p.conn(addr)
	if err != nil {
		return nil, nil, err
	}
	req := &logaggv1.ExecuteRequest{
		Query: q.Text, StartUnixNano: r.Start.UnixNano(), EndUnixNano: r.End.UnixNano(),
		Limit: int32(r.Limit), Forward: r.Direction == query.Forward, StreamIds: ids, //nolint:gosec // limit is capped by config
	}
	resp, err := logaggv1.NewPeerServiceClient(conn).Execute(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("peer %s: %w", addr, err)
	}
	return recordsFromProto(resp.GetRecords()), pointsFromProto(resp.GetPoints()), nil
}

func (p *Peers) conn(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(p.creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(p.maxReply)),
		// A member that has just died is still "ready" in gossip until
		// failure detection catches up, a few seconds. gRPC's default
		// minimum connect timeout is 20s, longer than the query budget, so a
		// query touching it would time out whole instead of degrading to a
		// warning. Give up on a connection fast; gossip fixes the routing.
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 3 * time.Second}),
	)
	if err != nil {
		return nil, fmt.Errorf("peer %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

// Close releases every connection.
func (p *Peers) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for addr, c := range p.conns {
		errs = append(errs, c.Close())
		delete(p.conns, addr)
	}
	return errors.Join(errs...)
}

func recordsToProto(records []model.LogRecord) []*logaggv1.Record {
	out := make([]*logaggv1.Record, len(records))
	for i, r := range records {
		out[i] = &logaggv1.Record{
			StreamId: int64(r.StreamID), TimeUnixNano: r.Time.UnixNano(), Seq: r.Seq,
			Level: logaggv1.Level(r.Level), Message: r.Message, TraceId: r.TraceID, SpanId: r.SpanID, Fields: r.Fields,
		}
	}
	return out
}

func recordsFromProto(records []*logaggv1.Record) []model.LogRecord {
	if len(records) == 0 {
		return nil
	}
	out := make([]model.LogRecord, len(records))
	for i, r := range records {
		out[i] = model.LogRecord{
			StreamID: model.StreamID(r.GetStreamId()), Time: time.Unix(0, r.GetTimeUnixNano()).UTC(), Seq: r.GetSeq(),
			Level: model.Level(r.GetLevel()), Message: r.GetMessage(), TraceID: r.GetTraceId(), SpanID: r.GetSpanId(), Fields: r.GetFields(),
		}
	}
	return out
}

func pointsToProto(points []executor.Point) []*logaggv1.Point {
	out := make([]*logaggv1.Point, len(points))
	for i, p := range points {
		out[i] = &logaggv1.Point{BucketUnixNano: p.Bucket.UnixNano(), Value: p.Value, Labels: p.Labels}
	}
	return out
}

func pointsFromProto(points []*logaggv1.Point) []executor.Point {
	if len(points) == 0 {
		return nil
	}
	out := make([]executor.Point, len(points))
	for i, p := range points {
		out[i] = executor.Point{Bucket: time.Unix(0, p.GetBucketUnixNano()).UTC(), Value: p.GetValue(), Labels: p.GetLabels()}
	}
	return out
}
