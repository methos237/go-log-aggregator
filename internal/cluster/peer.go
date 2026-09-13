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

// The peer service is how a coordinator has another collector execute one
// shard of a planned query. Two rules keep it from being a remote SQL
// console: the statement must pass query.CheckSQL, so only text the planner
// could have written runs, and the listener is mTLS or loopback unless an
// operator opts into plaintext.

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
		grpc: grpc.NewServer(creds),
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
		s.log.Warn("peer service listening in plaintext: anything reaching it can run planned queries", slog.String("addr", s.Addr()))
	}
	if err := s.grpc.Serve(s.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// Shutdown stops accepting calls and waits for in-flight ones until ctx ends.
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
		return ctx.Err()
	}
}

// Execute runs one shipped logs statement and returns its rows.
func (s *PeerServer) Execute(ctx context.Context, req *logaggv1.ExecuteRequest) (*logaggv1.ExecuteResponse, error) {
	if req.GetLogs() == nil {
		return nil, status.Error(codes.InvalidArgument, "statement is required")
	}
	if err := query.CheckSQL(req.GetLogs().GetSql()); err != nil {
		s.log.Warn("peer refused a statement", slog.Any("error", err))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	stmt, err := stmtFromProto(req.GetLogs())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	records, points, err := executor.ExecLogs(ctx, s.db, stmt, executor.Shape{Aggregate: req.GetAggregate(), By: req.GetBy()})
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		// The coordinator gets a generic code; the detail stays in this
		// node's log, where a database error message belongs.
		s.log.Error("peer execution failed", slog.Any("error", err))
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
	creds credentials.TransportCredentials
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPeers builds the client pool. The same key pair that serves the peer
// port is presented to other peers, so one CA signs every collector.
func NewPeers(cfg *config.Cluster) (*Peers, error) {
	creds := insecure.NewCredentials()
	if cfg.PeerTLSCertFile != "" {
		var err error
		if creds, err = tlsx.Client(cfg.PeerTLSCertFile, cfg.PeerTLSKeyFile, cfg.PeerTLSCAFile); err != nil {
			return nil, fmt.Errorf("cluster: peer client tls: %w", err)
		}
	}
	return &Peers{creds: creds, conns: make(map[string]*grpc.ClientConn)}, nil
}

// Execute runs logs on the peer at addr and returns its rows.
func (p *Peers) Execute(ctx context.Context, addr string, logs query.Stmt, shape executor.Shape) ([]model.LogRecord, []executor.Point, error) {
	conn, err := p.conn(addr)
	if err != nil {
		return nil, nil, err
	}
	stmt, err := stmtToProto(logs)
	if err != nil {
		return nil, nil, err
	}
	resp, err := logaggv1.NewPeerServiceClient(conn).Execute(ctx, &logaggv1.ExecuteRequest{Logs: stmt, Aggregate: shape.Aggregate, By: shape.By})
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
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(p.creds))
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

// stmtToProto encodes a planned statement. The type switch is the closed set
// of argument types the planner mints; a new one is a wire change, so it
// fails loudly here rather than binding as the wrong type on the peer.
func stmtToProto(s query.Stmt) (*logaggv1.Statement, error) {
	out := &logaggv1.Statement{Sql: s.SQL, Args: make([]*logaggv1.Arg, 0, len(s.Args))}
	for i, a := range s.Args {
		arg := &logaggv1.Arg{}
		switch v := a.(type) {
		case string:
			arg.Value = &logaggv1.Arg_Text{Text: v}
		case int:
			arg.Value = &logaggv1.Arg_Integer{Integer: int64(v)}
		case int16:
			arg.Value = &logaggv1.Arg_Level{Level: int32(v)}
		case float64:
			arg.Value = &logaggv1.Arg_Number{Number: v}
		case time.Time:
			arg.Value = &logaggv1.Arg_TimeUnixNano{TimeUnixNano: v.UnixNano()}
		case time.Duration:
			arg.Value = &logaggv1.Arg_DurationNanos{DurationNanos: int64(v)}
		case []int64:
			arg.Value = &logaggv1.Arg_StreamIds{StreamIds: &logaggv1.StreamIDs{Ids: v}}
		default:
			return nil, fmt.Errorf("cluster: argument %d has type %T, which the peer wire format does not carry", i, a)
		}
		out.Args = append(out.Args, arg)
	}
	return out, nil
}

func stmtFromProto(s *logaggv1.Statement) (query.Stmt, error) {
	out := query.Stmt{SQL: s.GetSql(), Args: make([]any, 0, len(s.GetArgs()))}
	for i, a := range s.GetArgs() {
		switch v := a.GetValue().(type) {
		case *logaggv1.Arg_Text:
			out.Args = append(out.Args, v.Text)
		case *logaggv1.Arg_Integer:
			out.Args = append(out.Args, int(v.Integer))
		case *logaggv1.Arg_Level:
			out.Args = append(out.Args, int16(v.Level)) //nolint:gosec // a level is a smallint by construction
		case *logaggv1.Arg_Number:
			out.Args = append(out.Args, v.Number)
		case *logaggv1.Arg_TimeUnixNano:
			out.Args = append(out.Args, time.Unix(0, v.TimeUnixNano).UTC())
		case *logaggv1.Arg_DurationNanos:
			out.Args = append(out.Args, time.Duration(v.DurationNanos))
		case *logaggv1.Arg_StreamIds:
			ids := v.StreamIds.GetIds()
			if ids == nil {
				ids = []int64{}
			}
			out.Args = append(out.Args, ids)
		default:
			return query.Stmt{}, fmt.Errorf("argument %d has no value", i)
		}
	}
	return out, nil
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
