package httpapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/query/executor"
)

// Request-side caps. The body cap bounds the JSON decode; the query cap bounds
// lexer and parser work per request, generously above any query a person
// types but far below what a hostile client could otherwise send.
const (
	maxBodyBytes    = 64 << 10
	maxQueryLen     = 8 << 10
	defaultLimit    = 1000
	defaultRange    = time.Hour
	defaultQueryDir = "backward"
)

type queryAPI struct {
	db      executor.Querier
	run     executor.Runner
	timeout time.Duration
	maxRows int
	log     *slog.Logger
	// node is this process's name, reported by /v1/cluster when clustering is
	// off; cluster is nil in that case.
	node    string
	cluster ClusterView
}

// queryRequest is the body of POST /v1/query. Times are RFC 3339; end
// defaults to now, start to an hour before end, and limit to defaultLimit.
type queryRequest struct {
	Query     string    `json:"query"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Limit     int       `json:"limit"`
	Direction string    `json:"direction"`
}

// queryResponse echoes the range actually covered, since an aggregation's is
// widened to whole buckets, and says when the row cap cut the result short.
type queryResponse struct {
	Records   []record         `json:"records,omitempty"`
	Points    []executor.Point `json:"points,omitempty"`
	Source    string           `json:"source"`
	Start     time.Time        `json:"start"`
	End       time.Time        `json:"end"`
	Streams   int              `json:"streams"`
	Truncated bool             `json:"truncated"`
	// Warnings name shards a clustered query could not search; the rows are
	// what the reachable members returned.
	Warnings  []string `json:"warnings,omitempty"`
	ElapsedMS float64  `json:"elapsed_ms"`
}

// record is the wire form of a model.LogRecord: ids as hex rather than the
// base64 encoding/json would pick for []byte, level by name via its
// MarshalText.
type record struct {
	Time     time.Time         `json:"time"`
	StreamID model.StreamID    `json:"stream_id"`
	Seq      int64             `json:"seq"`
	Level    model.Level       `json:"level"`
	Message  string            `json:"message"`
	TraceID  string            `json:"trace_id,omitempty"`
	SpanID   string            `json:"span_id,omitempty"`
	Fields   map[string]string `json:"fields,omitempty"`
}

func (a *queryAPI) query(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if len(req.Query) > maxQueryLen {
		writeError(w, http.StatusBadRequest, "query too long")
		return
	}
	q, err := query.Parse(req.Query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	qr, err := a.request(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Compile here first so a planner rejection is a 400. It is pure and cheap,
	// and it leaves every error out of executor.Run a database-side one.
	if _, err = query.Compile(q, qr); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()
	res, err := a.run.Run(ctx, q, qr)
	if err != nil {
		a.fail(w, r, err, "query failed", slog.String("query", req.Query))
		return
	}

	resp := queryResponse{
		Source: res.Source, Start: res.Start, End: res.End, Streams: res.Streams, Truncated: res.Truncated,
		Warnings: res.Warnings, ElapsedMS: float64(res.Elapsed) / float64(time.Millisecond), Points: res.Points,
	}
	if q.Agg == nil {
		resp.Records = make([]record, len(res.Records))
		for i, rec := range res.Records {
			resp.Records[i] = record{
				Time: rec.Time, StreamID: rec.StreamID, Seq: rec.Seq, Level: rec.Level, Message: rec.Message,
				TraceID: hex.EncodeToString(rec.TraceID), SpanID: hex.EncodeToString(rec.SpanID), Fields: rec.Fields,
			}
		}
	}
	writeJSON(w, resp)
}

// fail answers a database-side error. The error itself decides: pgx wraps the
// context's error, so a deadline is a 504 whether it fired before the
// statement or during it. Anything else is logged in full and reported
// generically, since a database error message can carry schema and statement
// text. A client that has gone away gets nothing.
func (a *queryAPI) fail(w http.ResponseWriter, r *http.Request, err error, msg string, attrs ...slog.Attr) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "query exceeded "+a.timeout.String())
	case r.Context().Err() != nil:
	default:
		a.log.LogAttrs(r.Context(), slog.LevelError, msg, append(attrs, slog.Any("error", err))...)
		writeError(w, http.StatusInternalServerError, msg)
	}
}

// request applies the defaults and caps that turn a body into a
// query.Request; Compile validates the rest.
func (a *queryAPI) request(req *queryRequest) (query.Request, error) {
	qr := query.Request{Start: req.Start, End: req.End, Limit: req.Limit}
	if qr.End.IsZero() {
		qr.End = time.Now()
	}
	if qr.Start.IsZero() {
		qr.Start = qr.End.Add(-defaultRange)
	}
	if qr.Limit <= 0 {
		qr.Limit = defaultLimit
	}
	qr.Limit = min(qr.Limit, a.maxRows)
	switch req.Direction {
	case "", defaultQueryDir:
	case "forward":
		qr.Direction = query.Forward
	default:
		return qr, errors.New(`direction must be "backward" or "forward"`)
	}
	return qr, nil
}

func (a *queryAPI) labels(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(ctx context.Context) ([]string, error) { return executor.Labels(ctx, a.db) })
}

func (a *queryAPI) labelValues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	a.list(w, r, func(ctx context.Context) ([]string, error) { return executor.LabelValues(ctx, a.db, name) })
}

func (a *queryAPI) list(w http.ResponseWriter, r *http.Request, fetch func(context.Context) ([]string, error)) {
	ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
	defer cancel()
	values, err := fetch(ctx)
	if err != nil {
		a.fail(w, r, err, "lookup failed", slog.String("path", r.URL.Path))
		return
	}
	if values == nil {
		values = []string{}
	}
	writeJSON(w, struct {
		Values []string `json:"values"`
	}{values})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}
