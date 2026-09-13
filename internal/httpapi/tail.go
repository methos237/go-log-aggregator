package httpapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/query"
	"github.com/jamespolk/go-log-aggregator/internal/tail"
)

// tailAPI serves GET /v1/tail?query=...: a WebSocket that streams each record
// the query matches, as JSON text messages, from the moment of the handshake
// until either side closes.
//
// The server never expects a data message from the client, so the read side is
// handed to CloseRead, which answers pings and close frames and cancels the
// connection's context when the peer goes away. Heartbeats go the other way as
// well: a ping every interval, and a client that does not answer within one
// interval is gone. That is what keeps an idle tail alive through nginx's
// proxy_read_timeout and what notices a vanished laptop without waiting for
// TCP to give up.
type tailAPI struct {
	reg *tail.Registry
	// ping is the heartbeat interval and the pong deadline; write bounds one
	// message. A write that takes longer than that is a client that has
	// stopped reading, not a slow network.
	ping, write time.Duration
	log         *slog.Logger
}

// tailRecord is one message on the wire: the query API's record plus the
// stream's labels, which a tail client has no other way to learn, and how many
// matches were dropped for this client since the previous message. Carried on
// the next record rather than as a message of its own so a client that only
// looks at records loses nothing but the count.
type tailRecord struct {
	record
	Labels  map[string]string `json:"labels"`
	Dropped int64             `json:"dropped,omitempty"`
}

func (a *tailAPI) tail(w http.ResponseWriter, r *http.Request) {
	src := r.URL.Query().Get("query")
	if src == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return
	}
	if len(src) > maxQueryLen {
		writeError(w, http.StatusBadRequest, "query too long")
		return
	}
	q, err := query.Parse(src)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Subscribed before the upgrade, and the subscription is live on the broker
	// before the 101 goes out. That is the guarantee a tail client relies on:
	// anything accepted after the handshake completes is seen. It also makes an
	// unsupported stage a plain 400 the client can read rather than a close
	// frame. The cost is that an authenticated plain GET briefly holds a broker
	// subscription; a probe that sends a bearer token to /v1/tail is the odd one.
	sub, err := a.reg.Subscribe(q)
	if err != nil {
		var qe *query.Error
		switch {
		case errors.As(err, &qe):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, tail.ErrClosed):
			writeError(w, http.StatusServiceUnavailable, "shutting down")
		default:
			a.log.Error("tail subscribe failed", slog.String("query", src), slog.Any("error", err))
			writeError(w, http.StatusInternalServerError, "subscribe failed")
		}
		return
	}
	defer sub.Close()

	// Accept writes its own 4xx on a malformed handshake. Origin is left at the
	// default, same-origin only: the CLI sends none, and a browser page served
	// from elsewhere has no business holding a bearer token for this API.
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	// CloseNow rather than Close on every exit path: by the time the handler
	// returns, either the peer is gone or a close frame has already been sent.
	defer c.CloseNow() //nolint:errcheck // idempotent teardown

	ctx := c.CloseRead(r.Context())
	// Writes and pings are cut short by the subscription ending, so a client
	// stalled mid-write cannot hold shutdown for the whole write timeout. The
	// read context is left alone: canceling it would slam the connection shut
	// before the goodbye below could be sent to a client that is still there.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-sub.Done():
			cancel()
		case <-wctx.Done():
		}
	}()

	heartbeat := time.NewTicker(a.ping)
	defer heartbeat.Stop()
	// Pings run off the loop. Waiting for the pong in the loop would stop
	// draining the client's buffer for one round trip on every tick, and a
	// client on a slow link would see drops it did nothing to earn.
	pongs := make(chan error, 1)
	pinging := false

	var reported int64
	for {
		select {
		case <-ctx.Done():
			// The peer closed, or a write timed out and the library closed for us.
			return
		case <-sub.Done():
			a.close(c, websocket.StatusGoingAway, "server shutting down")
			return
		case <-heartbeat.C:
			if pinging {
				// The previous ping's own deadline decides; a second in flight
				// would only race it.
				continue
			}
			pinging = true
			go func() { pongs <- a.bounded(wctx, c.Ping) }()
		case err := <-pongs:
			pinging = false
			if err != nil {
				a.goodbye(c, sub)
				return
			}
		case rec := <-sub.Records():
			dropped := sub.Dropped()
			if err := a.send(wctx, c, &rec, dropped-reported); err != nil {
				a.goodbye(c, sub)
				return
			}
			reported = dropped
		}
	}
}

// goodbye handles a failed write or ping: when the subscription ended under
// it, the failure was ours and the client still deserves the shutdown close
// frame; otherwise the peer is gone and there is nobody to tell.
func (a *tailAPI) goodbye(c *websocket.Conn, sub *tail.Subscription) {
	select {
	case <-sub.Done():
		a.close(c, websocket.StatusGoingAway, "server shutting down")
	default:
	}
}

// send writes one record. The write is bounded by ctx's deadline: when it
// expires the library closes the connection, which is the right outcome for a
// client that has stopped reading — its buffer in the registry is already
// dropping, and holding the socket open would only hold the goroutine.
func (a *tailAPI) send(ctx context.Context, c *websocket.Conn, rec *tail.Record, dropped int64) error {
	body, err := json.Marshal(tailRecord{
		record: record{
			Time: rec.Time, StreamID: rec.StreamID, Seq: rec.Seq, Level: rec.Level, Message: rec.Message,
			TraceID: hex.EncodeToString(rec.TraceID), SpanID: hex.EncodeToString(rec.SpanID), Fields: rec.Fields,
		},
		Labels:  labelMap(rec.Labels),
		Dropped: dropped,
	})
	if err != nil {
		// A LogRecord always marshals; this is a programming error worth a log line.
		a.log.Error("encoding tail record failed", slog.Any("error", err))
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, a.write)
	defer cancel()
	return c.Write(wctx, websocket.MessageText, body)
}

// bounded runs a ping under the heartbeat deadline.
func (a *tailAPI) bounded(ctx context.Context, ping func(context.Context) error) error {
	pctx, cancel := context.WithTimeout(ctx, a.ping)
	defer cancel()
	return ping(pctx)
}

// close sends a close frame and waits briefly for the peer's. Bounded because
// a peer that never answers must not hold shutdown.
func (a *tailAPI) close(c *websocket.Conn, code websocket.StatusCode, reason string) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Close(code, reason)
	}()
	select {
	case <-done:
	case <-time.After(a.write):
		_ = c.CloseNow()
	}
}

// labelMap flattens a LabelSet for the wire, promoted labels first so the
// object reads the way a selector is written.
func labelMap(ls model.LabelSet) map[string]string {
	m := make(map[string]string, 3+len(ls.Extra))
	for k, v := range ls.Extra {
		m[k] = v
	}
	m["service"], m["host"], m["env"] = ls.Service, ls.Host, ls.Env
	return m
}
