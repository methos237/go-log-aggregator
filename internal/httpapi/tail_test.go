package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	logaggv1 "github.com/jamespolk/go-log-aggregator/api/proto/logagg/v1"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/queue"
	"github.com/jamespolk/go-log-aggregator/internal/queue/queuetest"
	"github.com/jamespolk/go-log-aggregator/internal/tail"
)

const tailPing = 50 * time.Millisecond

func newTailServer(t *testing.T) (*Server, *queuetest.Publisher, *httptest.Server) {
	t.Helper()
	pub := &queuetest.Publisher{}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	srv := New(&config.HTTP{
		Addr: ":0", AuthToken: "secret", QueryTimeout: time.Second, QueryMaxRows: 50,
		WriteTimeout: time.Second, TailPingInterval: tailPing,
	}, observability.NewHealth(time.Second), Deps{Node: "solo", Tails: tail.New(pub, "tail", 8, nil, nil)}, log)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, pub, ts
}

// dial connects to /v1/tail and reports the handshake status. The response
// body is closed here: on a completed upgrade it is a no-op reader, and on a
// refusal it is the error JSON the tests do not need.
func dial(t *testing.T, ts *httptest.Server, q, token string) (*websocket.Conn, int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	if token != "" {
		opts.HTTPHeader.Set("Authorization", "Bearer "+token)
	}
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/tail?query=" + q
	c, resp, err := websocket.Dial(ctx, url, opts)
	if c != nil {
		t.Cleanup(func() { _ = c.CloseNow() })
	}
	status := 0
	if resp != nil {
		status = resp.StatusCode
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}
	return c, status, err
}

func fanout(t *testing.T, pub *queuetest.Publisher, ls model.LabelSet, msgs ...string) {
	t.Helper()
	b := &logaggv1.LogBatch{Labels: ls.Proto()}
	for i, m := range msgs {
		rec := model.LogRecord{Time: time.Unix(1_700_000_000, 0).UTC(), Seq: int64(i), Level: model.LevelWarn, Message: m}
		b.Records = append(b.Records, rec.Proto())
	}
	payload, err := proto.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	_ = pub.Fanout(queue.Subject("tail", ls.Env, ls.Service), payload)
}

// message is what a reading client sees: a decoded record, or the error that
// ended the connection.
type message struct {
	body map[string]any
	err  error
}

// reader drains the connection the way a real client does. It must run for the
// whole test: the library answers the server's pings only while a Read is in
// progress, so a client that stops reading is one the heartbeat rightly
// disconnects.
func reader(t *testing.T, c *websocket.Conn) <-chan message {
	t.Helper()
	ch := make(chan message, 16)
	go func() {
		for {
			typ, data, err := c.Read(context.Background())
			if err != nil {
				ch <- message{err: err}
				return
			}
			if typ != websocket.MessageText {
				ch <- message{err: fmt.Errorf("message type = %v, want text", typ)}
				return
			}
			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil {
				ch <- message{err: fmt.Errorf("non-JSON tail message %q: %w", data, err)}
				return
			}
			ch <- message{body: m}
		}
	}()
	return ch
}

func next(t *testing.T, ch <-chan message) (map[string]any, error) {
	t.Helper()
	select {
	case m := <-ch:
		return m.body, m.err
	case <-time.After(5 * time.Second):
		t.Fatal("no message within 5s")
		return nil, nil
	}
}

func TestTailStreamsMatchingRecords(t *testing.T) {
	t.Parallel()
	_, pub, ts := newTailServer(t)
	c, _, err := dial(t, ts, `{service="api"}%20|=%20"timeout"`, "secret")
	if err != nil {
		t.Fatal(err)
	}
	msgs := reader(t, c)

	api := model.LabelSet{Service: "api", Host: "web-1", Env: "prod", Extra: map[string]string{"region": "eu"}}
	fanout(t, pub, model.LabelSet{Service: "web", Host: "web-1", Env: "prod"}, "timeout")
	fanout(t, pub, api, "ok", "Timeout after 30s")

	got, err := next(t, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if got["message"] != "Timeout after 30s" || got["level"] != "warn" || got["seq"] != float64(1) {
		t.Errorf("record = %v, want the api timeout at seq 1 with level warn", got)
	}
	labels, _ := got["labels"].(map[string]any)
	if labels["service"] != "api" || labels["region"] != "eu" {
		t.Errorf("labels = %v, want service and region", labels)
	}

	// Long enough for several heartbeats: a ping the client did not answer, or
	// one the server mishandled, would have closed the connection by now.
	time.Sleep(4 * tailPing)
	fanout(t, pub, api, "still here after a timeout")
	if got, err = next(t, msgs); err != nil || got["message"] != "still here after a timeout" {
		t.Fatalf("after heartbeats: record = %v, err = %v", got, err)
	}
}

func TestTailHandshakeErrors(t *testing.T) {
	t.Parallel()
	_, _, ts := newTailServer(t)

	tests := []struct {
		name, query, token string
		want               int
	}{
		{"missing token", `{service="api"}`, "", http.StatusUnauthorized},
		{"missing query", ``, "secret", http.StatusBadRequest},
		{"unparsable query", `{service=}`, "secret", http.StatusBadRequest},
		{"stage the evaluator cannot run", `{service="api"}%20|%20json`, "secret", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, status, err := dial(t, ts, tt.query, tt.token)
			if err == nil {
				t.Fatal("handshake succeeded")
			}
			if status != tt.want {
				t.Fatalf("status = %d, want %d (%v)", status, tt.want, err)
			}
		})
	}
}

func TestTailShutdownSaysGoodbye(t *testing.T) {
	t.Parallel()
	srv, _, ts := newTailServer(t)
	c, _, err := dial(t, ts, `{service="api"}`, "secret")
	if err != nil {
		t.Fatal(err)
	}
	msgs := reader(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = next(t, msgs)
	if got := websocket.CloseStatus(err); got != websocket.StatusGoingAway {
		t.Errorf("close status = %v (%v), want GoingAway", got, err)
	}
}
