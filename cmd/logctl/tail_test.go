package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestTailPrintsRecordsAndDropWarnings(t *testing.T) {
	var gotQuery, gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery, gotAuth = r.URL.Query().Get("query"), r.Header.Get("Authorization")
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow() //nolint:errcheck // test teardown
		ctx := r.Context()
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"time":"2026-09-01T12:00:00Z","level":"warn","message":"timeout","fields":{"route":"/a"},"labels":{"service":"api"}}`))
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"time":"2026-09-01T12:00:01Z","level":"info","message":"ok","dropped":3}`))
		_ = c.Close(websocket.StatusGoingAway, "bye")
	}))
	defer ts.Close()

	var out, errOut bytes.Buffer
	// Upper-case scheme and trailing slash, both of which query accepts too.
	opt := tailOptions{addr: strings.Replace(ts.URL, "http://", "HTTP://", 1) + "/", token: "secret"}
	err := opt.run(context.Background(), `{service="api"}`, &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("err = %v, want the server's goodbye reported", err)
	}
	if gotQuery != `{service="api"}` || gotAuth != "Bearer secret" {
		t.Errorf("sent query %q auth %q", gotQuery, gotAuth)
	}
	want := "2026-09-01T12:00:00Z  warn   timeout  route=/a\n2026-09-01T12:00:01Z  info   ok\n"
	if out.String() != want {
		t.Errorf("stdout = %q, want %q", out.String(), want)
	}
	if !strings.Contains(errOut.String(), "3 matching records dropped") {
		t.Errorf("stderr = %q, want a drop warning", errOut.String())
	}
}

func TestTailReportsARefusedHandshake(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"1:19: tail cannot aggregate; use query"}`))
	}))
	defer ts.Close()

	opt := tailOptions{addr: ts.URL}
	err := opt.run(context.Background(), `{service="api"} | rate(5m)`, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "400 Bad Request: 1:19: tail cannot aggregate") {
		t.Fatalf("err = %v, want the server's reason", err)
	}
}

func TestTailStopsCleanlyOnCancel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow() //nolint:errcheck // test teardown
		<-c.CloseRead(r.Context()).Done()
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	opt := tailOptions{addr: ts.URL}
	if err := opt.run(ctx, `{service="api"}`, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("err = %v, want nil on Ctrl-C", err)
	}
}
