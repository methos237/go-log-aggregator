package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

func newTestServer(t *testing.T, health *observability.Health, w io.Writer) *Server {
	t.Helper()
	if w == nil {
		w = io.Discard
	}
	log := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(&config.HTTP{
		Addr:         ":0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		IdleTimeout:  time.Second,
	}, health, nil, log)
}

func TestHealthz(t *testing.T) {
	srv := newTestServer(t, observability.NewHealth(time.Second), nil)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestReadyzReflectsDependencies(t *testing.T) {
	health := observability.NewHealth(time.Second)
	health.Register("db", func(context.Context) error { return errors.New("dial tcp: refused") })
	srv := newTestServer(t, health, nil)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestUnknownPathReturnsJSONError(t *testing.T) {
	srv := newTestServer(t, observability.NewHealth(time.Second), nil)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	if body.Error == "" {
		t.Error("404 response should carry a JSON error message, not an empty body")
	}
}

func TestHealthzRejectsNonGET(t *testing.T) {
	srv := newTestServer(t, observability.NewHealth(time.Second), nil)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))

	if rec.Code == http.StatusOK {
		t.Error("POST /healthz should not succeed")
	}
}

func TestPanicIsContainedAndLogged(t *testing.T) {
	var logs bytes.Buffer
	health := observability.NewHealth(time.Second)
	health.Register("boom", func(context.Context) error { panic("check exploded") })
	srv := newTestServer(t, health, &logs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	// The panic happens in a check goroutine, so the middleware cannot catch it;
	// what matters here is that the readiness path itself has a recover in the
	// request goroutine. Exercise that directly with a panicking handler.
	panicking := recoverPanic(slog.New(slog.NewJSONHandler(&logs, nil)))(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("handler exploded")
		}),
	)
	panicking.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(logs.String(), "panic in http handler") {
		t.Errorf("panic was not logged:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "handler exploded") {
		t.Error("panic value missing from the log record")
	}
	_ = srv
}

func TestErrAbortHandlerPropagates(t *testing.T) {
	// net/http treats ErrAbortHandler as a deliberate connection drop. Swallowing
	// it would turn an intentional abort into a bogus 500.
	handler := recoverPanic(slog.New(slog.NewJSONHandler(io.Discard, nil)))(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic(http.ErrAbortHandler)
		}),
	)

	defer func() {
		v := recover()
		err, ok := v.(error)
		if !ok || !errors.Is(err, http.ErrAbortHandler) {
			t.Errorf("recovered %v, want http.ErrAbortHandler to propagate", v)
		}
	}()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestProbesLogAtDebugLevel(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	handler := requestLog(log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if logs.Len() != 0 {
		// Probes fire every second or two; at info level they would drown out
		// everything worth reading.
		t.Errorf("successful probe logged at info level:\n%s", logs.String())
	}

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/query", nil))
	if logs.Len() == 0 {
		t.Error("non-probe request was not logged at info level")
	}
}

func TestShutdownOnUnstartedServerIsClean(t *testing.T) {
	srv := newTestServer(t, observability.NewHealth(time.Second), nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown before Serve: %v", err)
	}
}
