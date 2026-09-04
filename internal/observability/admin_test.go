package observability

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jamespolk/go-log-aggregator/internal/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMetricsEndpointServesBuildInfoAndNodeLabel(t *testing.T) {
	m := NewMetrics("collector-1")
	srv := NewAdminServer(config.Admin{Addr: ":0", EnablePprof: false}, m, testLogger())

	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "logagg_build_info") {
		t.Error("build info metric missing from /metrics")
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Error("Go runtime collector missing from /metrics")
	}
}

func TestRegistererAddsNodeLabel(t *testing.T) {
	m := NewMetrics("collector-9")

	counter := newTestCounter()
	m.Registerer.MustRegister(counter)
	counter.Inc()

	srv := NewAdminServer(config.Admin{Addr: ":0"}, m, testLogger())
	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	// Every project metric must be attributable to a replica without the caller
	// remembering to add the label at each definition site.
	if want := `logagg_test_total{node="collector-9"}`; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("/metrics output missing %s:\n%s", want, rec.Body.String())
	}
}

func TestPprofDisabled(t *testing.T) {
	m := NewMetrics("collector-1")
	srv := NewAdminServer(config.Admin{Addr: ":0", EnablePprof: false}, m, testLogger())

	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d when pprof is disabled", rec.Code, http.StatusNotFound)
	}
}

func TestPprofEnabled(t *testing.T) {
	m := NewMetrics("collector-1")
	srv := NewAdminServer(config.Admin{Addr: ":0", EnablePprof: true}, m, testLogger())

	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d when pprof is enabled", rec.Code, http.StatusOK)
	}
}

func TestPprofNotOnDefaultServeMux(t *testing.T) {
	// net/http/pprof registers itself on http.DefaultServeMux in init(). Anything
	// that serves the default mux would then expose profiles unintentionally, so
	// no server in this project may use it.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)

	m := NewMetrics("collector-1")
	srv := NewAdminServer(config.Admin{Addr: ":0", EnablePprof: false}, m, testLogger())
	if srv.srv.Handler == http.DefaultServeMux {
		t.Fatal("admin server must not use http.DefaultServeMux")
	}
	srv.srv.Handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Error("pprof reachable through a mux that should not have it")
	}
}
