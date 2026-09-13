package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
	"github.com/jamespolk/go-log-aggregator/internal/query"
)

// newQueryServer has no database: every case here must be answered before the
// executor is reached, which is exactly what the auth and validation layers
// are for. The database-backed paths are in test/integration.
func newQueryServer(t *testing.T, token string) *Server {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return New(&config.HTTP{Addr: ":0", AuthToken: token, QueryTimeout: time.Second, QueryMaxRows: 50},
		observability.NewHealth(time.Second), nil, log)
}

func do(t *testing.T, srv *Server, method, path, token, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("%s %s: non-JSON body %q", method, path, rec.Body.String())
	}
	return rec.Code, e.Error
}

func TestBearerAuth(t *testing.T) {
	srv := newQueryServer(t, "s3cret")
	cases := []struct {
		name, method, path, token string
		wantCode                  int
		wantErr                   string
	}{
		{"missing token", http.MethodPost, "/v1/query", "", http.StatusUnauthorized, "bearer token"},
		{"wrong token", http.MethodPost, "/v1/query", "s3cret ", http.StatusUnauthorized, "bearer token"},
		{"labels need auth", http.MethodGet, "/v1/labels", "", http.StatusUnauthorized, "bearer token"},
		{"values need auth", http.MethodGet, "/v1/labels/service/values", "nope", http.StatusUnauthorized, "bearer token"},
		// A 400 rather than 401 proves the token was accepted.
		{"right token", http.MethodPost, "/v1/query", "s3cret", http.StatusBadRequest, "invalid request body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, msg := do(t, srv, tc.method, tc.path, tc.token, "not json")
			if code != tc.wantCode || !strings.Contains(msg, tc.wantErr) {
				t.Errorf("got %d %q, want %d containing %q", code, msg, tc.wantCode, tc.wantErr)
			}
		})
	}
}

func TestNoTokenConfiguredFailsClosed(t *testing.T) {
	srv := newQueryServer(t, "")
	code, msg := do(t, srv, http.MethodPost, "/v1/query", "anything", `{"query":"{service=\"api\"}"}`)
	if code != http.StatusUnauthorized || !strings.Contains(msg, "HTTP_AUTH_TOKEN") {
		t.Errorf("got %d %q, want 401 naming the variable to set", code, msg)
	}
}

func TestQueryRejectsBadRequests(t *testing.T) {
	srv := newQueryServer(t, "t")
	cases := []struct {
		name, body, wantErr string
	}{
		{"parse error names the column", `{"query":"{service=\"api\""}`, "1:"},
		{"planner error", `{"query":"{service=\"api\"} | regexp \"(?P<a>x)\" | b = \"1\"", "start":"2026-09-01T00:00:00Z","end":"2026-09-01T01:00:00Z"}`, `"b"`},
		{"bad level", `{"query":"{level=\"loud\"}"}`, "level"},
		{"direction", `{"query":"{service=\"api\"}","direction":"sideways"}`, "direction"},
		{"end before start", `{"query":"{service=\"api\"}","start":"2026-09-02T00:00:00Z","end":"2026-09-01T00:00:00Z"}`, "end time"},
		{"too long", `{"query":"` + strings.Repeat(" ", maxQueryLen+1) + `"}`, "too long"},
		{"oversized body", `{"query":"{service=\"api\"}","x":"` + strings.Repeat("a", maxBodyBytes) + `"}`, "request body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, msg := do(t, srv, http.MethodPost, "/v1/query", "t", tc.body)
			if code != http.StatusBadRequest || !strings.Contains(msg, tc.wantErr) {
				t.Errorf("got %d %q, want 400 containing %q", code, msg, tc.wantErr)
			}
		})
	}
}

func TestRequestDefaultsAndCaps(t *testing.T) {
	api := &queryAPI{maxRows: 50}
	before := time.Now()
	qr, err := api.request(&queryRequest{Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	if qr.Limit != 50 {
		t.Errorf("limit = %d, want clamped to 50", qr.Limit)
	}
	if qr.End.Before(before) || qr.Start != qr.End.Add(-defaultRange) {
		t.Errorf("range = %v..%v, want the hour ending now", qr.Start, qr.End)
	}
	if qr.Direction != query.Backward {
		t.Errorf("direction = %v, want Backward", qr.Direction)
	}
	qr, err = api.request(&queryRequest{Direction: "forward"})
	if err != nil || qr.Limit != 50 || qr.Direction != query.Forward {
		t.Errorf("request(forward) = %+v, %v", qr, err)
	}
}
