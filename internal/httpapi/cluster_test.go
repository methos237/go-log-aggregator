package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/cluster"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/observability"
)

// fakeCluster is a static three-member view; the ring is real.
type fakeCluster struct{ ring *cluster.Ring }

func (fakeCluster) Self() string          { return "b" }
func (f fakeCluster) Ring() *cluster.Ring { return f.ring }
func (fakeCluster) Members() []cluster.Member {
	return []cluster.Member{
		{Name: "a", Addr: "10.0.0.1", PeerPort: 9096, HTTPPort: 8080, Ready: true, VNodes: 16},
		{Name: "b", Addr: "10.0.0.2", PeerPort: 9096, HTTPPort: 8080, Ready: true, VNodes: 16},
		{Name: "c", Addr: "10.0.0.3", PeerPort: 9096, HTTPPort: 8080, VNodes: 16},
	}
}

func getCluster(t *testing.T, view ClusterView, path string) clusterResponse {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	srv := New(&config.HTTP{Addr: ":0", AuthToken: "t", QueryTimeout: time.Second, QueryMaxRows: 10},
		observability.NewHealth(time.Second), Deps{Node: "solo", Cluster: view}, log)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out clusterResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestClusterEndpoint(t *testing.T) {
	ring := cluster.NewRing([]string{"a", "b", "c"}, 16)
	out := getCluster(t, fakeCluster{ring}, "/v1/cluster")
	if !out.Enabled || out.Self != "b" || len(out.Members) != 3 || out.Ring.Members != 3 || out.Ring.Tokens != 48 {
		t.Errorf("response = %+v", out)
	}
	var total float64
	for _, m := range out.Members {
		total += m.Share
		if m.Tokens != 16 {
			t.Errorf("%s tokens = %d", m.Name, m.Tokens)
		}
	}
	if total < 0.999 || total > 1.001 {
		t.Errorf("shares sum to %v", total)
	}
	if out.Ring.Arcs != nil {
		t.Error("arcs included without ?arcs=1")
	}

	out = getCluster(t, fakeCluster{ring}, "/v1/cluster?arcs=1")
	if len(out.Ring.Arcs) != 48 || len(out.Ring.Arcs[0].Lo) != 16 {
		t.Errorf("arcs = %d, first %+v", len(out.Ring.Arcs), out.Ring.Arcs[0])
	}
}

func TestClusterEndpointDisabled(t *testing.T) {
	out := getCluster(t, nil, "/v1/cluster")
	if out.Enabled || out.Self != "solo" || len(out.Members) != 0 {
		t.Errorf("response = %+v", out)
	}
}

func TestClusterEndpointNeedsAuth(t *testing.T) {
	srv := newQueryServer(t, "t")
	code, _ := do(t, srv, http.MethodGet, "/v1/cluster", "", "")
	if code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", code)
	}
}
