package cluster

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// startNode binds a memberlist on an OS-assigned loopback port and returns the
// cluster plus the address peers should join.
func startNode(t *testing.T, name string, peers ...string) (*Cluster, string) {
	t.Helper()
	cfg := config.Cluster{
		Enabled: true, BindAddr: "127.0.0.1:0", Peers: peers, PeerAddr: "127.0.0.1:9096",
		VNodes: 32, JoinTimeout: 5 * time.Second,
	}
	c, err := New(&cfg, name, "127.0.0.1:8080", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatalf("New(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = c.Leave(time.Second) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Join(ctx); err != nil {
		t.Fatalf("Join(%s): %v", name, err)
	}
	self := c.ml.LocalNode()
	return c, "127.0.0.1:" + strconv.Itoa(int(self.Port))
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestClusterMembershipAndRing(t *testing.T) {
	a, addrA := startNode(t, "a")
	b, _ := startNode(t, "b", addrA)
	c, _ := startNode(t, "c", addrA)
	all := []*Cluster{a, b, c}

	eventually(t, "three members everywhere", func() bool {
		for _, n := range all {
			if len(n.Members()) != 3 {
				return false
			}
		}
		return true
	})

	// Every node computes the same ring from the same names, so owners agree.
	for id := int64(0); id < 200; id++ {
		want := a.Ring().Owner(modelID(id))
		for _, n := range all[1:] {
			if got := n.Ring().Owner(modelID(id)); got != want {
				t.Fatalf("owner of %d: %s says %q, a says %q", id, n.Self(), got, want)
			}
		}
	}

	// Metadata made the round trip: b sees a's ports and version.
	var seen Member
	for _, m := range b.Members() {
		if m.Name == "a" {
			seen = m
		}
	}
	if seen.PeerPort != 9096 || seen.HTTPPort != 8080 || seen.VNodes != 32 || seen.Ready {
		t.Errorf("b's view of a = %+v", seen)
	}
	if got := seen.PeerAddr(); got != "127.0.0.1:9096" {
		t.Errorf("PeerAddr = %q", got)
	}

	// Readiness is broadcast as a metadata update.
	a.SetReady(true)
	eventually(t, "a ready as seen by c", func() bool {
		for _, m := range c.Members() {
			if m.Name == "a" && m.Ready {
				return true
			}
		}
		return false
	})

	// A leave shrinks the ring on the survivors and moves only the leaver's keys.
	before := b.Ring()
	if err := c.Leave(time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, "two members on a and b", func() bool { return len(a.Members()) == 2 && len(b.Members()) == 2 })
	after := b.Ring()
	if after.Len() != 2 || after.Owner(modelID(1)) == "c" {
		t.Errorf("ring after leave: nodes %v", after.Nodes())
	}
	for id := int64(0); id < 200; id++ {
		if was := before.Owner(modelID(id)); was != "c" && after.Owner(modelID(id)) != was {
			t.Errorf("key %d moved from %q though only c left", id, was)
		}
	}
	if len(Diff(before, after)) == 0 {
		t.Error("Diff reports no ownership change after a leave")
	}
}

func TestClusterSingleNode(t *testing.T) {
	a, _ := startNode(t, "solo")
	eventually(t, "self membership", func() bool { return len(a.Members()) == 1 })
	m, ok := a.Owner(modelID(7))
	if !ok || m.Name != "solo" {
		t.Errorf("Owner = %+v, %v", m, ok)
	}
}

func TestClusterJoinTimeoutContinuesAlone(t *testing.T) {
	cfg := config.Cluster{
		Enabled: true, BindAddr: "127.0.0.1:0", Peers: []string{"127.0.0.1:1"}, PeerAddr: "127.0.0.1:9096",
		VNodes: 8, JoinTimeout: 300 * time.Millisecond,
	}
	c, err := New(&cfg, "lonely", ":8080", slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Leave(time.Second) }()
	if err := c.Join(context.Background()); err != nil {
		t.Errorf("Join after timeout should carry on alone, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Join(ctx); err == nil {
		t.Error("Join with a canceled context should fail")
	}
}

func TestNewRejectsBadAddrs(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, cfg := range map[string]config.Cluster{
		"bind": {BindAddr: "nope", PeerAddr: "127.0.0.1:9096", VNodes: 8},
		"peer": {BindAddr: "127.0.0.1:0", PeerAddr: "9096", VNodes: 8},
		"adv":  {BindAddr: "127.0.0.1:0", AdvertiseAddr: "x", PeerAddr: "127.0.0.1:9096", VNodes: 8},
	} {
		if _, err := New(&cfg, name, ":8080", log, nil); err == nil {
			t.Errorf("%s: New accepted %+v", name, cfg)
		}
	}
	if _, err := New(&config.Cluster{BindAddr: "127.0.0.1:0", PeerAddr: "127.0.0.1:9096", VNodes: 8}, "x", "8080", log, nil); err == nil {
		t.Error("New accepted an HTTP addr without a port")
	}
}

func modelID(i int64) model.StreamID { return model.StreamID(i) }
