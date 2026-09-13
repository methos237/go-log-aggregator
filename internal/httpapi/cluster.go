package httpapi

import (
	"net/http"

	"github.com/jamespolk/go-log-aggregator/internal/cluster"
)

// ClusterView is what /v1/cluster reads. *cluster.Cluster satisfies it; nil
// means clustering is off and the node answers for itself alone.
type ClusterView interface {
	Self() string
	Members() []cluster.Member
	Ring() *cluster.Ring
}

// clusterResponse is the body of GET /v1/cluster: who is in the cluster, how
// the ring divides the key space between them, and where the answering node
// sits. It is the page the failure demo is built around, so it favors being
// readable over being small.
type clusterResponse struct {
	Enabled bool            `json:"enabled"`
	Self    string          `json:"self"`
	Members []clusterMember `json:"members"`
	Ring    clusterRing     `json:"ring"`
}

type clusterMember struct {
	cluster.Member
	// Share is the member's fraction of the key space, 0 to 1.
	Share float64 `json:"share"`
	// Tokens is the member's virtual-node count on this node's ring.
	Tokens int `json:"tokens"`
}

type clusterRing struct {
	// Members is the ring's member count; Tokens the total virtual nodes.
	Members int `json:"members"`
	Tokens  int `json:"tokens"`
	// Arcs is every owned key range in ring order, hex-encoded so the ranges
	// read in the same order they sort. Present only with ?arcs=1, since 128
	// per member adds up.
	Arcs []clusterArc `json:"arcs,omitempty"`
}

type clusterArc struct {
	Lo    string `json:"lo"`
	Hi    string `json:"hi"`
	Owner string `json:"owner"`
}

func (a *queryAPI) clusterState(w http.ResponseWriter, r *http.Request) {
	if a.cluster == nil {
		writeJSON(w, clusterResponse{Self: a.node, Members: []clusterMember{}})
		return
	}
	ring := a.cluster.Ring()
	shares := ring.Shares()
	resp := clusterResponse{Enabled: true, Self: a.cluster.Self(), Ring: clusterRing{Members: ring.Len()}}
	for _, m := range a.cluster.Members() {
		tokens := len(ring.Tokens(m.Name))
		resp.Ring.Tokens += tokens
		resp.Members = append(resp.Members, clusterMember{Member: m, Share: shares[m.Name], Tokens: tokens})
	}
	if resp.Members == nil {
		resp.Members = []clusterMember{}
	}
	if r.URL.Query().Get("arcs") == "1" {
		for _, arc := range ring.Arcs() {
			resp.Ring.Arcs = append(resp.Ring.Arcs, clusterArc{Lo: hex16(arc.Lo), Hi: hex16(arc.Hi), Owner: arc.Owner})
		}
	}
	writeJSON(w, resp)
}

func hex16(v uint64) string {
	const digits = "0123456789abcdef"
	var b [16]byte
	for i := 15; i >= 0; i-- {
		b[i] = digits[v&0xf]
		v >>= 4
	}
	return string(b[:])
}
