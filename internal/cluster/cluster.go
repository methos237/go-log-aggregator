package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/jamespolk/go-log-aggregator/internal/backoff"
	"github.com/jamespolk/go-log-aggregator/internal/config"
	"github.com/jamespolk/go-log-aggregator/internal/model"
	"github.com/jamespolk/go-log-aggregator/internal/version"
)

// Member is one collector as the cluster sees it: where to reach it and what
// it advertised about itself.
type Member struct {
	Name string `json:"name"`
	// Addr is the gossip address; the peer and HTTP ports below are on the
	// same host.
	Addr     string `json:"addr"`
	PeerPort int    `json:"peer_port"`
	HTTPPort int    `json:"http_port"`
	Version  string `json:"version"`
	// Ready is the member's own readiness, as of its last metadata broadcast.
	Ready bool `json:"ready"`
	// VNodes is the ring size the member was configured with. Members must
	// agree; a mismatch is logged because two rings would disagree on owners.
	VNodes int `json:"vnodes"`
}

// PeerAddr is host:port of the member's internal gRPC service.
func (m *Member) PeerAddr() string { return net.JoinHostPort(m.Addr, strconv.Itoa(m.PeerPort)) }

// meta is what a node gossips about itself, JSON inside memberlist's 512-byte
// node metadata. Ring tokens are not in it: they derive from the node name
// and VNodes, so every member computes the same ring from names alone.
type meta struct {
	PeerPort int    `json:"p"`
	HTTPPort int    `json:"h"`
	VNodes   int    `json:"v"`
	Version  string `json:"ver"`
	Ready    bool   `json:"r"`
}

// Cluster is this node's view of membership and the ring built from it.
//
// Membership is maintained only from memberlist's event callbacks, never by
// asking memberlist for its member list inside one: memberlist holds its node
// lock while it notifies, so a call back into it there would deadlock.
type Cluster struct {
	cfg     config.Cluster
	log     *slog.Logger
	metrics *Metrics
	peers   []string

	ml *memberlist.Memberlist

	mu      sync.RWMutex
	self    meta
	members map[string]Member
	ring    *Ring

	leaveOnce sync.Once
	leaveErr  error
}

// New binds the gossip listener and starts memberlist with this node as the
// only member. Call Join to find the others.
func New(cfg *config.Cluster, node string, httpAddr string, log *slog.Logger, metrics *Metrics) (*Cluster, error) {
	peerPort, err := port(cfg.PeerAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: peer addr: %w", err)
	}
	httpPort, err := port(httpAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: http addr: %w", err)
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}
	c := &Cluster{
		cfg:     *cfg,
		log:     log.With(slog.String("component", "cluster")),
		metrics: metrics,
		peers:   cfg.Peers,
		self:    meta{PeerPort: peerPort, HTTPPort: httpPort, VNodes: cfg.VNodes, Version: version.Version},
		members: make(map[string]Member),
		ring:    NewRing(nil, cfg.VNodes),
	}

	mc := memberlist.DefaultLANConfig()
	mc.Name = node
	if mc.BindAddr, mc.BindPort, err = hostPort(cfg.BindAddr); err != nil {
		return nil, fmt.Errorf("cluster: bind addr: %w", err)
	}
	if cfg.AdvertiseAddr != "" {
		if mc.AdvertiseAddr, mc.AdvertisePort, err = hostPort(cfg.AdvertiseAddr); err != nil {
			return nil, fmt.Errorf("cluster: advertise addr: %w", err)
		}
	}
	mc.Delegate = (*delegate)(c)
	mc.Events = (*events)(c)
	// memberlist logs through the standard logger; route it into slog at
	// debug, since its chatter is diagnostic and its real events reach us as
	// callbacks that we log ourselves.
	mc.Logger = slog.NewLogLogger(c.log.Handler(), slog.LevelDebug)

	c.ml, err = memberlist.Create(mc)
	if err != nil {
		return nil, fmt.Errorf("cluster: start memberlist: %w", err)
	}
	return c, nil
}

// Join contacts the seed peers, retrying with backoff until one answers or
// cfg.JoinTimeout passes. Running out of time is not fatal: the node carries
// on alone and is found when a peer joins through it or gossip reaches it.
// No peers configured means a deliberate single-node cluster.
func (c *Cluster) Join(ctx context.Context) error {
	if len(c.peers) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.JoinTimeout)
	defer cancel()
	var lastErr error
	for attempt := 0; ; attempt++ {
		n, err := c.ml.Join(c.peers)
		if err == nil {
			c.log.Info("joined cluster", slog.Int("contacted", n), slog.Any("peers", c.peers))
			return nil
		}
		lastErr = err
		if !backoff.Sleep(ctx, backoff.Delay(attempt, 500*time.Millisecond, 5*time.Second)) {
			break
		}
	}
	if ctx.Err() != nil && !errors.Is(context.Cause(ctx), context.Canceled) {
		c.log.Warn("no seed peer answered; continuing alone", slog.Any("peers", c.peers), slog.Any("error", lastErr))
		return nil
	}
	return fmt.Errorf("cluster: join: %w", lastErr)
}

// SetReady broadcasts this node's readiness to the other members.
func (c *Cluster) SetReady(ready bool) {
	c.mu.Lock()
	changed := c.self.Ready != ready
	c.self.Ready = ready
	c.mu.Unlock()
	if !changed {
		return
	}
	if err := c.ml.UpdateNode(5 * time.Second); err != nil {
		c.log.Warn("readiness broadcast failed", slog.Any("error", err))
	}
}

// Leave tells the cluster this node is going, so peers drop it now rather
// than after failure detection, then stops memberlist. Safe to call more than
// once; memberlist itself panics on a second Leave.
func (c *Cluster) Leave(timeout time.Duration) error {
	c.leaveOnce.Do(func() {
		c.leaveErr = errors.Join(c.ml.Leave(timeout), c.ml.Shutdown())
	})
	return c.leaveErr
}

// Self is this node's name.
func (c *Cluster) Self() string { return c.ml.LocalNode().Name }

// GossipAddr is the address this node's memberlist advertises, which is what
// another node passes as a seed peer.
func (c *Cluster) GossipAddr() string { return c.ml.LocalNode().Address() }

// Ring is the current ring. The value is immutable; callers may hold it.
func (c *Cluster) Ring() *Ring {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring
}

// Members lists the alive members, sorted by name.
func (c *Cluster) Members() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Member, 0, len(c.members))
	for _, n := range c.ring.nodes {
		out = append(out, c.members[n])
	}
	return out
}

// Owner returns the member owning id. ok is false only on an empty ring,
// which cannot happen once memberlist has reported this node's own join.
func (c *Cluster) Owner(id model.StreamID) (m Member, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok = c.members[c.ring.Owner(id)]
	return m, ok
}

// update applies a membership event and rebuilds the ring, logging every
// ownership transfer with its before and after.
func (c *Cluster) update(n *memberlist.Node, alive bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if alive {
		var md meta
		if len(n.Meta) > 0 {
			if err := json.Unmarshal(n.Meta, &md); err != nil {
				c.log.Warn("member metadata unreadable", slog.String("member", n.Name), slog.Any("error", err))
			}
		}
		if md.VNodes != 0 && md.VNodes != c.self.VNodes {
			c.log.Error("member disagrees on ring size; owners will not match",
				slog.String("member", n.Name), slog.Int("theirs", md.VNodes), slog.Int("ours", c.self.VNodes))
		}
		c.members[n.Name] = Member{
			Name: n.Name, Addr: n.Addr.String(), PeerPort: md.PeerPort, HTTPPort: md.HTTPPort,
			Version: md.Version, Ready: md.Ready, VNodes: md.VNodes,
		}
	} else {
		delete(c.members, n.Name)
	}
	c.metrics.Members.Set(float64(len(c.members)))

	names := make([]string, 0, len(c.members))
	for name := range c.members {
		names = append(names, name)
	}
	old := c.ring
	c.ring = NewRing(names, c.cfg.VNodes)
	moves := Diff(old, c.ring)
	if len(moves) == 0 {
		return
	}
	c.metrics.OwnershipChanges.Add(float64(len(moves)))
	for _, t := range moves {
		c.log.Debug("ownership transfer",
			slog.String("from", t.From), slog.String("to", t.To),
			slog.String("range", fmt.Sprintf("(%016x, %016x]", t.Lo, t.Hi)))
	}
	event := "joined"
	if !alive {
		event = "left"
	}
	c.log.Info("ring rebuilt",
		slog.String("member", n.Name), slog.String("event", event),
		slog.Int("members", len(c.members)), slog.Int("ranges_moved", len(moves)))
}

// delegate is Cluster's memberlist.Delegate: node metadata only. The gossip
// message and state-sync hooks are unused; membership is the whole payload.
type delegate Cluster

func (d *delegate) NodeMeta(limit int) []byte {
	c := (*Cluster)(d)
	c.mu.RLock()
	b, _ := json.Marshal(c.self) // cannot fail on a struct of ints, a string and a bool
	c.mu.RUnlock()
	if len(b) > limit {
		c.log.Error("node metadata exceeds memberlist limit", slog.Int("bytes", len(b)), slog.Int("limit", limit))
		return nil
	}
	return b
}

func (*delegate) NotifyMsg([]byte)                {}
func (*delegate) GetBroadcasts(int, int) [][]byte { return nil }
func (*delegate) LocalState(bool) []byte          { return nil }
func (*delegate) MergeRemoteState([]byte, bool)   {}

// events is Cluster's memberlist.EventDelegate.
type events Cluster

func (e *events) NotifyJoin(n *memberlist.Node)   { (*Cluster)(e).update(n, true) }
func (e *events) NotifyLeave(n *memberlist.Node)  { (*Cluster)(e).update(n, false) }
func (e *events) NotifyUpdate(n *memberlist.Node) { (*Cluster)(e).update(n, true) }

func hostPort(addr string) (string, int, error) {
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return "", 0, fmt.Errorf("port %q: %w", p, err)
	}
	if host == "" {
		host = "0.0.0.0"
	}
	return host, n, nil
}

func port(addr string) (int, error) {
	_, p, err := hostPort(addr)
	return p, err
}
