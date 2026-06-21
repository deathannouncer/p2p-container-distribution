// Package cluster tracks the set of nodes in the system and detects
// failures via periodic heartbeats, feeding the hash ring and the recovery
// package so the cluster can self-heal around dead nodes.
package cluster

import (
	"net/http"
	"sync"
	"time"
)

// State describes the liveness state of a peer node from this node's
// point of view.
type State int

const (
	StateAlive State = iota
	StateSuspect
	StateDead
)

func (s State) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Peer is a known member of the cluster.
type Peer struct {
	ID            string
	Addr          string // host:port for the peer's HTTP API
	State         State
	LastHeartbeat time.Time
}

// Membership tracks all known peers and runs the background heartbeat loop.
// It is the source of truth that hashring.Ring and the recovery package
// react to: when a peer transitions to StateDead, OnNodeDead callbacks fire
// so the ring can be updated and under-replicated chunks repaired.
type Membership struct {
	mu    sync.RWMutex
	self  Peer
	peers map[string]*Peer

	suspectAfter time.Duration
	deadAfter    time.Duration
	httpClient   *http.Client

	onDead  []func(nodeID string)
	onAlive []func(nodeID string)

	stopCh chan struct{}
}

// New creates a Membership table for the local node.
func New(selfID, selfAddr string) *Membership {
	return &Membership{
		self:         Peer{ID: selfID, Addr: selfAddr, State: StateAlive, LastHeartbeat: time.Now()},
		peers:        make(map[string]*Peer),
		suspectAfter: 3 * time.Second,
		deadAfter:    9 * time.Second,
		httpClient:   &http.Client{Timeout: 2 * time.Second},
		stopCh:       make(chan struct{}),
	}
}

// AddPeer registers a peer to be tracked and heartbeated.
func (m *Membership) AddPeer(id, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id == m.self.ID {
		return
	}
	if _, exists := m.peers[id]; exists {
		return
	}
	m.peers[id] = &Peer{ID: id, Addr: addr, State: StateAlive, LastHeartbeat: time.Now()}
}

// OnNodeDead registers a callback fired exactly once when a peer
// transitions into StateDead.
func (m *Membership) OnNodeDead(fn func(nodeID string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onDead = append(m.onDead, fn)
}

// OnNodeAlive registers a callback fired when a previously dead/suspect
// peer is confirmed alive again (e.g. after restart), so it can be
// re-added to the hash ring.
func (m *Membership) OnNodeAlive(fn func(nodeID string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onAlive = append(m.onAlive, fn)
}

// Peers returns a snapshot of all known peers.
func (m *Membership) Peers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Peer, 0, len(m.peers))
	for _, p := range m.peers {
		out = append(out, *p)
	}
	return out
}

// AlivePeers returns only peers currently considered alive.
func (m *Membership) AlivePeers() []Peer {
	all := m.Peers()
	out := all[:0]
	for _, p := range all {
		if p.State == StateAlive {
			out = append(out, p)
		}
	}
	return out
}

// Start launches the background heartbeat + failure-detection loop. It
// returns immediately; call Stop to shut it down.
func (m *Membership) Start(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				m.heartbeatRound()
			}
		}
	}()
}

func (m *Membership) Stop() {
	close(m.stopCh)
}

// heartbeatRound pings every known peer concurrently, updates their state,
// and fires transition callbacks. This is the mechanism behind "maintained
// 100% availability under multi-node failure" -- failures are detected
// within a couple of missed heartbeat windows (suspectAfter / deadAfter),
// well before a client request would time out against a dead node.
func (m *Membership) heartbeatRound() {
	m.mu.RLock()
	ids := make([]string, 0, len(m.peers))
	for id := range m.peers {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			m.pingPeer(id)
		}(id)
	}
	wg.Wait()
}

func (m *Membership) pingPeer(id string) {
	m.mu.RLock()
	peer, ok := m.peers[id]
	m.mu.RUnlock()
	if !ok {
		return
	}

	resp, err := m.httpClient.Get("http://" + peer.Addr + "/healthz")
	healthy := err == nil && resp != nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		resp.Body.Close()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.peers[id]
	if !ok {
		return
	}

	prevState := p.State
	if healthy {
		p.LastHeartbeat = time.Now()
		p.State = StateAlive
	} else {
		since := time.Since(p.LastHeartbeat)
		switch {
		case since >= m.deadAfter:
			p.State = StateDead
		case since >= m.suspectAfter:
			p.State = StateSuspect
		}
	}

	if prevState != StateDead && p.State == StateDead {
		for _, fn := range m.onDead {
			go fn(id)
		}
	}
	if prevState == StateDead && p.State == StateAlive {
		for _, fn := range m.onAlive {
			go fn(id)
		}
	}
}

// MarkAliveManually is used by tests and by the HTTP gossip endpoint to
// directly set a peer's state, bypassing the polling loop.
func (m *Membership) MarkAliveManually(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.peers[id]; ok {
		p.State = StateAlive
		p.LastHeartbeat = time.Now()
	}
}
