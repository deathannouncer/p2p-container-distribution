package cluster

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newHealthyServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func addrOf(srv *httptest.Server) string {
	// httptest.Server.URL is like "http://127.0.0.1:PORT"; strip the scheme.
	return srv.URL[len("http://"):]
}

func TestHeartbeatDetectsAlivePeer(t *testing.T) {
	srv := newHealthyServer(t)
	defer srv.Close()

	m := New("self", "localhost:0")
	m.AddPeer("peer-1", addrOf(srv))
	m.heartbeatRound()

	peers := m.AlivePeers()
	if len(peers) != 1 || peers[0].ID != "peer-1" {
		t.Fatalf("expected peer-1 alive, got %+v", peers)
	}
}

func TestHeartbeatDetectsDeadPeer(t *testing.T) {
	m := New("self", "localhost:0")
	// Point at an address nothing is listening on.
	m.AddPeer("peer-down", "127.0.0.1:1")
	m.deadAfter = 0 // force immediate dead classification for the test
	m.suspectAfter = 0

	var deadFired int32
	done := make(chan struct{}, 1)
	m.OnNodeDead(func(nodeID string) {
		if nodeID == "peer-down" {
			atomic.StoreInt32(&deadFired, 1)
			done <- struct{}{}
		}
	})

	m.heartbeatRound()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	if atomic.LoadInt32(&deadFired) != 1 {
		t.Fatalf("expected onDead callback to fire for peer-down")
	}
}

func TestSelfNotAddedAsPeer(t *testing.T) {
	m := New("self", "localhost:1234")
	m.AddPeer("self", "localhost:1234")
	if len(m.Peers()) != 0 {
		t.Fatalf("expected self not to be added as its own peer")
	}
}

func TestNodeAliveCallbackFiresAfterRecovery(t *testing.T) {
	srv := newHealthyServer(t)
	defer srv.Close()

	m := New("self", "localhost:0")
	m.AddPeer("peer-1", addrOf(srv))

	m.mu.Lock()
	m.peers["peer-1"].State = StateDead
	m.mu.Unlock()

	fired := make(chan string, 1)
	m.OnNodeAlive(func(nodeID string) { fired <- nodeID })

	m.heartbeatRound()

	select {
	case id := <-fired:
		if id != "peer-1" {
			t.Fatalf("unexpected node id: %s", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected onAlive callback to fire")
	}
}
