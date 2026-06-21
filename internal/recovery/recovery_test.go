package recovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vedansh/p2p-container-distribution/internal/hashring"
	"github.com/vedansh/p2p-container-distribution/internal/replication"
	"github.com/vedansh/p2p-container-distribution/internal/storage"
)

type fakeResolver struct {
	addrs map[string]string
}

func (f *fakeResolver) AddrFor(nodeID string) (string, bool) {
	a, ok := f.addrs[nodeID]
	return a, ok
}

type fakeTransport struct {
	mu       sync.Mutex
	received map[string]map[string]bool // addr -> set of chunkIDs received
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{received: make(map[string]map[string]bool)}
}

func (f *fakeTransport) PushChunk(ctx context.Context, peerAddr, chunkID string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.received[peerAddr] == nil {
		f.received[peerAddr] = make(map[string]bool)
	}
	f.received[peerAddr][chunkID] = true
	return nil
}

func (f *fakeTransport) has(addr, chunkID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.received[addr] != nil && f.received[addr][chunkID]
}

// TestSelfHealingAfterNodeDeath simulates the headline scenario: a node
// dies, and a survivor that holds a local copy of an affected chunk
// re-replicates it to whichever node now covers the gap in the replica set.
func TestSelfHealingAfterNodeDeath(t *testing.T) {
	ring := hashring.New()
	for _, n := range []string{"node-a", "node-b", "node-c", "node-d"} {
		ring.AddNode(n)
	}

	chunkID := "chunk-heal-1"
	targetsBefore := ring.GetN(chunkID, replication.Factor)

	// Survivor = some node in the original replica set.
	self := targetsBefore[0]
	local := storage.NewMemory()
	local.Put(chunkID, []byte("important data"))

	transport := newFakeTransport()
	resolver := &fakeResolver{addrs: map[string]string{
		"node-a": "addr-a", "node-b": "addr-b", "node-c": "addr-c", "node-d": "addr-d",
	}}
	engine := replication.NewEngine(ring, local, resolver, transport, self, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.Start(ctx)
	defer engine.Stop()

	mgr := New(ring, local, engine, self)

	// Kill a node from the original replica set that ISN'T self, if any;
	// otherwise kill any node not equal to self.
	var deadNode string
	for _, n := range targetsBefore {
		if n != self {
			deadNode = n
			break
		}
	}
	if deadNode == "" {
		t.Fatal("test setup error: need at least 2 nodes in replica set")
	}

	mgr.HandleNodeDead(deadNode)

	// Wait for async repair + replication to complete.
	deadline := time.Now().Add(2 * time.Second)
	newTargets := ring.GetN(chunkID, replication.Factor)
	for time.Now().Before(deadline) {
		allDelivered := true
		for _, nodeID := range newTargets {
			if nodeID == self {
				continue
			}
			addr, _ := resolver.AddrFor(nodeID)
			if !transport.has(addr, chunkID) {
				allDelivered = false
			}
		}
		if allDelivered {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, nodeID := range ring.Nodes() {
		if nodeID == deadNode {
			t.Fatalf("dead node %s should have been removed from ring", deadNode)
		}
	}

	for _, nodeID := range newTargets {
		if nodeID == self {
			continue
		}
		addr, _ := resolver.AddrFor(nodeID)
		if !transport.has(addr, chunkID) {
			t.Fatalf("expected chunk %s to be repaired onto new target %s (%s)", chunkID, nodeID, addr)
		}
	}
}

func TestNodeReAdmittedOnAlive(t *testing.T) {
	ring := hashring.New()
	ring.AddNode("node-a")
	ring.AddNode("node-b")
	local := storage.NewMemory()
	transport := newFakeTransport()
	resolver := &fakeResolver{addrs: map[string]string{"node-a": "a", "node-b": "b"}}
	engine := replication.NewEngine(ring, local, resolver, transport, "node-a", 1)

	mgr := New(ring, local, engine, "node-a")
	mgr.HandleNodeDead("node-b")

	if len(ring.Nodes()) != 1 {
		t.Fatalf("expected node-b removed, ring has %v", ring.Nodes())
	}

	mgr.HandleNodeAlive("node-b", "b")
	found := false
	for _, n := range ring.Nodes() {
		if n == "node-b" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected node-b re-added to ring after HandleNodeAlive")
	}
}
