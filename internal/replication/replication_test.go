package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vedansh/p2p-container-distribution/internal/hashring"
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
	received map[string][]string // addr -> chunkIDs pushed to it
	failFor  map[string]int      // addr -> number of times to fail before succeeding
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		received: make(map[string][]string),
		failFor:  make(map[string]int),
	}
}

func (f *fakeTransport) PushChunk(ctx context.Context, peerAddr, chunkID string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFor[peerAddr] > 0 {
		f.failFor[peerAddr]--
		return fmt.Errorf("simulated failure")
	}
	f.received[peerAddr] = append(f.received[peerAddr], chunkID)
	return nil
}

func (f *fakeTransport) countFor(addr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received[addr])
}

func setupRing(nodes ...string) *hashring.Ring {
	r := hashring.New()
	for _, n := range nodes {
		r.AddNode(n)
	}
	return r
}

func TestReplicationReachesAllTargets(t *testing.T) {
	ring := setupRing("node-a", "node-b", "node-c", "node-d")
	transport := newFakeTransport()
	resolver := &fakeResolver{addrs: map[string]string{
		"node-a": "addr-a", "node-b": "addr-b", "node-c": "addr-c", "node-d": "addr-d",
	}}

	local := storage.NewMemory()
	chunkID := "chunk-test-1"
	targets := ring.GetN(chunkID, Factor)
	self := targets[0] // pretend this node is the primary owner

	eng := NewEngine(ring, local, resolver, transport, self, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eng.Start(ctx)

	eng.Enqueue(chunkID, []byte("payload"))
	eng.Stop()

	for _, nodeID := range targets {
		if nodeID == self {
			continue
		}
		addr, _ := resolver.AddrFor(nodeID)
		if transport.countFor(addr) != 1 {
			t.Fatalf("expected chunk pushed exactly once to %s, got %d", addr, transport.countFor(addr))
		}
	}
}

func TestReplicationRetriesOnFailure(t *testing.T) {
	ring := setupRing("node-a", "node-b")
	transport := newFakeTransport()
	resolver := &fakeResolver{addrs: map[string]string{"node-a": "addr-a", "node-b": "addr-b"}}

	chunkID := "chunk-retry"
	targets := ring.GetN(chunkID, 2)
	self := targets[0]
	otherAddr, _ := resolver.AddrFor(targets[1])
	transport.failFor[otherAddr] = 2 // fail twice, succeed on 3rd attempt

	eng := NewEngine(ring, storage.NewMemory(), resolver, transport, self, 1)
	eng.retryBase = 1 * time.Millisecond // speed up test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eng.Start(ctx)
	eng.Enqueue(chunkID, []byte("data"))
	eng.Stop()

	if transport.countFor(otherAddr) != 1 {
		t.Fatalf("expected eventual successful push after retries, got count %d", transport.countFor(otherAddr))
	}
}

func TestEngineSkipsSelf(t *testing.T) {
	ring := setupRing("node-a")
	transport := newFakeTransport()
	resolver := &fakeResolver{addrs: map[string]string{"node-a": "addr-a"}}

	eng := NewEngine(ring, storage.NewMemory(), resolver, transport, "node-a", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eng.Start(ctx)
	eng.Enqueue("chunk-x", []byte("d"))
	eng.Stop()

	if transport.countFor("addr-a") != 0 {
		t.Fatalf("engine should not push chunk to itself")
	}
}
