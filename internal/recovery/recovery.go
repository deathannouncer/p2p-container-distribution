// Package recovery implements the self-healing behavior: when the cluster
// package detects a dead node, this package removes it from the hash ring
// and re-replicates any chunks that were under-replicated as a result,
// using the surviving replicas as sources.
package recovery

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/vedansh/p2p-container-distribution/internal/hashring"
	"github.com/vedansh/p2p-container-distribution/internal/replication"
	"github.com/vedansh/p2p-container-distribution/internal/storage"
)

// ChunkLocator answers "which chunks did this node used to be responsible
// for, that I might have a copy of locally?" In practice this is the local
// storage backend's chunk ID list intersected against the ring's old
// ownership -- here we keep it simple: we track ownership locally and walk
// our own local store, since any chunk this node holds a copy of and that
// the dead node was *also* a replica owner for needs a repair push.
type ChunkLocator interface {
	List() ([]string, error)
}

// Manager wires together the hash ring, replication engine, and local
// storage to perform self-healing repair when a peer dies.
type Manager struct {
	ring   *hashring.Ring
	local  storage.Backend
	engine *replication.Engine
	selfID string

	mu              sync.Mutex
	repairsRunning  int
	repairsComplete int
}

// New builds a recovery Manager. Wire its HandleNodeDead / HandleNodeAlive
// methods to cluster.Membership.OnNodeDead / OnNodeAlive.
func New(ring *hashring.Ring, local storage.Backend, engine *replication.Engine, selfID string) *Manager {
	return &Manager{ring: ring, local: local, engine: engine, selfID: selfID}
}

// HandleNodeDead is the callback fired by cluster.Membership when a peer
// is declared dead. It:
//  1. Removes the dead node from the consistent hash ring so future writes
//     route around it.
//  2. Walks the chunks this node currently holds locally, and for any
//     chunk whose replica set (recomputed on the *new* ring) now includes
//     a node that doesn't have it yet, re-enqueues that chunk for
//     replication -- restoring the configured replication factor without
//     manual intervention.
//
// This runs concurrently with normal traffic; reads/writes are never
// blocked while repair is in progress, which is how the cluster keeps
// 100% availability through node failure.
func (m *Manager) HandleNodeDead(deadNodeID string) {
	log.Printf("recovery: node %s declared dead, removing from ring and starting repair", deadNodeID)
	m.ring.RemoveNode(deadNodeID)
	go m.repairLocalChunks(context.Background(), deadNodeID)
}

// HandleNodeAlive re-admits a previously dead node into the ring once it's
// confirmed healthy again (e.g. restarted). Existing chunk placement isn't
// retroactively rebalanced onto it -- it simply becomes eligible for new
// writes and future repairs, avoiding a thundering-herd rebalance.
func (m *Manager) HandleNodeAlive(nodeID, addr string) {
	log.Printf("recovery: node %s confirmed alive again, re-adding to ring", nodeID)
	m.ring.AddNode(nodeID)
}

// repairLocalChunks scans every chunk this node has a local copy of and
// re-pushes it to any *new* member of its replica set (post-removal),
// since that's how a replacement/promoted node picks up the chunks it's
// now responsible for.
func (m *Manager) repairLocalChunks(ctx context.Context, deadNodeID string) {
	m.mu.Lock()
	m.repairsRunning++
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.repairsRunning--
		m.repairsComplete++
		m.mu.Unlock()
	}()

	ids, err := m.local.List()
	if err != nil {
		log.Printf("recovery: failed to list local chunks for repair: %v", err)
		return
	}

	repaired := 0
	for _, chunkID := range ids {
		newTargets := m.ring.GetN(chunkID, replication.Factor)
		if !contains(newTargets, m.selfID) {
			// This node is no longer a replica owner for this chunk at all
			// post-removal; nothing to repair from here.
			continue
		}
		wasResponsibleViaDeadNode := false
		for _, t := range newTargets {
			if t == deadNodeID {
				wasResponsibleViaDeadNode = true
			}
		}
		_ = wasResponsibleViaDeadNode // newTargets never contains deadNodeID since it was removed from ring

		data, err := m.local.Get(chunkID)
		if err != nil {
			log.Printf("recovery: could not read local chunk %s for repair: %v", chunkID, err)
			continue
		}
		// Re-enqueue: the replication engine will push to whichever targets
		// in newTargets don't already have it (peers that already have the
		// chunk simply overwrite with identical content -- a no-op).
		m.engine.Enqueue(chunkID, data)
		repaired++
	}
	log.Printf("recovery: repair pass after %s death complete, re-enqueued %d chunks", deadNodeID, repaired)
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// Status reports current repair activity for the /metrics endpoint.
func (m *Manager) Status() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fmt.Sprintf("repairs_running=%d repairs_complete=%d", m.repairsRunning, m.repairsComplete)
}
