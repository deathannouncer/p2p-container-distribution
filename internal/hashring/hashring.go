// Package hashring implements a consistent hash ring with virtual nodes,
// used to deterministically map chunk IDs to the physical nodes responsible
// for storing them (the chunk's primary + its N replicas).
package hashring

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// VirtualNodesPerNode controls how many points each physical node owns on
// the ring. More virtual nodes -> smoother load distribution across nodes,
// at the cost of more memory and slightly slower lookups.
const VirtualNodesPerNode = 150

// Ring is a thread-safe consistent hash ring.
type Ring struct {
	mu           sync.RWMutex
	sortedHashes []uint64          // sorted ring positions
	hashToNode   map[uint64]string // ring position -> physical node ID
	nodes        map[string]bool   // set of physical nodes currently on the ring
}

// New creates an empty ring.
func New() *Ring {
	return &Ring{
		hashToNode: make(map[uint64]string),
		nodes:      make(map[string]bool),
	}
}

// hashKey hashes an arbitrary string into the ring's 64-bit key space using
// SHA-1 (only the first 8 bytes are used). SHA-1 is fine here -- this is a
// load-distribution hash, not a security boundary.
func hashKey(key string) uint64 {
	sum := sha1.Sum([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

// AddNode adds a physical node to the ring, creating VirtualNodesPerNode
// points for it. Idempotent if the node is already present.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes[nodeID] {
		return
	}
	r.nodes[nodeID] = true
	for i := 0; i < VirtualNodesPerNode; i++ {
		vKey := fmt.Sprintf("%s#%d", nodeID, i)
		h := hashKey(vKey)
		r.hashToNode[h] = nodeID
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

// RemoveNode removes a physical node and all its virtual points from the
// ring. Used during self-healing when a node is declared dead.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.nodes[nodeID] {
		return
	}
	delete(r.nodes, nodeID)
	newHashes := r.sortedHashes[:0:0]
	for _, h := range r.sortedHashes {
		if r.hashToNode[h] == nodeID {
			delete(r.hashToNode, h)
			continue
		}
		newHashes = append(newHashes, h)
	}
	r.sortedHashes = newHashes
}

// Nodes returns the current set of physical node IDs on the ring.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Get returns the node responsible for a given key (the first node found
// walking clockwise from the key's hash position).
func (r *Ring) Get(key string) (string, bool) {
	nodes := r.GetN(key, 1)
	if len(nodes) == 0 {
		return "", false
	}
	return nodes[0], true
}

// GetN returns up to n distinct physical nodes responsible for key, walking
// clockwise around the ring starting at the key's hash. The first entry is
// the primary owner; subsequent entries are replica targets. This is the
// core operation used by the replication package to decide where a chunk
// and its replicas should live.
func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 || n <= 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})

	seen := make(map[string]bool)
	result := make([]string, 0, n)
	for i := 0; i < len(r.sortedHashes) && len(result) < n && len(seen) < len(r.nodes); i++ {
		pos := (idx + i) % len(r.sortedHashes)
		node := r.hashToNode[r.sortedHashes[pos]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}
