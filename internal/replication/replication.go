// Package replication handles getting a chunk's replicas onto their target
// nodes asynchronously, off the hot path of the client's upload request.
package replication

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/vedansh/p2p-container-distribution/internal/hashring"
	"github.com/vedansh/p2p-container-distribution/internal/storage"
)

// Factor is the number of copies of each chunk kept across the cluster
// (1 primary + Factor-1 replicas).
const Factor = 3

// Job describes one chunk that needs to reach its replica set.
type Job struct {
	ChunkID string
	Data    []byte
}

// Transport abstracts how a chunk is pushed to a remote peer, so it can be
// swapped for a fake in tests instead of doing real HTTP calls.
type Transport interface {
	PushChunk(ctx context.Context, peerAddr, chunkID string, data []byte) error
}

// HTTPTransport pushes chunks to peers over the node HTTP API
// (PUT /internal/chunks/{id}).
type HTTPTransport struct {
	Client *http.Client
}

func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{Client: &http.Client{Timeout: 15 * time.Second}}
}

func (t *HTTPTransport) PushChunk(ctx context.Context, peerAddr, chunkID string, data []byte) error {
	url := fmt.Sprintf("http://%s/internal/chunks/%s", peerAddr, chunkID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("replication: peer %s rejected chunk %s: status %d", peerAddr, chunkID, resp.StatusCode)
	}
	return nil
}

// AddrResolver maps a node ID (as returned by the hash ring) to its
// network address. Implemented by internal/cluster.Membership in practice.
type AddrResolver interface {
	AddrFor(nodeID string) (string, bool)
}

// Engine runs a background worker pool that asynchronously pushes chunks
// to their replica targets, with retry on failure. This is what lets the
// upload path return to the client as soon as the chunk is durable on the
// local/primary node, instead of blocking on every replica write.
type Engine struct {
	ring      *hashring.Ring
	local     storage.Backend
	resolver  AddrResolver
	transport Transport
	selfID    string

	queue   chan Job
	wg      sync.WaitGroup
	workers int

	maxRetries int
	retryBase  time.Duration
}

// NewEngine builds a replication engine. selfID is this node's ID on the
// ring -- the engine skips pushing to itself since the chunk is already
// stored locally by the caller before enqueueing.
func NewEngine(ring *hashring.Ring, local storage.Backend, resolver AddrResolver, transport Transport, selfID string, workers int) *Engine {
	if workers <= 0 {
		workers = 4
	}
	return &Engine{
		ring:       ring,
		local:      local,
		resolver:   resolver,
		transport:  transport,
		selfID:     selfID,
		queue:      make(chan Job, 1024),
		workers:    workers,
		maxRetries: 5,
		retryBase:  200 * time.Millisecond,
	}
}

// Start launches the worker pool.
func (e *Engine) Start(ctx context.Context) {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx)
	}
}

// Stop closes the queue and waits for in-flight jobs to drain.
func (e *Engine) Stop() {
	close(e.queue)
	e.wg.Wait()
}

// Enqueue schedules a chunk for async replication to its target nodes.
// Non-blocking unless the queue is completely full, in which case it
// applies backpressure rather than silently dropping replication work.
func (e *Engine) Enqueue(chunkID string, data []byte) {
	e.queue <- Job{ChunkID: chunkID, Data: data}
}

// TargetsFor returns the full replica set (node IDs) for a chunk,
// including whichever of those is this node itself.
func (e *Engine) TargetsFor(chunkID string) []string {
	return e.ring.GetN(chunkID, Factor)
}

func (e *Engine) worker(ctx context.Context) {
	defer e.wg.Done()
	for job := range e.queue {
		e.replicate(ctx, job)
	}
}

func (e *Engine) replicate(ctx context.Context, job Job) {
	targets := e.ring.GetN(job.ChunkID, Factor)
	for _, nodeID := range targets {
		if nodeID == e.selfID {
			continue // already stored locally by the request handler
		}
		addr, ok := e.resolver.AddrFor(nodeID)
		if !ok {
			log.Printf("replication: no address known for node %s, skipping chunk %s", nodeID, job.ChunkID)
			continue
		}
		e.pushWithRetry(ctx, addr, job)
	}
}

func (e *Engine) pushWithRetry(ctx context.Context, addr string, job Job) {
	var err error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := e.retryBase * time.Duration(1<<uint(attempt-1))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		err = e.transport.PushChunk(ctx, addr, job.ChunkID, job.Data)
		if err == nil {
			return
		}
	}
	log.Printf("replication: giving up on chunk %s -> %s after %d attempts: %v", job.ChunkID, addr, e.maxRetries+1, err)
}

// QueueDepth reports the current number of pending replication jobs, used
// for /metrics and operational visibility.
func (e *Engine) QueueDepth() int {
	return len(e.queue)
}
