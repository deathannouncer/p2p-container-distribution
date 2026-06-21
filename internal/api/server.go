// Package api exposes the node's HTTP surface: client-facing blob
// upload/download, and internal endpoints peers use for replication,
// health checks, and gossip.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/vedansh/p2p-container-distribution/internal/cluster"
	"github.com/vedansh/p2p-container-distribution/internal/hashring"
	"github.com/vedansh/p2p-container-distribution/internal/recovery"
	"github.com/vedansh/p2p-container-distribution/internal/replication"
	"github.com/vedansh/p2p-container-distribution/internal/storage"
	"github.com/vedansh/p2p-container-distribution/pkg/chunk"
)

// Server holds everything an HTTP handler needs to serve client and peer
// requests for this node.
type Server struct {
	SelfID     string
	Ring       *hashring.Ring
	Local      storage.Backend
	Cold       storage.Backend // optional S3 overflow tier; may be nil
	Membership *cluster.Membership
	Engine     *replication.Engine
	Recovery   *recovery.Manager

	manifests map[string]*chunk.Manifest // in-memory manifest index, blobID -> manifest
}

func NewServer(selfID string, ring *hashring.Ring, local, cold storage.Backend,
	membership *cluster.Membership, engine *replication.Engine, rec *recovery.Manager) *Server {
	return &Server{
		SelfID:     selfID,
		Ring:       ring,
		Local:      local,
		Cold:       cold,
		Membership: membership,
		Engine:     engine,
		Recovery:   rec,
		manifests:  make(map[string]*chunk.Manifest),
	}
}

// Routes registers all handlers on mux.
func (s *Server) Routes(mux *http.ServeMux) {
	// Client-facing
	mux.HandleFunc("POST /blobs/", s.handleUploadBlob)
	mux.HandleFunc("GET /blobs/", s.handleDownloadBlob)

	// Internal peer-to-peer
	mux.HandleFunc("PUT /internal/chunks/{id}", s.handlePutChunk)
	mux.HandleFunc("GET /internal/chunks/{id}", s.handleGetChunk)

	// Cluster + ops
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /status", s.handleStatus)
}

// handleUploadBlob accepts a raw blob body, splits it into chunks, stores
// the chunks this node is primary for locally, and enqueues async
// replication for the rest of each chunk's replica set. Returns the
// manifest so the client can later fetch the blob.
func (s *Server) handleUploadBlob(w http.ResponseWriter, r *http.Request) {
	blobID := strings.TrimPrefix(r.URL.Path, "/blobs/")
	if blobID == "" {
		http.Error(w, "missing blob id in path", http.StatusBadRequest)
		return
	}

	chunks, manifest, err := chunk.Split(blobID, r.Body, chunk.DefaultSize)
	if err != nil {
		http.Error(w, fmt.Sprintf("split failed: %v", err), http.StatusBadRequest)
		return
	}

	for _, c := range chunks {
		targets := s.Ring.GetN(c.ID, replication.Factor)
		isLocalTarget := false
		for _, t := range targets {
			if t == s.SelfID {
				isLocalTarget = true
				break
			}
		}
		if isLocalTarget || len(targets) == 0 {
			if err := s.Local.Put(c.ID, c.Data); err != nil {
				log.Printf("api: failed to store chunk %s locally: %v", c.ID, err)
				http.Error(w, "storage failure", http.StatusInternalServerError)
				return
			}
		}
		// Always enqueue -- the engine knows to skip pushing to itself, and
		// pushing even when we're not a "primary" target keeps things
		// simple and correct: every node that touches a chunk during
		// upload makes sure the full replica set converges.
		s.Engine.Enqueue(c.ID, c.Data)
	}

	s.manifests[blobID] = manifest

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(manifest)
}

// handleDownloadBlob reassembles a previously uploaded blob from its
// chunks, fetching from local storage, then peers, then the S3 cold tier
// as a last resort.
func (s *Server) handleDownloadBlob(w http.ResponseWriter, r *http.Request) {
	blobID := strings.TrimPrefix(r.URL.Path, "/blobs/")
	manifest, ok := s.manifests[blobID]
	if !ok {
		http.Error(w, "unknown blob id", http.StatusNotFound)
		return
	}

	err := chunk.Reassemble(w, manifest, func(chunkID string) ([]byte, error) {
		return s.fetchChunk(chunkID)
	})
	if err != nil {
		log.Printf("api: reassembly failed for blob %s: %v", blobID, err)
	}
}

// fetchChunk tries local storage first, then walks the chunk's replica set
// over the network, then finally falls back to the S3 cold tier if
// configured. This fallback chain is what keeps reads available even when
// some replica nodes are down.
func (s *Server) fetchChunk(chunkID string) ([]byte, error) {
	if data, err := s.Local.Get(chunkID); err == nil {
		return data, nil
	}

	for _, nodeID := range s.Ring.GetN(chunkID, replication.Factor) {
		if nodeID == s.SelfID {
			continue
		}
		addr, ok := addrForNode(s.Membership, nodeID)
		if !ok {
			continue
		}
		resp, err := http.Get(fmt.Sprintf("http://%s/internal/chunks/%s", addr, chunkID))
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				return data, nil
			}
		}
		resp.Body.Close()
	}

	if s.Cold != nil {
		if data, err := s.Cold.Get(chunkID); err == nil {
			return data, nil
		}
	}

	return nil, storage.ErrNotFound
}

func addrForNode(m *cluster.Membership, nodeID string) (string, bool) {
	for _, p := range m.Peers() {
		if p.ID == nodeID && p.State == cluster.StateAlive {
			return p.Addr, true
		}
	}
	return "", false
}

// handlePutChunk accepts a replicated chunk push from a peer.
func (s *Server) handlePutChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	if !chunk.Verify(id, data) {
		http.Error(w, "chunk integrity check failed", http.StatusBadRequest)
		return
	}
	if err := s.Local.Put(id, data); err != nil {
		http.Error(w, "storage failure", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleGetChunk serves a chunk to a requesting peer (used both for client
// reassembly fallback and for repair flows).
func (s *Server) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	data, err := s.Local.Get(id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Write(data)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"node_id":       s.SelfID,
		"ring_nodes":    s.Ring.Nodes(),
		"replication_q": s.Engine.QueueDepth(),
		"recovery":      s.Recovery.Status(),
		"peers":         s.Membership.Peers(),
	})
}
