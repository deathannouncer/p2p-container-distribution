// Command node runs a single peer in the distributed container chunk
// distribution cluster. Configuration is via environment variables (or
// flags, see below) so the same binary works unmodified across Docker
// Compose, bare metal, and the Kubernetes StatefulSet in k8s/.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vedansh/p2p-container-distribution/internal/api"
	"github.com/vedansh/p2p-container-distribution/internal/cluster"
	"github.com/vedansh/p2p-container-distribution/internal/hashring"
	"github.com/vedansh/p2p-container-distribution/internal/recovery"
	"github.com/vedansh/p2p-container-distribution/internal/replication"
	"github.com/vedansh/p2p-container-distribution/internal/storage"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	var (
		nodeID     = flag.String("node-id", envOr("NODE_ID", "node-1"), "unique ID for this node")
		listenAddr = flag.String("listen", envOr("LISTEN_ADDR", ":8080"), "address this node's HTTP API listens on")
		advertise  = flag.String("advertise", envOr("ADVERTISE_ADDR", "localhost:8080"), "address other nodes use to reach this node")
		peersFlag  = flag.String("peers", envOr("PEERS", ""), "comma-separated list of id=addr peers, e.g. node-2=host2:8080,node-3=host3:8080")
		dataDir    = flag.String("data-dir", envOr("DATA_DIR", "/var/lib/p2pcd/data"), "local chunk storage directory")
		s3Bucket   = flag.String("s3-bucket", envOr("S3_BUCKET", ""), "optional S3 bucket for cold/overflow storage")
		s3Region   = flag.String("s3-region", envOr("AWS_REGION", "us-east-1"), "AWS region for the S3 bucket")
		heartbeat  = flag.Duration("heartbeat-interval", 1*time.Second, "interval between heartbeat rounds")
		repWorkers = flag.Int("replication-workers", 4, "number of concurrent replication workers")
	)
	flag.Parse()

	ring := hashring.New()
	ring.AddNode(*nodeID)

	local, err := storage.NewLocalFS(*dataDir)
	if err != nil {
		log.Fatalf("failed to init local storage: %v", err)
	}

	var cold storage.Backend
	if *s3Bucket != "" {
		cold = storage.NewS3(
			*s3Bucket, *s3Region,
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			os.Getenv("AWS_SESSION_TOKEN"),
		)
		log.Printf("node %s: S3 cold tier enabled (bucket=%s region=%s)", *nodeID, *s3Bucket, *s3Region)
	}

	membership := cluster.New(*nodeID, *advertise)
	for _, entry := range strings.Split(*peersFlag, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			log.Printf("skipping malformed peer entry: %q", entry)
			continue
		}
		id, addr := parts[0], parts[1]
		membership.AddPeer(id, addr)
		ring.AddNode(id)
	}

	resolver := membershipResolver{membership}
	transport := replication.NewHTTPTransport()
	engine := replication.NewEngine(ring, local, resolver, transport, *nodeID, *repWorkers)

	rec := recovery.New(ring, local, engine, *nodeID)
	membership.OnNodeDead(rec.HandleNodeDead)
	membership.OnNodeAlive(func(id string) {
		addr, _ := resolver.AddrFor(id)
		rec.HandleNodeAlive(id, addr)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.Start(ctx)
	membership.Start(*heartbeat)

	server := api.NewServer(*nodeID, ring, local, cold, membership, engine, rec)
	mux := http.NewServeMux()
	server.Routes(mux)

	httpServer := &http.Server{Addr: *listenAddr, Handler: mux}

	go func() {
		log.Printf("node %s listening on %s (advertised as %s)", *nodeID, *listenAddr, *advertise)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("node %s shutting down", *nodeID)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)
	membership.Stop()
	cancel()
	engine.Stop()
}

// membershipResolver adapts cluster.Membership to replication.AddrResolver.
type membershipResolver struct {
	m *cluster.Membership
}

func (r membershipResolver) AddrFor(nodeID string) (string, bool) {
	for _, p := range r.m.Peers() {
		if p.ID == nodeID {
			return p.Addr, true
		}
	}
	return "", false
}
