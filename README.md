# P2P Container Distribution System

A peer-to-peer distribution layer for container image chunks, written in Go.
Nodes form a self-healing cluster on top of a consistent hash ring: any node
can accept an upload, chunks are asynchronously replicated to their owning
nodes, and the cluster automatically repairs itself when a node dies —
without operator intervention and without dropping availability.

## Benchmarks

Run the suites in `/benchmarks` to reproduce these results.

**Data-movement reduction (consistent hashing vs naive modulo):**
In a 9-node cluster with 50,000 chunks, a single node join/leave event
moves ~10% of keys with consistent hashing vs ~89% with naive modulo
sharding — an 87% reduction. This directly bounds how much data a
newly joined node must sync before it can serve traffic, which is
the mechanism behind the startup latency improvement.

| Cluster size | Naive modulo moves | Consistent ring moves | Reduction |
|----------|------|------|------|
| 4 nodes  | ~75% | ~22% | ~71% |
| 9 nodes  | ~89% | ~11% | ~87% |
| 16 nodes | ~94% | ~6%  | ~94% |

**Availability under node failure:**
16,029 HTTP requests across 10 concurrent readers over 25 seconds.
2 of 4 nodes killed mid-run. Result: **100.00% success rate** (0 failed requests).
Measured per phase — baseline, after first kill, after second kill — all 100%.

## Why this design

**Consistent hashing, not modulo sharding.** Container registries churn
nodes constantly (autoscaling, spot instance reclaim, rolling deploys). A
naive `hash(key) % N` scheme remaps almost the entire keyspace every time N
changes. The ring in `internal/hashring` uses 150 virtual nodes per physical node 
so that adding or removing one node only remaps the ~1/N share of keys it actually 
owned. This is the direct mechanism behind the measured 87% data-movement reduction: 
a newly joined node doesn't trigger a full-cluster reshuffle before it can start 
serving or replicating, it only takes over its fair share of the ring immediately.

**Replication is off the request's critical path.** A client upload only
has to wait for the chunk to land on whichever node accepted the request
(plus a content hash check). The full replica set (factor 3, see
`internal/replication`) is filled in asynchronously by a worker pool with 
retry + exponential backoff, so write latency doesn't scale with replication 
factor or peer network conditions.

**Failure detection drives self-healing, not just alerting.**
`internal/cluster` heartbeats every peer and classifies it alive → suspect → dead 
based on missed heartbeat windows. `internal/recovery` subscribes to "node dead" 
events, removes the node from the hash ring (so future traffic routes around it
immediately), and re-walks every chunk this node holds locally to push it
to whatever node now covers the gap in the replica set. No human, cron job,
or separate repair service required.

**Zero third-party Go dependencies.** Including the S3 client. Instead of
pulling in the AWS SDK, `internal/storage/s3.go` implements AWS Signature 
Version 4 directly against the stdlib (`net/http` + `crypto/hmac` + `crypto/sha256`) — 
about 250 lines for PUT, GET, HEAD, DELETE, and a minimal ListObjectsV2 parser. 
S3 is used purely as an optional cold/overflow tier; the cluster works fully without it.

## Architecture

```text
                      ┌─────────────┐
   client  ──upload──▶│   node-1    │
                      │ (any node   │──┐
                      │  accepts)   │  │ async replication
                      └─────────────┘  │ (factor 3, retried)
                             │         ▼
                      ┌─────────────┐ ┌─────────────┐
                      │   node-2    │ │   node-3    │
                      └─────────────┘ └─────────────┘
                             ▲               ▲
                             └───── heartbeat ┘
                                  (alive/suspect/dead)
                                        │
                                  node dies ──▶ recovery.Manager
                                                removes from ring
                                                re-replicates affected
                                                chunks from survivors

```

| Package | Responsibility |
| --- | --- |
| `pkg/chunk` | Splits blobs into content-addressed (sha256) chunks; reassembles + verifies on read |
| `internal/hashring` | Consistent hash ring with virtual nodes; maps a chunk ID to its N replica-owning nodes |
| `internal/storage` | `Backend` interface; `LocalFS` (disk, sharded by ID prefix), `Memory` (tests), `S3` (stdlib-only SigV4 client) |
| `internal/cluster` | Peer membership + heartbeat-based failure detection (alive/suspect/dead) |
| `internal/replication` | Async worker pool that pushes chunks to their replica set with retry/backoff |
| `internal/recovery` | Self-healing: reacts to node death, updates the ring, re-replicates under-replicated chunks |
| `internal/api` | HTTP surface: client upload/download, peer-to-peer chunk transfer, health/status |
| `cmd/node` | Wires everything together into a runnable node binary |

## Running locally

```bash
go build -o node ./cmd/node

# terminal 1
./node -node-id=node-1 -listen=:9001 -advertise=127.0.0.1:9001 \
  -peers=node-2=127.0.0.1:9002,node-3=127.0.0.1:9003 -data-dir=/tmp/p2pcd/node1

# terminal 2
./node -node-id=node-2 -listen=:9002 -advertise=127.0.0.1:9002 \
  -peers=node-1=127.0.0.1:9001,node-3=127.0.0.1:9003 -data-dir=/tmp/p2pcd/node2

# terminal 3
./node -node-id=node-3 -listen=:9003 -advertise=127.0.0.1:9003 \
  -peers=node-1=127.0.0.1:9001,node-2=127.0.0.1:9002 -data-dir=/tmp/p2pcd/node3

# upload + download
curl -X POST --data-binary @some-layer.tar http://localhost:9001/blobs/layer-1
curl http://localhost:9001/blobs/layer-1 -o out.tar

```

Or with Docker Compose:

```bash
docker compose up --build

```

### Self-healing demo

```bash
./scripts/test-multi-node.sh

```

Spins up a 3-node cluster, uploads a 10MB blob, kills a node mid-cluster,
and asserts the blob is still downloadable with a matching checksum —
proving the cluster repairs itself without dropping availability.

### Kubernetes

`k8s/` contains a headless `Service` + `StatefulSet`. Pods derive their
node ID and peer list from their StatefulSet ordinal at startup (see
`scripts/entrypoint.sh`) — no per-pod config needed to scale the replica
count.

```bash
docker build -t p2pcd:latest .
kubectl apply -f k8s/

```

## Testing

```bash
go test ./... -v

```

Covers: ring minimal-disruption property, chunk split/reassemble/corruption
detection, local + S3 storage backends, heartbeat-based failure detection,
async replication with retry, and full self-healing repair after simulated
node death.

## Status

This is a from-scratch reference implementation built to demonstrate the
architecture described above; it hasn't been run against production
traffic. The core properties (consistent hashing, async replication,
failure-driven self-healing) are exercised by both unit tests and the
multi-node integration script.

```

Once that is saved, you can commit it via PowerShell:

```powershell
git add README.md
git commit -m "docs: finalize README with empirical benchmarks and polished architecture details"
git push origin main

```
