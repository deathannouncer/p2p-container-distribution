// Command rebalance-bench answers a concrete question: when a node joins
// or leaves the cluster, how much chunk data has to move before the
// cluster is back to a fully-replicated, consistent state -- and what does
// that cost in modeled transfer time?
//
// It compares two placement strategies head-to-head over the same
// synthetic chunk set:
//   - naive: node = hash(chunkID) % N        (what a lot of from-scratch
//     systems reach for first)
//   - consistent: internal/hashring.Ring      (what this project uses)
//
// This is the actual mechanism behind the "reduced startup latency"
// claim: less data that must move = less time before a newly joined node
// is fully synced and able to safely serve/replicate. The output is a
// real measured percentage from real code, not an assumed number.
package main

import (
	"crypto/sha1"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/vedansh/p2p-container-distribution/internal/hashring"
)

func naiveOwner(chunkID string, n int) int {
	sum := sha1.Sum([]byte(chunkID))
	h := binary.BigEndian.Uint64(sum[:8])
	return int(h % uint64(n))
}

type result struct {
	scenario       string
	totalKeys      int
	movedNaive     int
	movedConsist   int
	pctMovedNaive  float64
	pctMovedConsis float64
	reductionPct   float64
	// Modeled transfer time: assume a fixed per-chunk cost (typical for a
	// 4MB chunk over a real network link) to convert "keys moved" into a
	// concrete latency number, exactly as a node-join would have to
	// transfer that many chunks before being fully synced.
	perChunkCost   time.Duration
	timeNaive      time.Duration
	timeConsistent time.Duration
}

func runScenario(scenario string, totalKeys, nodesBefore, nodesAfter int, perChunkCost time.Duration, removed string) result {
	keys := make([]string, totalKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("chunk-%d-%d", totalKeys, i)
	}

	// --- naive modulo ---
	beforeNaive := make([]int, totalKeys)
	for i, k := range keys {
		beforeNaive[i] = naiveOwner(k, nodesBefore)
	}
	movedNaive := 0
	for i, k := range keys {
		if naiveOwner(k, nodesAfter) != beforeNaive[i] {
			movedNaive++
		}
	}

	// --- consistent hash ring ---
	ring := hashring.New()
	for i := 0; i < nodesBefore; i++ {
		ring.AddNode(fmt.Sprintf("node-%d", i))
	}
	beforeConsistent := make([]string, totalKeys)
	for i, k := range keys {
		owner, _ := ring.Get(k)
		beforeConsistent[i] = owner
	}
	if nodesAfter > nodesBefore {
		ring.AddNode(fmt.Sprintf("node-%d", nodesBefore)) // node join
	} else if nodesAfter < nodesBefore {
		ring.RemoveNode(removed) // node leave
	}
	movedConsistent := 0
	for i, k := range keys {
		owner, _ := ring.Get(k)
		if owner != beforeConsistent[i] {
			movedConsistent++
		}
	}

	pctNaive := 100 * float64(movedNaive) / float64(totalKeys)
	pctConsistent := 100 * float64(movedConsistent) / float64(totalKeys)
	reduction := 0.0
	if movedNaive > 0 {
		reduction = 100 * (1 - float64(movedConsistent)/float64(movedNaive))
	}

	return result{
		scenario:       scenario,
		totalKeys:      totalKeys,
		movedNaive:     movedNaive,
		movedConsist:   movedConsistent,
		pctMovedNaive:  pctNaive,
		pctMovedConsis: pctConsistent,
		reductionPct:   reduction,
		perChunkCost:   perChunkCost,
		timeNaive:      time.Duration(movedNaive) * perChunkCost,
		timeConsistent: time.Duration(movedConsistent) * perChunkCost,
	}
}

func printResult(r result) {
	fmt.Printf("\n=== %s ===\n", r.scenario)
	fmt.Printf("total chunks tracked:      %d\n", r.totalKeys)
	fmt.Printf("naive modulo  -> moved:    %d (%.1f%% of keyspace)\n", r.movedNaive, r.pctMovedNaive)
	fmt.Printf("consistent ring -> moved:  %d (%.1f%% of keyspace)\n", r.movedConsist, r.pctMovedConsis)
	fmt.Printf("data-movement reduction:   %.1f%%\n", r.reductionPct)
	fmt.Printf("modeled cost/chunk:        %s\n", r.perChunkCost)
	fmt.Printf("modeled resync time naive:      %s\n", r.timeNaive)
	fmt.Printf("modeled resync time consistent: %s\n", r.timeConsistent)
	if r.timeNaive > 0 {
		latencyReduction := 100 * (1 - float64(r.timeConsistent)/float64(r.timeNaive))
		fmt.Printf("modeled startup-latency reduction: %.1f%%\n", latencyReduction)
	}
}

func main() {
	totalKeys := flag.Int("keys", 10000, "number of synthetic chunks to simulate")
	nodesBefore := flag.Int("nodes", 9, "cluster size before the join/leave event")
	perChunkMs := flag.Float64("per-chunk-ms", 8.0, "modeled milliseconds to transfer one 4MB chunk over the network (tune to your link speed)")
	flag.Parse()

	cost := time.Duration(*perChunkMs * float64(time.Millisecond))

	joinResult := runScenario(
		fmt.Sprintf("Node JOIN: %d -> %d nodes", *nodesBefore, *nodesBefore+1),
		*totalKeys, *nodesBefore, *nodesBefore+1, cost, "",
	)
	printResult(joinResult)

	leaveResult := runScenario(
		fmt.Sprintf("Node LEAVE (failure): %d -> %d nodes", *nodesBefore, *nodesBefore-1),
		*totalKeys, *nodesBefore, *nodesBefore-1, cost, "node-0",
	)
	printResult(leaveResult)

	avgReduction := (joinResult.reductionPct + leaveResult.reductionPct) / 2
	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("average data-movement reduction across join+leave: %.1f%%\n", avgReduction)
	fmt.Printf("(this is the real, measured number behind a \"reduced startup latency\" claim --\n")
	fmt.Printf(" it will vary with --keys and --nodes; it is NOT a fixed constant of the algorithm.)\n")

	// Sanity floor: with N virtual-node-equipped physical nodes, consistent
	// hashing's expected move fraction on a single join/leave is ~1/N,
	// vs naive's expected move fraction of ~(N-1)/N. Print the theoretical
	// expectation alongside the measured one so a reader can see this
	// wasn't cherry-picked.
	theoreticalConsistent := 100.0 / float64(*nodesBefore+1)
	theoreticalNaive := 100.0 * (1 - 1.0/float64(*nodesBefore+1))
	fmt.Printf("\ntheoretical expectation (join, %d->%d nodes):\n", *nodesBefore, *nodesBefore+1)
	fmt.Printf("  naive ~%.1f%% of keys move, consistent ~%.1f%% of keys move\n", theoreticalNaive, theoreticalConsistent)
	_ = math.Abs // (kept for potential future variance checks)
}
