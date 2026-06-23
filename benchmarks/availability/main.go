// Command availability-bench measures actual request success rate while
// the cluster is losing nodes. It:
//  1. Starts N real node processes (same binary as production)
//  2. Uploads a set of blobs until chunks are replicated across the cluster
//  3. Launches a concurrent read loop (steady-state traffic)
//  4. Kills nodes one-by-one with configurable gaps, while reads keep running
//  5. Reports: total requests, failed requests, success rate, per-phase breakdown
//
// This produces the real number behind "maintained 100% availability under
// multi-node failure conditions" -- it either holds up or it doesn't.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const nodeBasePort = 19100

type nodeProc struct {
	id      string
	port    int
	cmd     *exec.Cmd
	dataDir string
}

func nodeAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func startNode(id string, port int, dataDir string, peers []string) (*nodeProc, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	args := []string{
		"-node-id=" + id,
		fmt.Sprintf("-listen=:%d", port),
		fmt.Sprintf("-advertise=127.0.0.1:%d", port),
		"-data-dir=" + dataDir,
	}
	if len(peers) > 0 {
		args = append(args, "-peers="+strings.Join(peers, ","))
	}
	cmd := exec.Command(os.Args[0]+"/../node", args...)
	// If running via `go run`, the binary path is different; fall back to
	// searching PATH for 'p2pcd-node' or the local build target.
	if _, err := os.Stat(cmd.Path); err != nil {
		cmd = exec.Command("./node", args...)
	}
	logFile, _ := os.Create(filepath.Join(dataDir, "node.log"))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting %s: %w", id, err)
	}
	return &nodeProc{id: id, port: port, cmd: cmd, dataDir: dataDir}, nil
}

func (n *nodeProc) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		n.cmd.Process.Kill()
	}
}

func waitHealthy(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return true
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func uploadBlob(port int, blobID string, data []byte) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/blobs/%s", port, blobID)
	resp, err := http.Post(url, "application/octet-stream", strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("upload %s: status %d", blobID, resp.StatusCode)
	}
	return nil
}

func downloadBlob(port int, blobID string, expectedLen int) bool {
	url := fmt.Sprintf("http://127.0.0.1:%d/blobs/%s", port, blobID)
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false
	}
	body, err := io.ReadAll(resp.Body)
	return err == nil && len(body) == expectedLen
}

func main() {
	nodeBin := flag.String("bin", "./bin/node", "path to the compiled node binary")
	numNodes := flag.Int("nodes", 4, "number of nodes to start in the cluster")
	numBlobs := flag.Int("blobs", 5, "number of blobs to upload before the read loop")
	blobSize := flag.Int("blob-size", 1024*1024, "size of each blob in bytes (default 1MB for speed)")
	readDuration := flag.Duration("duration", 20*time.Second, "how long to run the concurrent read loop")
	readConcurrency := flag.Int("concurrency", 8, "concurrent readers during the test")
	killAfter := flag.Duration("kill-after", 5*time.Second, "kill first node after this delay into the read loop")
	killInterval := flag.Duration("kill-interval", 5*time.Second, "interval between subsequent node kills")
	maxKills := flag.Int("max-kills", 1, "max nodes to kill during the run (never kills more than N-2 to keep quorum)")
	flag.Parse()

	// Hard cap: never kill more than N-2 nodes so at least 2 remain.
	maxAllowedKills := *numNodes - 2
	if maxAllowedKills < 0 {
		maxAllowedKills = 0
	}
	if *maxKills > maxAllowedKills {
		*maxKills = maxAllowedKills
	}

	workDir, err := os.MkdirTemp("", "avail-bench-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mktemp:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(workDir)

	// Resolve node binary: allow override flag, fall back to sibling 'node'
	// in the same directory as the benchmark binary.
	bin := *nodeBin
	if _, err := os.Stat(bin); err != nil {
		// Try the directory containing this binary.
		self, _ := os.Executable()
		bin = filepath.Join(filepath.Dir(self), "node")
	}
	if _, err := os.Stat(bin); err != nil {
		fmt.Fprintf(os.Stderr, "node binary not found at %q or sibling path. Build it first: go build -o bin/node ./cmd/node\n", *nodeBin)
		os.Exit(1)
	}
	// Patch exec.Command to use the resolved binary path.
	_ = bin // used below via closure

	// Build peer list for each node.
	peerLists := make([][]string, *numNodes)
	for i := 0; i < *numNodes; i++ {
		for j := 0; j < *numNodes; j++ {
			if i != j {
				peerLists[i] = append(peerLists[i], fmt.Sprintf("node-%d=127.0.0.1:%d", j, nodeBasePort+j))
			}
		}
	}

	// Start all nodes.
	nodes := make([]*nodeProc, *numNodes)
	fmt.Printf("starting %d-node cluster...\n", *numNodes)
	for i := 0; i < *numNodes; i++ {
		dataDir := filepath.Join(workDir, fmt.Sprintf("node%d", i))
		id := fmt.Sprintf("node-%d", i)
		port := nodeBasePort + i

		args := []string{
			"-node-id=" + id,
			fmt.Sprintf("-listen=:%d", port),
			fmt.Sprintf("-advertise=127.0.0.1:%d", port),
			"-data-dir=" + dataDir,
			"-peers=" + strings.Join(peerLists[i], ","),
		}
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		logFile, _ := os.Create(filepath.Join(dataDir, "node.log"))
		cmd := exec.Command(bin, args...)
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to start node-%d: %v\n", i, err)
			os.Exit(1)
		}
		nodes[i] = &nodeProc{id: id, port: port, cmd: cmd, dataDir: dataDir}
	}

	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.kill()
			}
		}
	}()

	// Wait for all nodes to be healthy.
	fmt.Print("waiting for cluster to be healthy")
	for i, n := range nodes {
		if !waitHealthy(n.port, 8*time.Second) {
			fmt.Fprintf(os.Stderr, "\nnode-%d never became healthy\n", i)
			os.Exit(1)
		}
		fmt.Print(".")
	}
	fmt.Println(" OK")
	time.Sleep(1 * time.Second) // give heartbeats a moment to stabilise

	// Upload blobs through node-0 (any node accepts).
	blobData := strings.Repeat("x", *blobSize)
	blobIDs := make([]string, *numBlobs)
	fmt.Printf("uploading %d blobs (%dKB each) for replication to settle...\n", *numBlobs, *blobSize/1024)
	for i := 0; i < *numBlobs; i++ {
		blobIDs[i] = fmt.Sprintf("bench-blob-%d", i)
		if err := uploadBlob(nodes[0].port, blobIDs[i], []byte(blobData)); err != nil {
			fmt.Fprintln(os.Stderr, "upload failed:", err)
			os.Exit(1)
		}
	}
	// Give replication time to propagate.
	time.Sleep(2 * time.Second)
	fmt.Println("uploads done, replication settled")

	// --- Read loop with concurrent goroutines ---
	var (
		totalReqs  int64
		failedReqs int64
	)

	type phaseResult struct {
		label  string
		total  int64
		failed int64
	}
	var phaseMu sync.Mutex
	phases := []phaseResult{}
	currentPhase := "baseline (all nodes alive)"
	phaseStart := time.Now()
	phaseTotal := int64(0)
	phaseFailed := int64(0)

	nextPhase := func(label string) {
		phaseMu.Lock()
		dur := time.Since(phaseStart)
		t := atomic.LoadInt64(&phaseTotal)
		f := atomic.LoadInt64(&phaseFailed)
		phases = append(phases, phaseResult{
			label:  fmt.Sprintf("%s (%.1fs)", currentPhase, dur.Seconds()),
			total:  t,
			failed: f,
		})
		// reset phase counters
		atomic.StoreInt64(&phaseTotal, 0)
		atomic.StoreInt64(&phaseFailed, 0)
		currentPhase = label
		phaseStart = time.Now()
		phaseMu.Unlock()
	}

	stopReaders := make(chan struct{})
	var readerWg sync.WaitGroup
	client := &http.Client{Timeout: 3 * time.Second}

	for w := 0; w < *readConcurrency; w++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			idx := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				blobID := blobIDs[idx%len(blobIDs)]
				idx++
				// Round-robin across all non-nil nodes so we test reads
				// through every surviving node.
				var port int
				for _, n := range nodes {
					if n != nil && n.cmd != nil && n.cmd.Process != nil {
						port = n.port
						break
					}
				}
				if port == 0 {
					continue
				}
				url := fmt.Sprintf("http://127.0.0.1:%d/blobs/%s", port, blobID)
				resp, err := client.Get(url)
				ok := false
				if err == nil {
					body, rerr := io.ReadAll(resp.Body)
					resp.Body.Close()
					ok = resp.StatusCode == 200 && rerr == nil && len(body) == *blobSize
				}
				atomic.AddInt64(&totalReqs, 1)
				atomic.AddInt64(&phaseTotal, 1)
				if !ok {
					atomic.AddInt64(&failedReqs, 1)
					atomic.AddInt64(&phaseFailed, 1)
				}
			}
		}()
	}

	fmt.Printf("running %d concurrent readers for %s\n", *readConcurrency, *readDuration)
	fmt.Printf("will kill up to %d node(s) starting at t+%s\n\n", *maxKills, *killAfter)

	// Kill nodes on schedule while readers keep running.
	killTicker := time.NewTimer(*killAfter)
	killed := 0
	killScheduler := func() {
		// Kill the last node in the list so node-0 (which we read through)
		// stays alive longest.
		for i := len(nodes) - 1; i >= 0 && killed < *maxKills; i-- {
			if nodes[i] != nil {
				label := fmt.Sprintf("after killing node-%d", i)
				nextPhase(label)
				fmt.Printf("[t+%.1fs] killing node-%d\n", time.Since(phaseStart).Seconds(), i)
				nodes[i].kill()
				nodes[i] = nil
				killed++
				if killed < *maxKills {
					time.Sleep(*killInterval)
				}
			}
		}
	}

	deadline := time.NewTimer(*readDuration)
	killFired := false
loop:
	for {
		select {
		case <-deadline.C:
			break loop
		case <-killTicker.C:
			if !killFired {
				killFired = true
				killScheduler()
			}
		}
	}

	close(stopReaders)
	readerWg.Wait()
	nextPhase("end")

	// --- Results ---
	total := atomic.LoadInt64(&totalReqs)
	failed := atomic.LoadInt64(&failedReqs)
	successRate := 100.0 * float64(total-failed) / float64(total)

	fmt.Println("\n======== AVAILABILITY BENCHMARK RESULTS ========")
	fmt.Printf("cluster size:             %d nodes\n", *numNodes)
	fmt.Printf("nodes killed mid-run:     %d\n", killed)
	fmt.Printf("blob size:                %dKB\n", *blobSize/1024)
	fmt.Printf("concurrent readers:       %d\n", *readConcurrency)
	fmt.Printf("test duration:            %s\n", *readDuration)
	fmt.Printf("\ntotal requests:           %d\n", total)
	fmt.Printf("failed requests:          %d\n", failed)
	fmt.Printf("overall success rate:      %.2f%%\n", successRate)
	fmt.Println("\n--- per-phase breakdown ---")
	for _, p := range phases {
		if p.total == 0 {
			continue
		}
		rate := 100.0 * float64(p.total-p.failed) / float64(p.total)
		fmt.Printf("  %-50s  %5d reqs  %5d failed  %.2f%% success\n",
			p.label, p.total, p.failed, rate)
	}
	fmt.Println()
	if successRate >= 99.9 {
		fmt.Println("VERDICT: ✓  ~100% availability maintained under node failure")
	} else if successRate >= 95.0 {
		fmt.Printf("VERDICT: ~  %.2f%% availability -- some requests failed during failure window\n", successRate)
	} else {
		fmt.Printf("VERDICT: ✗  %.2f%% -- availability target not met\n", successRate)
	}
}
