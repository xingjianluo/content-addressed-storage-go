package network

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

// TestConcurrentWritesSameContent: many goroutines writing the same chunk
// concurrently (while one node flaps) must yield a consistent conclusion:
// every successful write reports the replica factor, all copies hash to
// the same ID, and no confirmed copy is ever lost.
func TestConcurrentWritesSameContent(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	c := chunkFor("hot-content")
	opt := Options{Replication: 3}
	preferred := net.preferredNodes(c.ID, 3)

	// Baseline confirmed write before any outage: this copy must survive.
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatalf("baseline write: %v", err)
	}

	stop := make(chan struct{})
	flapWG := sync.WaitGroup{}
	flapWG.Add(1)
	go func() {
		defer flapWG.Done()
		up := true
		for {
			select {
			case <-stop:
				preferred[2].SetAvailable(true)
				return
			default:
				up = !up
				preferred[2].SetAvailable(up)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	const writers = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, failed := 0, 0
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acked, err := net.PutChunk(ctx, c, opt)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed++
				// A failed write must never overstate: acked < RF.
				if acked >= opt.Replication {
					t.Errorf("failed write overstated replicas: %d err=%v", acked, err)
				}
				return
			}
			if acked != opt.Replication {
				t.Errorf("successful write reported %d replicas, want %d", acked, opt.Replication)
			}
			success++
		}()
	}
	wg.Wait()
	close(stop)
	flapWG.Wait()

	// The baseline-confirmed data must remain readable throughout:
	// a concurrent failed write never removes or corrupts existing copies.
	if got, err := net.GetChunk(ctx, c.ID, opt); err != nil || got.ID != c.ID {
		t.Fatalf("confirmed baseline write lost: err=%v got=%s", err, got.ID)
	}

	// Eventually bring the flapping node back and repair to convergence.
	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}
	healthy, names := countHealthy(t, ctx, preferred, c.ID)
	if healthy != 3 {
		t.Fatalf("post-flap repair did not converge to 3: healthy=%d", healthy)
	}
	t.Logf("id=%s concurrent-writers=%d success=%d failed=%d final-nodes=%v basis=verified-copies+repair-convergence",
		c.ID, writers, success, failed, names)
}

// TestConcurrentReadWriteMix: readers run while chunks are read and one
// node flaps; readers must only ever receive data that hashes to the
// requested ID, and after repair every chunk converges to its replica set.
func TestConcurrentReadWriteMix(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3}

	chunks := make([]chunk.Chunk, 0, 12)
	for i := 0; i < 12; i++ {
		c := chunkFor(fmt.Sprintf("mixed-%d", i))
		chunks = append(chunks, c)
		if _, err := net.PutChunk(ctx, c, opt); err != nil {
			t.Fatalf("seed write %d: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var flapWG sync.WaitGroup
	flapWG.Add(1)
	go func() {
		defer flapWG.Done()
		nd := net.sorted[3]
		up := true
		for {
			select {
			case <-stop:
				nd.SetAvailable(true)
				return
			default:
				up = !up
				nd.SetAvailable(up)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for round := 0; round < 40; round++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, c := range chunks {
				got, err := net.GetChunk(ctx, c.ID, opt)
				if err != nil {
					// With RF=3 and only one flapping node, some copy must serve.
					t.Errorf("unexpected read error for %s: %v", c.ID, err)
					return
				}
				if got.ID != c.ID || string(got.Data) != string(c.Data) {
					t.Errorf("integrity violated: wanted %s got %s", c.ID, got.ID)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	flapWG.Wait()

	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}
	for _, c := range chunks {
		preferred := net.preferredNodes(c.ID, 3)
		healthy, names := countHealthy(t, ctx, preferred, c.ID)
		if healthy != 3 {
			t.Fatalf("chunk %s converged to %d on %v", c.ID, healthy, names)
		}
	}
	t.Logf("chunks=%d readers=40 basis=every-read-hash-verified-then-repair-to-rf3", len(chunks))
}
