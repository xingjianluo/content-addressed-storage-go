package network

import (
	"context"
	"testing"
	"time"
)

// TestBackgroundRepairConvergesAfterReturn: with the repair loop running,
// a node that was down while its copy was deleted converges back to the
// replica set shortly after returning, with no client intervention.
func TestBackgroundRepairConvergesAfterReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	net, _ := newTestNetwork(t, 4)
	opt := Options{Replication: 3}
	c := chunkFor("bg-repair")
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	preferred := net.preferredNodes(c.ID, 3)

	loopDone := make(chan struct{})
	go func() {
		_ = net.RepairLoop(ctx, 10*time.Millisecond, opt)
		close(loopDone)
	}()

	// Simulate data loss while the node is away: delete its local copy
	// first (disk loss), then take it offline for a while.
	if err := preferred[0].Delete(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	preferred[0].SetAvailable(false)
	time.Sleep(30 * time.Millisecond)
	preferred[0].SetAvailable(true)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		healthy, names := countHealthy(t, ctx, preferred, c.ID)
		if healthy == 3 {
			t.Logf("id=%s converged-nodes=%v basis=background-loop-restored-verified-copy", c.ID, names)
			cancel()
			<-loopDone
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	cancel()
	<-loopDone
	t.Fatalf("background repair did not converge: id=%s", c.ID)
}
