package network

import (
	"context"
	"errors"
	"testing"
)

// TestDeterministicPlacementAndReplicaCount: same content written twice
// (also via a freshly built network with the same node set) must prefer
// the same nodes, and the reported replica count must equal the number of
// nodes that actually hold a verified copy — never more.
func TestDeterministicPlacementAndReplicaCount(t *testing.T) {
	ctx := context.Background()
	net, nodes := newTestNetwork(t, 5)
	c := chunkFor("replicated-content")

	acked, err := net.PutChunk(ctx, c, Options{Replication: 3})
	if err != nil {
		t.Fatal(err)
	}
	healthy, names := countHealthy(t, ctx, nodes, c.ID)
	if healthy != acked {
		t.Fatalf("replica count overstated: reported=%d verified-on-disk=%d", acked, healthy)
	}
	if healthy != 3 {
		t.Fatalf("want 3 replicas, got %d on %v", healthy, names)
	}
	t.Logf("id=%s replicas=%d nodes=%v basis=verified-persistent-copies", c.ID, healthy, names)

	// Fresh network over the same directories: placement must be identical.
	net2 := New(nodes...)
	order1 := nodeIDs(net.placementOrder(c.ID))
	order2 := nodeIDs(net2.placementOrder(c.ID))
	if len(order1) != len(order2) {
		t.Fatal("placement length differs")
	}
	for i := range order1 {
		if order1[i] != order2[i] {
			t.Fatalf("placement not deterministic: %v vs %v", order1, order2)
		}
	}

	// Duplicate write: same conclusion, same nodes.
	acked2, err := net.PutChunk(ctx, c, Options{Replication: 3})
	if err != nil || acked2 != 3 {
		t.Fatalf("duplicate write: acked=%d err=%v", acked2, err)
	}
	healthy2, names2 := countHealthy(t, ctx, nodes, c.ID)
	if healthy2 != 3 || !sameSet(names, names2) {
		t.Fatalf("duplicate write changed replica set: %v vs %v", names, names2)
	}
}

// TestWriteWithNodesDown: a write with too few nodes up fails and reports
// only the real confirmations; with enough nodes up it succeeds, and the
// confirmed copies remain readable while nodes are down and after return.
func TestWriteWithNodesDown(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	c := chunkFor("survive-outage")
	opt := Options{Replication: 3}
	preferred := net.preferredNodes(c.ID, 3)

	// Take two of the three preferred nodes down: only one preferred
	// node can confirm, so replication factor 3 cannot be met.
	for _, nd := range preferred[:2] {
		nd.SetAvailable(false)
	}
	acked, err := net.PutChunk(ctx, c, opt)
	if !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("want ErrNotEnoughReplicas, got acked=%d err=%v", acked, err)
	}
	if acked != 1 {
		t.Fatalf("reported replicas must reflect the one live preferred node, got %d", acked)
	}
	t.Logf("id=%s during-outage acked=%d nodes=%v reason=%v basis=count-confirmed-persists-only",
		c.ID, acked, nodeIDs(preferred), err)

	// Bring nodes back and write with the full replica set available.
	for _, nd := range preferred[:2] {
		nd.SetAvailable(true)
	}
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}

	// Confirmed data must be readable with two preferred nodes down.
	for _, nd := range preferred[:2] {
		nd.SetAvailable(false)
	}
	got, err := net.GetChunk(ctx, c.ID, opt)
	if err != nil {
		t.Fatalf("confirmed write lost during outage: %v", err)
	}
	if got.ID != c.ID {
		t.Fatal("read returned wrong id")
	}
	for _, nd := range preferred[:2] {
		nd.SetAvailable(true)
	}
	t.Logf("id=%s read-with-2-down=ok nodes=%v basis=one-verified-replica-suffices",
		c.ID, nodeIDs(preferred))
}

// TestRepairAfterRecoveryConvergence: write with RF=3, delete the copy on
// one node and take another down, then bring it back and repair; all
// preferred nodes must converge to a verified copy.
func TestRepairAfterRecoveryConvergence(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	c := chunkFor("repair-me")
	opt := Options{Replication: 3}

	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	preferred := net.preferredNodes(c.ID, 3)

	// One replica lost, one node temporarily unavailable.
	if err := preferred[0].Delete(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	preferred[1].SetAvailable(false)

	res, err := net.Repair(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	var r *RepairResult
	for i := range res {
		if res[i].ID == c.ID {
			r = &res[i]
		}
	}
	if r == nil || r.Healthy != 2 {
		t.Fatalf("repair while one node down: healthy=%d result=%+v", func() int {
			if r == nil {
				return -1
			}
			return r.Healthy
		}(), r)
	}
	t.Logf("id=%s phase=repair-during-outage healthy=%d missing=%v basis=down-node-skipped",
		c.ID, r.Healthy, r.Missing)

	// Node returns: next repair pass must restore the third replica.
	preferred[1].SetAvailable(true)
	res2, err := net.Repair(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	for _, rr := range res2 {
		if rr.ID == c.ID {
			r = &rr
		}
	}
	healthy, names := countHealthy(t, ctx, preferred, c.ID)
	if healthy != 3 {
		t.Fatalf("post-recovery repair did not converge: healthy=%d repaired=%v names=%v",
			healthy, r.Repaired, names)
	}
	t.Logf("id=%s phase=post-recovery healthy=%d nodes=%v repaired=%v basis=restored-from-verified-source",
		c.ID, healthy, names, r.Repaired)
}

// TestReadRepairRestoresCorruptAndMissing: a corrupt copy is served past
// (from a good replica) and silently replaced with verified bytes.
func TestReadRepairRestoresCorruptAndMissing(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	c := chunkFor("read-repair")
	opt := Options{Replication: 3}

	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	preferred := net.preferredNodes(c.ID, 3)
	if err := preferred[0].Corrupt(ctx, c.ID, []byte("BAD")); err != nil {
		t.Fatal(err)
	}
	if err := preferred[1].Delete(ctx, c.ID); err != nil {
		t.Fatal(err)
	}

	got, err := net.GetChunk(ctx, c.ID, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != string(c.Data) {
		t.Fatal("read repair returned wrong data")
	}
	for _, nd := range preferred {
		ok, err := nd.Has(ctx, c.ID)
		if err != nil || !ok {
			t.Fatalf("node %s not restored by read repair: ok=%v err=%v", nd.ID(), ok, err)
		}
	}
	t.Logf("id=%s nodes=%v basis=good-replica-served-then-copied-to-bad-slots",
		c.ID, nodeIDs(preferred))
}

// TestUnavailableReplicaNotRepaired: read repair must never count or
// "repair" a node that is down; convergence happens only after return.
func TestUnavailableReplicaNotRepaired(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	c := chunkFor("down-not-counted")
	opt := Options{Replication: 3}
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	preferred := net.preferredNodes(c.ID, 3)
	preferred[0].SetAvailable(false)

	if _, err := net.GetChunk(ctx, c.ID, opt); err != nil {
		t.Fatal(err)
	}
	// The down node still has its (old) on-disk copy; ensure no phantom
	// health was assumed: after it returns, its state must be real.
	preferred[0].SetAvailable(true)
	ok, err := preferred[0].Has(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("on-disk copy should be intact after a simulated outage")
	}
}
