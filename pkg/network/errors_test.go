package network

import (
	"context"
	"errors"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/store"
)

// TestGetMissingClassifiesNotFound: content that was never written must
// be distinguished from corruption or outages.
func TestGetMissingClassifiesNotFound(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	id := chunk.ComputeID([]byte("never-existed"))
	_, err := net.GetChunk(ctx, id, Options{Replication: 3})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want store.ErrNotFound, got %v", err)
	}
	t.Logf("id=%s reason=%v basis=every-reachable-node-reported-missing", id, err)
}

// TestAllReplicasCorruptClassified: with the full replica set corrupt and
// no healthy source anywhere, reads must fail as ErrAllReplicasBad rather
// than serving garbage or being mislabeled as missing.
func TestAllReplicasCorruptClassified(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	c := chunkFor("all-corrupt")
	opt := Options{Replication: 3}
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	preferred := net.preferredNodes(c.ID, 3)
	for _, nd := range preferred {
		if err := nd.Corrupt(ctx, c.ID, []byte("ROTTEN")); err != nil {
			t.Fatal(err)
		}
	}
	_, err := net.GetChunk(ctx, c.ID, opt)
	if !errors.Is(err, ErrAllReplicasBad) {
		t.Fatalf("want ErrAllReplicasBad, got %v", err)
	}
	t.Logf("id=%s nodes=%v reason=%v basis=every-contacted-replica-hash-mismatch",
		c.ID, nodeIDs(preferred), err)
}

// TestQuorumUnavailableClassified: with every node down, reads cannot
// contact anyone and must fail as ErrQuorumUnavailable.
func TestQuorumUnavailableClassified(t *testing.T) {
	ctx := context.Background()
	net, nodes := newTestNetwork(t, 4)
	c := chunkFor("all-down")
	if _, err := net.PutChunk(ctx, c, Options{Replication: 3}); err != nil {
		t.Fatal(err)
	}
	for _, nd := range nodes {
		nd.SetAvailable(false)
	}
	_, err := net.GetChunk(ctx, c.ID, Options{Replication: 3})
	if !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("want ErrQuorumUnavailable, got %v", err)
	}
	t.Logf("id=%s reason=%v basis=no-node-contactable", c.ID, err)
}

// TestNotEnoughReplicasWhenPlacementNodeDown: a write that cannot meet
// the replica factor reports a stable error and a true, lower count.
func TestNotEnoughReplicasWhenPlacementNodeDown(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 3)
	c := chunkFor("need-three")
	opt := Options{Replication: 3}
	net.preferredNodes(c.ID, 3)[0].SetAvailable(false)
	acked, err := net.PutChunk(ctx, c, opt)
	if !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("want ErrNotEnoughReplicas, got %v", err)
	}
	if acked != 2 {
		t.Fatalf("want 2 confirmed, got %d", acked)
	}
	t.Logf("id=%s acked=%d reason=%v basis=preferred-set-member-down", c.ID, acked, err)
}

// TestObjectEndToEndWithVersionedManifest: PutObject/GetObject round-trip
// larger content across chunk boundaries; the returned manifest is v1 and
// fetching it by its manifest ID reconstructs identical bytes.
func TestObjectEndToEndWithVersionedManifest(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3, ChunkSize: 64}

	payload := make([]byte, 300)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	manifestID, man, err := net.PutObject(ctx, "blob.bin", payload, opt)
	if err != nil {
		t.Fatal(err)
	}
	if man.Version != manifest.CurrentVersion {
		t.Fatalf("manifest version=%d", man.Version)
	}
	if len(man.Entries) != 5 {
		t.Fatalf("want 5 entries, got %d", len(man.Entries))
	}
	got, gotMan, err := net.GetObject(ctx, manifestID, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("object payload round-trip mismatch")
	}
	t.Logf("object=blob.bin manifest-id=%s version=%d chunks=%d basis=deterministic-split+manifest-by-id",
		manifestID, gotMan.Version, len(gotMan.Entries))

	// A second PutObject of identical content yields the same manifest ID.
	manifestID2, _, err := net.PutObject(ctx, "blob.bin", payload, opt)
	if err != nil || manifestID2 != manifestID {
		t.Fatalf("identical content must yield identical manifest id: %s vs %s err=%v",
			manifestID, manifestID2, err)
	}
}
