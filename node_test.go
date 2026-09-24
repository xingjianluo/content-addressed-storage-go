package cas

import (
	"errors"
	"testing"
)

func TestNodePutGetAndRestart(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 3, 2, 2)
	data := []byte("persisted-payload")
	id := ChunkID(data)

	if err := nodes[0].PutChunk(id, data); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := nodes[0].GetChunk(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(data) {
		t.Fatal("round-trip data mismatch")
	}

	// 模拟节点重启：重新打开同一目录，数据必须仍在（已确认写入不丢失）。
	reopened, err := NewNode(nodes[0].ID(), nodes[0].dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.HasChunk(id) {
		t.Fatal("chunk missing after node restart")
	}
	if _, err := reopened.GetChunk(id); err != nil {
		t.Fatalf("chunk unreadable after restart: %v", err)
	}
	logVerdict(t, "node", string(id), []string{nodes[0].ID()},
		"put/get ok and survives reopen; cluster nodes=%v", nodeIDs(cluster.Nodes()))
}

func TestNodePutMismatchRejected(t *testing.T) {
	_, nodes, _ := testCluster(t, 1, 1, 1)
	id := ChunkID([]byte("real"))
	err := nodes[0].PutChunk(id, []byte("fake"))
	if !errors.Is(err, ErrIDMismatch) || !IsKind(err, KindIDMismatch) {
		t.Fatalf("putting bytes under wrong id must be KindIDMismatch, got %v", err)
	}
	if nodes[0].HasChunk(id) {
		t.Fatal("rejected put must not create stored data")
	}
	logVerdict(t, "node", string(id), []string{nodes[0].ID()}, "wrong-id put rejected: %v", err)
}

func TestNodeMissingAndCorruption(t *testing.T) {
	_, nodes, _ := testCluster(t, 2, 1, 1)
	data := []byte("corrupt-me-please")
	id := ChunkID(data)
	missing := ChunkID([]byte("never-stored"))

	if err := nodes[0].PutChunk(id, data); err != nil {
		t.Fatal(err)
	}

	if _, err := nodes[0].GetChunk(missing); !errors.Is(err, ErrMissingContent) ||
		!IsKind(err, KindMissing) {
		t.Fatalf("absent chunk must be KindMissing, got %v", err)
	}
	if _, err := nodes[1].GetChunk(id); !IsKind(err, KindMissing) {
		t.Fatalf("chunk on another node must be KindMissing, got %v", err)
	}

	if err := nodes[0].CorruptChunk(id); err != nil {
		t.Fatal(err)
	}
	_, err := nodes[0].GetChunk(id)
	if !errors.Is(err, ErrContentCorrupted) || !IsKind(err, KindCorrupted) {
		t.Fatalf("tampered bytes must be KindCorrupted, got %v", err)
	}
	logVerdict(t, "node", string(id), []string{nodes[0].ID(), nodes[1].ID()},
		"missing on empty node, corrupted after bit flip: %v", err)
}

func TestNodeUnavailableClassifiesQuorum(t *testing.T) {
	_, nodes, _ := testCluster(t, 1, 1, 1)
	id := ChunkID([]byte("x"))
	nodes[0].SetAvailable(false)
	if err := nodes[0].PutChunk(id, []byte("x")); !IsKind(err, KindQuorum) {
		t.Fatalf("write to down node must be KindQuorum, got %v", err)
	}
	if _, err := nodes[0].GetChunk(id); !IsKind(err, KindQuorum) {
		t.Fatalf("read from down node must be KindQuorum, got %v", err)
	}
	nodes[0].SetAvailable(true)
	if nodes[0].Available() != true {
		t.Fatal("node did not recover availability")
	}
	logVerdict(t, "node", string(id), []string{nodes[0].ID()}, "down=>quorum, up=>available")
}
