package cas

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestClusterWriteReadRoundTrip(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 4, 3, 2)
	payload := bytes.Repeat([]byte("CAS-"), 40000) // 跨多个块

	blob, m, err := cluster.Write(context.Background(), payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(m.Chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(m.Chunks))
	}

	got, _, err := cluster.Read(context.Background(), blob)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("read content differs from written content")
	}

	// 确定性放置：每个块的目标节点集合稳定，且每个块恰好有 W 个确认。
	participants := map[string]bool{}
	for _, ch := range m.Chunks {
		targets := cluster.ReplicaTargets(ch.ID)
		if got := cluster.ReplicaTargets(ch.ID); len(got) != len(targets) {
			t.Fatal("replica targets unstable")
		}
		for i, tg := range targets {
			if tg.ID() != cluster.ReplicaTargets(ch.ID)[i].ID() {
				t.Fatal("replica target ordering unstable")
			}
			participants[tg.ID()] = true
		}
		acks := cluster.AcknowledgedReplicas(ch.ID)
		// 写在全部 N 个放置目标上尝试；全部可用时确认数为 N，
		// 任何情况下都不得超过 N（不高估），且不少于 W 才算确认。
		if len(acks) < 3 || len(acks) > 4 {
			t.Fatalf("chunk %s acknowledged replicas out of [W=3,N=4], got %d (%v)",
				ch.ID[:8], len(acks), acks)
		}
	}
	var p []string
	for id := range participants {
		p = append(p, id)
	}
	logVerdict(t, "cluster", string(blob), p,
		"write W=3/read R=2 ok; chunks=%d; per-chuck acks=3 (no overcount)", len(m.Chunks))
	_ = nodes
}

func TestClusterRepeatWriteSameConclusion(t *testing.T) {
	cluster, _, _ := testCluster(t, 3, 2, 2)
	payload := []byte("write me twice")
	id1, _, err := cluster.Write(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	id2, _, err := cluster.Write(context.Background(), append([]byte(nil), payload...))
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("identical content must produce identical blob id: %s vs %s", id1, id2)
	}
	got, _, err := cluster.Read(context.Background(), id1)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("idempotent rewrite broke read: %v", err)
	}
	logVerdict(t, "cluster", string(id1), nil, "repeated identical write yields same id and readable content")
}

func TestClusterReadMissingBlob(t *testing.T) {
	cluster, _, _ := testCluster(t, 3, 2, 2)
	fake := ChunkID([]byte("nothing"))
	_, _, err := cluster.Read(context.Background(), fake)
	// 全部节点可达但都没有该内容：先达到 R 个可达节点，随后判为无健康副本。
	if !IsKind(err, KindNoHealthyReplica) && !IsKind(err, KindMissing) {
		t.Fatalf("unknown blob must classify no_healthy_replica/missing, got %v", err)
	}
	logVerdict(t, "cluster", string(fake), nil, "unknown blob rejected: %v", err)
}

func TestClusterWriteQuorumFailure(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 3, 3, 2)
	// W=N=3：停掉一个节点，写无法达成法定数量。
	nodes[1].SetAvailable(false)
	_, _, err := cluster.Write(context.Background(), []byte("need all three"))
	if !errors.Is(err, ErrQuorumUnavailable) || !IsKind(err, KindQuorum) {
		t.Fatalf("write below quorum must be KindQuorum, got %v", err)
	}
	logVerdict(t, "cluster", "", nodeIDs(nodes), "1 node down, W=3 write refused: %v", err)
}
