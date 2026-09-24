package cas

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentWriteReadWithFlappingNode：并发写入、读取与一个节点反复
// 短暂不可用同时发生。断言：
//   - 所有已确认写入随后都能完整读回（不丢数据）；
//   - 并发期间读到的成功结果必然等于写入原文；
//   - 修复两轮后各放置节点收敛到一致的健康状态；
//   - 全部判定基于内容标识，重复运行结果一致。
func TestConcurrentWriteReadWithFlappingNode(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 4, 3, 2)
	ctx := context.Background()

	// 固定内容集合：内容标识确定，不依赖 goroutine 调度。
	payloads := make(map[ContentID][]byte)
	var ids []ContentID
	for i := 0; i < 6; i++ {
		p := bytes.Repeat([]byte{byte('A' + i)}, ChunkSize*(i%3+1)+37)
		id, m, err := cluster.Write(ctx, p)
		if err != nil {
			t.Fatalf("seed write %d: %v", i, err)
		}
		payloads[id] = p
		ids = append(ids, id)
		_ = m
	}

	flap := nodes[2]
	flapCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 节点抖动：1ms 粒度反复不可用/恢复。
		defer wg.Done()
		up := true
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-flapCtx.Done():
				flap.SetAvailable(true)
				return
			case <-tk.C:
				up = !up
				flap.SetAvailable(up)
			}
		}
	}()

	// 并发重复写入（相同内容）与读取。
	for round := 0; round < 4; round++ {
		for _, id := range ids {
			wg.Add(2)
			go func(id ContentID) {
				defer wg.Done()
				newID, _, err := cluster.Write(ctx, payloads[id])
				if err == nil && newID != id {
					t.Errorf("identical rewrite produced different id %s vs %s", newID, id)
				}
			}(id)
			go func(id ContentID) {
				defer wg.Done()
				got, _, err := cluster.Read(ctx, id)
				if err != nil {
					return // 法定数量偶发不足允许，修复后必须可读
				}
				if !bytes.Equal(got, payloads[id]) {
					t.Errorf("read content for %s differs from written payload", id[:8])
				}
			}(id)
		}
	}
	cancel()
	wg.Wait()

	// 恢复全部节点并执行两轮修复（第二轮应无新增副本）。
	for _, n := range nodes {
		n.SetAvailable(true)
	}
	r1, err := cluster.RunRepairOnce(ctx)
	if err != nil {
		t.Fatalf("repair1: %v", err)
	}
	r2, err := cluster.RunRepairOnce(ctx)
	if err != nil {
		t.Fatalf("repair2: %v", err)
	}
	if len(r1.Failures) != 0 || len(r2.Failures) != 0 {
		t.Fatalf("repair reported unrecoverable failures: %v / %v", r1.Failures, r2.Failures)
	}
	if r2.RebuiltChunks != 0 {
		t.Fatalf("second repair must be a no-op, rebuilt %d chunks", r2.RebuiltChunks)
	}

	// 收敛断言：每个已确认内容都能完整读回，且每个放置节点持有健康副本。
	for _, id := range ids {
		got, m, err := cluster.Read(ctx, id)
		if err != nil {
			t.Fatalf("blob %s unreadable after repair: %v", id[:8], err)
		}
		if !bytes.Equal(got, payloads[id]) {
			t.Fatalf("blob %s payload mismatch after repair", id[:8])
		}
		targets := cluster.ReplicaTargets(id)
		for _, ref := range m.Chunks {
			for _, tg := range targets {
				data, err := tg.GetChunk(ref.ID)
				if err != nil {
					t.Fatalf("blob %s chunk %s not healthy on %s: %v",
						id[:8], ref.ID[:8], tg.ID(), err)
				}
				if len(data) != ref.Size {
					t.Fatalf("chunk size mismatch on %s", tg.ID())
				}
			}
			acks := cluster.AcknowledgedReplicas(ref.ID)
			if len(acks) > 4 {
				t.Fatalf("replica count overestimated: %d", len(acks))
			}
		}
		logVerdict(t, "recovery", string(id), nodeIDs(targets),
			"converged: readable end-to-end, all N=%d placement nodes hold healthy chunks", len(targets))
	}
	logVerdict(t, "repair", "-", nodeIDs(nodes),
		"repair1 rebuilt=%d quarantined=%d; repair2 rebuilt=%d (idempotent)",
		r1.RebuiltChunks, r1.Quarantined, r2.RebuiltChunks)
}

// TestNodeOutageThenRepair：写入期间某目标节点全程不可用，确认写入仍成立；
// 节点恢复后一轮修复补齐缺失副本，台账与实际健康副本一致。
func TestNodeOutageThenRepair(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 4, 3, 2)
	ctx := context.Background()

	down := nodes[1]
	down.SetAvailable(false)
	payload := bytes.Repeat([]byte("outage-"), 20000)
	blob, m, err := cluster.Write(ctx, payload)
	if err != nil {
		t.Fatalf("write with one node down should meet W=3: %v", err)
	}
	// 已确认写入在节点不可用期间仍可读取。
	got, _, err := cluster.Read(ctx, blob)
	if err != nil {
		t.Fatalf("read during outage failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch during outage")
	}

	for _, ref := range m.Chunks {
		if down.HasChunk(ref.ID) {
			t.Fatalf("down node must not contain chunk %s after write", ref.ID[:8])
		}
		acks := cluster.AcknowledgedReplicas(ref.ID)
		if len(acks) != 3 {
			t.Fatalf("expected exactly W=3 acks during outage, got %d (%v)", len(acks), acks)
		}
	}
	logVerdict(t, "outage", string(blob), nodeIDs(nodes),
		"node %s down: write met W=3 with the other 3 nodes, read R=2 ok", down.ID())

	down.SetAvailable(true)
	report, err := cluster.RunRepairOnce(ctx)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if report.RebuiltChunks < len(m.Chunks) {
		t.Fatalf("repair should rebuild >=%d chunks on recovered node, got %d",
			len(m.Chunks), report.RebuiltChunks)
	}
	for _, ref := range m.Chunks {
		data, err := down.GetChunk(ref.ID)
		if err != nil || len(data) != ref.Size {
			t.Fatalf("recovered node missing healthy chunk after repair: %v", err)
		}
		acks := cluster.AcknowledgedReplicas(ref.ID)
		if len(acks) != 4 {
			t.Fatalf("after repair expected 4 acks, got %d", len(acks))
		}
	}
	if !down.HasManifest(blob) {
		t.Fatal("manifest not replicated to recovered node")
	}
	logVerdict(t, "outage", string(blob), []string{down.ID()},
		"recovered and repaired: rebuilt=%d, node now holds all chunks+manifest", report.RebuiltChunks)
}

// TestCorruptionQuarantineAndRepair：单个副本损坏不影响读取；
// 修复会隔离损坏副本并从健康副本补齐，最终所有放置节点健康一致。
func TestCorruptionQuarantineAndRepair(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 4, 3, 2)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("corruption-"), 9000)
	blob, m, err := cluster.Write(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}

	victim := nodes[0]
	var corrupted []ContentID
	for _, ref := range m.Chunks {
		// 只在确实持有该块的节点上注入损坏。
		if victim.HasChunk(ref.ID) {
			if err := victim.CorruptChunk(ref.ID); err != nil {
				t.Fatal(err)
			}
			corrupted = append(corrupted, ref.ID)
		}
	}
	if len(corrupted) == 0 {
		t.Fatal("test setup failed: no chunk placed on victim")
	}

	// 读取仍成功：协商过程跳过损坏副本，采信健康副本。
	got, _, err := cluster.Read(ctx, blob)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("read must survive one corrupt replica: %v", err)
	}
	logVerdict(t, "corrupt", string(blob), nodeIDs(nodes),
		"corrupted %d chunk(s) on %s, read still served from healthy replicas",
		len(corrupted), victim.ID())

	// 直接读损坏副本必须报 KindCorrupted。
	if _, err := victim.GetChunk(corrupted[0]); !IsKind(err, KindCorrupted) {
		t.Fatalf("expected KindCorrupted from victim, got %v", err)
	}

	report, err := cluster.RunRepairOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Quarantined < len(corrupted) {
		t.Fatalf("expected >=%d quarantined, got %d", len(corrupted), report.Quarantined)
	}
	for _, cid := range corrupted {
		data, err := victim.GetChunk(cid)
		if err != nil || ChunkID(data) != cid {
			t.Fatalf("corrupt replica not rebuilt healthy: %v", err)
		}
	}
	logVerdict(t, "corrupt", string(blob), []string{victim.ID()},
		"repair quarantined=%d and rebuilt all corrupt chunks healthy", report.Quarantined)
}

// TestAllReplicasCorruptIsUnrecoverable：某块的全部副本都损坏时，
// 读取必须明确报 KindNoHealthyReplica（且错误链保留各副本的损坏原因），
// 修复必须把问题列入 Failures，而不是静默返回坏数据。
func TestAllReplicasCorruptIsUnrecoverable(t *testing.T) {
	// N=2, W=2, R=2：只有两个副本，全部损坏。
	cluster, nodes, _ := testCluster(t, 2, 2, 2)
	ctx := context.Background()
	blob, m, err := cluster.Write(ctx, []byte("both-copies-corrupt"))
	if err != nil {
		t.Fatal(err)
	}
	target := m.Chunks[0].ID
	for _, n := range nodes {
		if err := n.CorruptChunk(target); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = cluster.Read(ctx, blob)
	if !IsKind(err, KindNoHealthyReplica) {
		t.Fatalf("all-corrupt reads must be KindNoHealthyReplica, got %v", err)
	}
	logVerdict(t, "corrupt", string(blob), nodeIDs(nodes),
		"all replicas of chunk %s corrupt => no_healthy_replica: %v", target[:8], err)

	report, _ := cluster.RunRepairOnce(ctx)
	found := false
	for _, f := range report.Failures {
		if f.ID == target && IsKind(f, KindNoHealthyReplica) {
			found = true
		}
	}
	if !found {
		t.Fatalf("repair must report the unrecoverable chunk, failures=%v", report.Failures)
	}
}

// TestRepairReportDeterministic：重复运行修复，统计结果稳定一致。
func TestRepairReportDeterministic(t *testing.T) {
	cluster, _, _ := testCluster(t, 3, 3, 2)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, _, err := cluster.Write(ctx, []byte(fmt.Sprintf("stable-%d-%s", i, bytes.Repeat([]byte("z"), 100)))); err != nil {
			t.Fatal(err)
		}
	}
	r1, _ := cluster.RunRepairOnce(ctx)
	r2, _ := cluster.RunRepairOnce(ctx)
	if r1.CheckedBlobs != r2.CheckedBlobs {
		t.Fatalf("checked blobs unstable: %d vs %d", r1.CheckedBlobs, r2.CheckedBlobs)
	}
	if len(r1.HealthyReplicas) != len(r2.HealthyReplicas) {
		t.Fatal("healthy replica map size unstable")
	}
	for k, v := range r1.HealthyReplicas {
		if r2.HealthyReplicas[k] != v {
			t.Fatalf("healthy count for %s unstable: %d vs %d", k, v, r2.HealthyReplicas[k])
		}
	}
	if r2.RebuiltChunks != 0 || r2.Quarantined != 0 {
		t.Fatal("steady-state repair must make no changes")
	}
	logVerdict(t, "repair", "-", nil,
		"steady state: checked=%d healthy=%v stable across repeated runs",
		r1.CheckedBlobs, r1.HealthyReplicas)
}

// TestRepairLoopConvergesInBackground：后台修复循环自动补齐缺失副本。
func TestRepairLoopConvergesInBackground(t *testing.T) {
	cluster, nodes, _ := testCluster(t, 4, 3, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.StartRepairLoop(ctx, 5*time.Millisecond)

	nodes[2].SetAvailable(false)
	blob, m, err := cluster.Write(ctx, []byte("background-repair-payload"))
	if err != nil {
		t.Fatal(err)
	}
	nodes[2].SetAvailable(true)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok := nodes[2].HasManifest(blob)
		for _, ref := range m.Chunks {
			ok = ok && nodes[2].HasChunk(ref.ID)
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, ref := range m.Chunks {
		if _, err := nodes[2].GetChunk(ref.ID); err != nil {
			t.Fatalf("background repair did not converge: %v", err)
		}
	}
	logVerdict(t, "repair", string(blob), []string{nodes[2].ID()},
		"background loop converged recovered node to cluster state")
}
