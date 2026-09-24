// cas-demo 在本地临时目录上启动一个 4 节点内容寻址存储网络，
// 演示写入、读取、节点故障与后台修复的完整过程。
//
// 运行：go run ./cmd/cas-demo
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	cas "github.com/xingjianluo/content-addressed-storage-go"
)

func main() {
	root, err := os.MkdirTemp("", "cas-demo-")
	must(err)
	defer os.RemoveAll(root)
	fmt.Printf("本地存储根目录: %s\n\n", root)

	var nodes []*cas.Node
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("node%d", i)
		n, err := cas.NewNode(id, filepath.Join(root, id))
		must(err)
		nodes = append(nodes, n)
	}
	// N=4, W=3, R=2：容忍一个节点不可用，且 W+R > N 保证读写交集。
	cluster := cas.NewCluster(nodes, cas.Config{ReplicationFactor: 4, WriteQuorum: 3, ReadQuorum: 2})

	payload := []byte("hello content-addressed world: 相同内容必得相同标识")
	ctx := context.Background()

	blob, manifest, err := cluster.Write(ctx, payload)
	must(err)
	fmt.Printf("1) 写入成功  内容标识=%s 块数=%d\n", blob, len(manifest.Chunks))
	for _, ch := range manifest.Chunks {
		fmt.Printf("   块 %d: id=%s size=%d 目标节点=%v 确认节点=%v\n",
			ch.Index, ch.ID, ch.Size, ids(cluster.ReplicaTargets(ch.ID)),
			cluster.AcknowledgedReplicas(ch.ID))
	}

	blob2, _, _ := cluster.Write(ctx, append([]byte(nil), payload...))
	fmt.Printf("2) 重复写入  标识=%s 一致=%v\n\n", blob2, blob2 == blob)

	// 让 node2 暂时不可用，并破坏 node1 上的全部块。
	nodes[1].SetAvailable(false)
	for _, ch := range manifest.Chunks {
		if nodes[0].HasChunk(ch.ID) {
			must(nodes[0].CorruptChunk(ch.ID))
		}
	}
	fmt.Println("3) 注入故障: node2 不可用; node1 上块字节损坏")

	got, _, err := cluster.Read(ctx, blob)
	must(err)
	fmt.Printf("   读取仍成功(跳过损坏副本, 可联系节点满足 R=2): %q\n\n", string(got[:40]))

	// 恢复 node2，先执行一轮显式修复以展示隔离与补齐过程。
	nodes[1].SetAvailable(true)
	report, err := cluster.RunRepairOnce(ctx)
	must(err)
	fmt.Printf("4) 一轮修复  检查内容=%d 新补齐块=%d 隔离损坏=%d 失败=%d\n",
		report.CheckedBlobs, report.RebuiltChunks, report.Quarantined, len(report.Failures))
	for _, n := range nodes {
		fmt.Printf("   %s 健康块=%d 清单存在=%v\n", n.ID(), report.HealthyReplicas[n.ID()], n.HasManifest(blob))
	}

	// 再演示后台修复循环：再次让 node2 短暂下线并删掉它的块，循环自动收敛。
	for _, ch := range manifest.Chunks {
		must(nodes[1].DeleteChunk(ch.ID))
	}
	nodes[1].SetAvailable(false)
	loopCtx, stopLoop := context.WithCancel(ctx)
	cluster.StartRepairLoop(loopCtx, 10*time.Millisecond)
	nodes[1].SetAvailable(true)
	time.Sleep(150 * time.Millisecond)
	stopLoop()
	if _, err := nodes[1].GetChunk(manifest.Chunks[0].ID); err != nil {
		must(err)
	}
	fmt.Println("5) 后台循环: node2 块被删除后由 10ms 周期修复循环自动补齐")

	got, _, err = cluster.Read(ctx, blob)
	must(err)
	fmt.Printf("6) 收敛校验  读取字节与原文一致=%v\n", string(got) == string(payload))
}

func ids(ns []*cas.Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.ID()
	}
	return out
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}
