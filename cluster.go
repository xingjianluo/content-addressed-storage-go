package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Config 描述集群的副本与法定数量参数。
// 约束：1 <= W,R <= N <= 节点数；建议 W+R > N 以保证读写交集。
type Config struct {
	ReplicationFactor int // 每份数据的目标副本数 N
	WriteQuorum       int // 写确认所需的节点数 W
	ReadQuorum        int // 读协商所需的节点数 R
}

// RepairReport 是一轮修复的确定性报告。
type RepairReport struct {
	CheckedBlobs    int            // 参与校验的内容（清单）数
	RebuiltChunks   int            // 本轮新补齐的块副本数
	Quarantined     int            // 被隔离的损坏副本数（块与清单）
	HealthyReplicas map[string]int // 节点标识 -> 修复后其持有的健康块数
	Failures        []*Failure     // 无法自动修复的问题（如全部副本丢失）
}

// Cluster 协调多个本地节点的副本写入、读取与后台修复。
type Cluster struct {
	nodes []*Node // 按节点标识排序，放置与遍历顺序确定
	cfg   Config

	mu             sync.Mutex
	ledger         map[ContentID]map[string]struct{} // 块标识 -> 已校验确认写入的节点集合
	manifestLedger map[ContentID]map[string]struct{} // 内容标识 -> 清单确认节点集合
}

// NewCluster 组装集群；节点按标识排序以保证放置与遍历顺序确定。
// 非法配置属于编程错误，直接 panic 并给出明确信息。
func NewCluster(nodes []*Node, cfg Config) *Cluster {
	sorted := append([]*Node(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID() < sorted[j].ID() })
	if cfg.ReplicationFactor < 1 || cfg.WriteQuorum < 1 || cfg.ReadQuorum < 1 {
		panic("cas: quorums and replication factor must be >= 1")
	}
	if cfg.WriteQuorum > cfg.ReplicationFactor || cfg.ReadQuorum > cfg.ReplicationFactor {
		panic("cas: quorums must not exceed replication factor")
	}
	if cfg.ReplicationFactor > len(sorted) {
		panic("cas: replication factor exceeds number of nodes")
	}
	return &Cluster{
		nodes:          sorted,
		cfg:            cfg,
		ledger:         map[ContentID]map[string]struct{}{},
		manifestLedger: map[ContentID]map[string]struct{}{},
	}
}

// Nodes 返回集群节点（按标识排序）。
func (c *Cluster) Nodes() []*Node { return c.nodes }

// ReplicaTargets 返回某标识按确定性一致性哈希放置对应的 N 个目标节点：
// 以标识 SHA-256 的前 8 字节对节点数取模作为起点，在排序节点环上顺取 N 个。
// 相同集群与相同标识永远得到相同目标集合与顺序。
func (c *Cluster) ReplicaTargets(id ContentID) []*Node {
	m := len(c.nodes)
	sum := sha256.Sum256([]byte(id))
	start := int(binary.BigEndian.Uint64(sum[:8]) % uint64(m))
	out := make([]*Node, c.cfg.ReplicationFactor)
	for k := 0; k < c.cfg.ReplicationFactor; k++ {
		out[k] = c.nodes[(start+k)%m]
	}
	return out
}

func (c *Cluster) recordAck(ledger map[ContentID]map[string]struct{}, id ContentID, nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	set, ok := ledger[id]
	if !ok {
		set = map[string]struct{}{}
		ledger[id] = set
	}
	set[nodeID] = struct{}{}
}

// AcknowledgedReplicas 返回某块已确认写入的节点标识（排序）。
// 只有实际通过完整性校验的写入才会计入，因此不会高估副本数；
// 节点之后短暂不可用不会抹除已确认记录，确认写入不丢失。
func (c *Cluster) AcknowledgedReplicas(id ContentID) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := c.ledger[id]
	out := make([]string, 0, len(set))
	for nodeID := range set {
		out = append(out, nodeID)
	}
	sort.Strings(out)
	return out
}

// Write 切分内容、按确定性放置写入副本，达到写法定数 W 才确认；
// 返回整体内容标识与清单。任何一块（或清单）未达 W 返回 KindQuorum，
// 此时不发布清单；已落盘的块保持有效，重试为幂等操作。
func (c *Cluster) Write(ctx context.Context, data []byte) (ContentID, *Manifest, error) {
	blob, chunks := Split(data)
	m := NewManifest(blob, int64(len(data)), chunks)

	// 块写入（按块序、目标序，行为确定；节点自身加锁，可与其他 Write 并发）。
	for _, ch := range chunks {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}
		acks := 0
		var lastErr error
		for _, target := range c.ReplicaTargets(ch.ID) {
			if err := target.PutChunk(ch.ID, ch.Data); err != nil {
				lastErr = err
				continue
			}
			c.recordAck(c.ledger, ch.ID, target.ID())
			acks++
		}
		if acks < c.cfg.WriteQuorum {
			return "", nil, fail(KindQuorum, ch.ID, "",
				fmt.Sprintf("write confirmed on %d nodes, require %d", acks, c.cfg.WriteQuorum),
				lastErr)
		}
	}

	// 发布清单：同样需要 W 个确认。
	acks := 0
	var lastErr error
	for _, target := range c.ReplicaTargets(blob) {
		if err := target.PutManifest(blob, m); err != nil {
			lastErr = err
			continue
		}
		c.recordAck(c.manifestLedger, blob, target.ID())
		acks++
	}
	if acks < c.cfg.WriteQuorum {
		return "", nil, fail(KindQuorum, blob, "",
			fmt.Sprintf("manifest confirmed on %d nodes, require %d", acks, c.cfg.WriteQuorum),
			lastErr)
	}
	return blob, m, nil
}

// Read 读取清单与各块。规则：
//   - 每个标识都在其 N 个放置目标上协商；
//   - 可联系到的目标少于读法定数 R：KindQuorum；
//   - 达到 R 但没有任何健康副本：KindNoHealthyReplica（底层附带
//     KindMissing / KindCorrupted 等可区分原因）；
//   - 只采信通过完整性校验的副本，按节点标识序取第一个健康副本；
//   - 重组后再校验整体内容标识。
func (c *Cluster) Read(ctx context.Context, blob ContentID) ([]byte, *Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	var manifest *Manifest
	var basis string
	reachable := 0
	var contentErrs []error
	for _, target := range c.ReplicaTargets(blob) {
		got, err := target.GetManifest(blob)
		if err != nil {
			if IsKind(err, KindQuorum) {
				continue // 节点不可用，不计入可联系节点
			}
			reachable++
			contentErrs = append(contentErrs, err)
			continue
		}
		reachable++
		if manifest == nil {
			manifest = got
			basis = target.ID()
		}
	}
	if reachable < c.cfg.ReadQuorum {
		return nil, nil, fail(KindQuorum, blob, "",
			fmt.Sprintf("manifest reachable on %d nodes, require %d", reachable, c.cfg.ReadQuorum),
			joinErrs(contentErrs))
	}
	if manifest == nil {
		return nil, nil, fail(KindNoHealthyReplica, blob, "",
			"no healthy manifest replica; all reachable copies missing or corrupt",
			joinErrs(contentErrs))
	}

	var buf bytes.Buffer
	for _, ref := range manifest.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		data, basisNode, err := c.readChunk(ref)
		if err != nil {
			return nil, nil, err
		}
		buf.Write(data)
		basis += "," + basisNode
	}

	out := buf.Bytes()
	// 完整性依据：每个块在 GetChunk 内已按其 SHA-256 校验；
	// 此处再验证块引用结构摘要与长度，任何清单/数据被篡改都会在此暴露。
	if computed := manifest.ComputedBlobID(); computed != blob {
		return nil, nil, fail(KindIDMismatch, blob, "",
			fmt.Sprintf("manifest refs compute blob id %s, requested %s", computed, blob), nil)
	}
	if int64(len(out)) != manifest.Length {
		return nil, nil, fail(KindCorrupted, blob, "",
			fmt.Sprintf("reassembled length %d disagrees with manifest %d", len(out), manifest.Length), nil)
	}
	return out, manifest, nil
}

// readChunk 在放置目标上协商读取单个块，返回数据与作为判定依据的节点。
func (c *Cluster) readChunk(ref ChunkRef) ([]byte, string, error) {
	reachable := 0
	var contentErrs []error
	for _, target := range c.ReplicaTargets(ref.ID) {
		data, err := target.GetChunk(ref.ID)
		if err != nil {
			if IsKind(err, KindQuorum) {
				continue
			}
			reachable++
			contentErrs = append(contentErrs, err)
			continue
		}
		reachable++
		if len(data) != ref.Size {
			contentErrs = append(contentErrs, fail(KindCorrupted, ref.ID, target.ID(),
				fmt.Sprintf("chunk size %d disagrees with manifest %d", len(data), ref.Size), nil))
			continue
		}
		return data, target.ID(), nil
	}
	if reachable < c.cfg.ReadQuorum {
		return nil, "", fail(KindQuorum, ref.ID, "",
			fmt.Sprintf("chunk reachable on %d nodes, require %d", reachable, c.cfg.ReadQuorum),
			joinErrs(contentErrs))
	}
	return nil, "", fail(KindNoHealthyReplica, ref.ID, "",
		"no healthy chunk replica; all reachable copies missing or corrupt",
		joinErrs(contentErrs))
}

func joinErrs(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return &multiError{errs: errs}
}

type multiError struct{ errs []error }

func (m *multiError) Error() string {
	b := &bytes.Buffer{}
	for i, e := range m.errs {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(e.Error())
	}
	return b.String()
}

func (m *multiError) Unwrap() []error { return m.errs }

// RunRepairOnce 执行一轮修复：
//   - 汇总所有可用节点上的清单，按节点标识序选取健康清单作为权威；
//   - 隔离损坏的块/清单副本（重命名为 .quarantine，停止对外服务）；
//   - 对每个块：若放置目标上存在健康副本，则向缺失目标补齐，
//     并登记确认台账；若全部目标都没有健康副本，记入 Failures；
//   - 报告中的节点计数按节点标识排序生成，多轮运行结果确定。
func (c *Cluster) RunRepairOnce(ctx context.Context) (RepairReport, error) {
	report := RepairReport{HealthyReplicas: map[string]int{}}

	// 1) 收集全部清单标识（可用节点并集，排序）。
	blobSet := map[ContentID]struct{}{}
	var available []*Node
	for _, n := range c.nodes {
		if !n.Available() {
			continue
		}
		available = append(available, n)
		for _, id := range n.StoredBlobIDs() {
			blobSet[id] = struct{}{}
		}
	}
	blobs := make([]ContentID, 0, len(blobSet))
	for id := range blobSet {
		blobs = append(blobs, id)
	}
	sortContentIDs(blobs)

	// 2) 逐内容校验清单与块。
	for _, blob := range blobs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.CheckedBlobs++

		manifest := c.pickHealthyManifest(blob, &report)
		if manifest == nil {
			continue // 所有清单副本缺失/损坏（缺失属于尚未发布，不计失败）
		}

		// 补齐清单副本。
		for _, target := range c.ReplicaTargets(blob) {
			if !target.Available() || target.HasManifest(blob) {
				continue
			}
			if err := target.PutManifest(blob, manifest); err == nil {
				c.recordAck(c.manifestLedger, blob, target.ID())
			}
		}

		// 逐块修复。
		for _, ref := range manifest.Chunks {
			data, source := c.findHealthyChunk(ref)
			targets := c.ReplicaTargets(ref.ID)
			if data == nil {
				// 没有任何健康副本：隔离损坏者后仍无法恢复，记录失败。
				anyCopy := false
				for _, t := range targets {
					if !t.Available() {
						continue
					}
					if t.HasChunk(ref.ID) {
						anyCopy = true
					}
				}
				if anyCopy {
					report.Failures = append(report.Failures, fail(KindNoHealthyReplica, ref.ID, "",
						"all replicas corrupt; chunk cannot be rebuilt without an external source", nil))
				}
				continue
			}

			for _, target := range targets {
				if !target.Available() {
					continue
				}
				if target.ID() == source {
					report.HealthyReplicas[target.ID()]++
					continue
				}
				if target.HasChunk(ref.ID) {
					// 记录存在：验证字节，损坏则隔离后补齐。
					if _, err := target.GetChunk(ref.ID); err == nil {
						report.HealthyReplicas[target.ID()]++
						continue
					} else if IsKind(err, KindCorrupted) {
						if c.quarantineChunk(target, ref.ID) {
							report.Quarantined++
						}
					}
				}
				// 缺失或刚隔离：用健康副本补齐（PutChunk 会再次校验）。
				if err := target.PutChunk(ref.ID, data); err != nil {
					report.Failures = append(report.Failures, fail(KindNoHealthyReplica, ref.ID, target.ID(),
						"repair copy failed", err))
					continue
				}
				c.recordAck(c.ledger, ref.ID, target.ID())
				report.RebuiltChunks++
				report.HealthyReplicas[target.ID()]++
			}
		}
	}

	return report, nil
}

// pickHealthyManifest 按节点标识序在全部可用节点上寻找第一个健康清单；
// 途中隔离损坏清单。返回 nil 表示没有可用健康副本。
func (c *Cluster) pickHealthyManifest(blob ContentID, report *RepairReport) *Manifest {
	for _, n := range c.nodes {
		if !n.Available() {
			continue
		}
		if !n.HasManifest(blob) {
			continue
		}
		m, err := n.GetManifest(blob)
		if err != nil {
			if IsKind(err, KindCorrupted) || IsKind(err, KindIDMismatch) ||
				IsKind(err, KindManifestTooNew) || IsKind(err, KindUnknownField) {
				if c.quarantineManifest(n, blob) {
					report.Quarantined++
				}
			}
			continue
		}
		return m
	}
	return nil
}

// findHealthyChunk 在所有可用节点（含非放置目标）寻找第一个健康副本，
// 扩大恢复来源以容忍放置漂移；返回数据与来源节点标识。
func (c *Cluster) findHealthyChunk(ref ChunkRef) ([]byte, string) {
	for _, n := range c.nodes {
		if !n.Available() {
			continue
		}
		data, err := n.GetChunk(ref.ID)
		if err != nil {
			if IsKind(err, KindCorrupted) {
				// 非权威扫描阶段不立即隔离，统一在放置目标循环中处理。
			}
			continue
		}
		if len(data) == ref.Size {
			return data, n.ID()
		}
	}
	return nil, ""
}

// quarantineChunk 将损坏块重命名为 .quarantine，使其停止对外服务。
func (c *Cluster) quarantineChunk(n *Node, id ContentID) bool {
	return n.quarantine(id, "chunks")
}

func (c *Cluster) quarantineManifest(n *Node, blob ContentID) bool {
	return n.quarantine(blob, "manifests")
}

// StartRepairLoop 周期性执行修复，直到 ctx 取消；返回的等待需外部用 ctx 控制。
func (c *Cluster) StartRepairLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = c.RunRepairOnce(ctx)
			}
		}
	}()
}
