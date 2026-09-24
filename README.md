# 本地内容寻址存储网络（Go）

一套**仅依赖本地文件系统**、无任何外部服务的内容寻址存储（Content-Addressed
Storage, CAS）网络，用于在单机上验证：

- 内容完整性（损坏 / 缺失 / 标识不匹配必须被检出且原因可区分）；
- 多副本一致性与法定数量（并发读写 + 节点短暂不可用下不丢数据、不高估副本）；
- 故障恢复（后台修复自动隔离损坏副本、补齐缺失副本并收敛）；
- 内容清单的版本演进与向后兼容（旧版本可读、新版本明确拒绝、未知字段不静默丢弃）。

## 运行方法

```bash
# 全部单元测试（并发、损坏、故障恢复、版本兼容）
go test -v -count=1 ./...

# 竞态检测下重复运行，验证并发安全与结果稳定
go test -race -count=3 ./...

# 本地端到端演示（在临时目录上启动 4 个节点并注入故障）
go run ./cmd/cas-demo
```

测试与演示使用 `t.TempDir()` / `os.MkdirTemp`，不依赖任何外部进程或端口。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `errors.go` | 稳定的错误分类（`FailureKind` / `Failure`），可 `errors.Is` 匹配的哨兵错误 |
| `chunk.go` | 确定性切分（64 KiB 固定块）、SHA-256 内容标识、块校验 |
| `manifest.go` | 版本化内容清单的确定性序列化与版本兼容规则 |
| `node.go` | 本地文件系统节点（原子写、重启恢复、损坏注入、隔离） |
| `cluster.go` | 副本放置、读写法定数量、协商读取、修复循环 |
| `*_test.go` | 覆盖并发 / 损坏 / 故障恢复 / 版本兼容的断言 |
| `cmd/cas-demo` | 本地可运行的端到端演示 |

## 1. 内容标识与完整性规则

### 1.1 标识算法

- **块标识（Chunk ID）**：块字节的 SHA-256 十六进制摘要（64 个十六进制字符）。
- **内容标识（Blob ID）**：内容按 **64 KiB 固定边界**切分后，对
  “总长度 + 有序块引用（序号、大小、块标识）”做确定性摘要：

  ```
  sha256(
    "cas-blob-v1\n" + 十进制字节长度 + "\n" +
    对每个块按序一行: "<index>:<size>:<chunkID>\n"
  )
  ```

  因为切分边界只由内容长度决定、块引用只由块字节决定，**相同内容重复写入
  必然得到相同的 Blob ID 与块结构**；空内容（零个块）也有确定标识。

### 1.2 完整性判定（三处独立校验）

1. **写入时**：`PutChunk` 先重算 SHA-256，与给定标识不一致则拒绝写入
   （`id_mismatch`），绝不存储名实不符的数据。
2. **读取时**：`GetChunk` 每次都重算块字节摘要，不匹配报 `corrupted`；
   集群读取还会核对清单记录的块大小，并由清单重算 Blob ID 与请求标识比对。
3. **重组后**：集群把读到的块引用结构重算为 Blob ID，并校验总长度，
   防止“块都合法但清单结构被篡改/缺块多块”的情况。

### 1.3 稳定的错误分类

所有失败都携带 `FailureKind`（不依赖错误文案），可用 `cas.IsKind(err, kind)`
或 `errors.Is(err, cas.ErrXxx)` 断言：

| 类别 | 含义 |
| --- | --- |
| `missing` | 内容在目标节点不存在 |
| `corrupted` | 内容存在但字节与其标识不一致（或清单结构损坏） |
| `id_mismatch` | 请求/声明的标识与实际计算结果不一致 |
| `manifest_too_new` | 清单版本高于当前支持的最高版本 |
| `unknown_field` | 清单含按规则不可忽略的 `critical_*` 未知字段 |
| `quorum_unavailable` | 可联系节点数达不到读/写法定数量 |
| `no_healthy_replica` | 达到协商数量但所有副本均缺失或损坏 |

## 2. 副本与修复语义

### 2.1 确定性副本放置

- 节点按标识排序组成有序环；对每个块/清单标识取其 SHA-256 前 8 字节
  对节点数取模作为起点，在环上顺取 N 个目标节点。
- 同一集群、同一标识永远得到相同的目标集合与顺序，因此重复运行结果一致。

### 2.2 写法定数量（W）与确认台账

- 写操作向全部 N 个放置目标尝试，**至少 W 个节点校验通过并落盘**才算确认；
  否则返回 `quorum_unavailable` 且不发布清单（重试是幂等的）。
- 每个节点的写都是**临时文件 + fsync + 原子重命名**，不存在半截数据；
  节点进程/测试重启后从目录重新加载，已确认写入不丢失。
- 集群维护**确认台账**：只有实际通过完整性校验的写入才登记。
  `AcknowledgedReplicas` 只统计确认节点，因此**不会把失败/未尝试的节点
  计成副本，副本数不被高估**；台账只随成功修复单调增长。

### 2.3 读法定数量（R）与协商

- 在 N 个放置目标上协商：不可用节点不计入可联系数；
  可联系节点 < R 返回 `quorum_unavailable`；
  达到 R 但没有任何健康副本返回 `no_healthy_replica`（错误链保留每个副本
  的 `missing` / `corrupted` 原因）。
- 只采信通过 SHA-256 校验的副本（按节点标识序取第一个健康者），
  单副本损坏不影响读取，坏数据永远不会返回给调用方。
- 建议 `W + R > N`（演示配置 N=4, W=3, R=2），保证读写副本集合有交集。

### 2.4 后台修复与收敛

`RunRepairOnce` 执行一轮确定性修复：

1. 汇总所有**可用**节点上的清单并集（按标识排序），按节点标识序选择第一个
   健康清单作为权威；损坏清单重命名为 `.quarantine` 隔离（保留证据、停止服务）。
2. 对每个块，在全部可用节点上寻找健康副本；向放置目标中缺失的节点补齐；
   损坏副本先隔离再用健康副本重写，并登记确认台账。
3. 若某块所有副本都损坏，无法无中生有，则在 `RepairReport.Failures` 中
   明确报告 `no_healthy_replica`，绝不静默返回或伪造数据。
4. 报告（检查数、补齐数、隔离数、各节点健康块数）按确定顺序生成；
   收敛后再次运行是**无变更的空操作**，重复运行结果一致。

`StartRepairLoop(ctx, interval)` 周期执行上述流程；节点恢复后自动收敛。

## 3. 版本兼容策略

清单为显式版本化 JSON（当前 `version: 1`）：

```json
{
  "version": 1,
  "blob": "<内容标识>",
  "length": 12345,
  "chunks": [ {"index": 0, "id": "<块标识>", "size": 65536} ]
}
```

兼容规则明确、可测试：

1. **旧版本可读**：`version` 缺失按最早版本 1 处理；
   `1 <= version <= CurrentManifestVersion` 一律按既有语义读取。
2. **新版本不猜测**：`version > CurrentManifestVersion` 返回
   `manifest_too_new`，明确拒绝而不是按旧格式曲解。
3. **未知字段分级处理**：
   - 普通未知字段（如未来加的 `experimental`、块级 `future_hint`）：
     **原样保留**，读改写后写回，不静默丢弃；
   - 以 `critical_` 开头的未知字段表示“不理解就不能正确解释内容”，
     返回 `unknown_field` 拒绝读取。
4. 序列化字段顺序固定、未知字段按键排序，同一清单的编码字节确定。

## 4. 本地验证对照表

| 需求 | 对应测试 |
| --- | --- |
| 相同内容标识一致、切分确定 | `TestChunkIDStable`, `TestSplitDeterministic`, `TestSplitEmpty` |
| 损坏 / 缺失 / 不匹配可区分 | `TestVerifyChunkErrorClassification`, `TestNodeMissingAndCorruption`, `TestNodePutMismatchRejected` |
| 重复写入结论一致 | `TestClusterRepeatWriteSameConclusion` |
| 并发读写 + 节点抖动不丢数据、收敛 | `TestConcurrentWriteReadWithFlappingNode` |
| 写法定数量失败分类 | `TestClusterWriteQuorumFailure` |
| 节点不可用后修复补齐、副本不高估 | `TestNodeOutageThenRepair` |
| 损坏副本隔离并从健康副本恢复 | `TestCorruptionQuarantineAndRepair` |
| 全部副本损坏不可静默恢复 | `TestAllReplicasCorruptIsUnrecoverable` |
| 修复报告确定、幂等 | `TestRepairReportDeterministic` |
| 后台修复循环收敛 | `TestRepairLoopConvergesInBackground` |
| 清单版本兼容 / 未知字段保留 | `TestManifest*` |
| 已确认写入跨“重启”保留 | `TestNodePutGetAndRestart` |

单测日志（`go test -v`）统一打印**内容标识、参与节点与判定依据**，例如：

```
[repair] id=- nodes=[node1 node2 node3 node4] verdict=repair1 rebuilt=3 quarantined=2 ...
[corrupt] id=b88794... nodes=[node1 ...] verdict=corrupted 2 chunk(s) on node1, read still served ...
```
