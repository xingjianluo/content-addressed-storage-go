# 本地内容寻址存储网络（Go）

一套**无外部服务依赖**、可在本机直接运行的内容寻址存储（Content-Addressed
Storage, CAS）网络，用于验证：

- 内容按确定性方式切分并获得稳定标识；相同内容重复写入结论一致；
- 内容损坏、缺失、标识不匹配会被识别，并给出**可区分的失败原因**；
- 多个本地存储节点之间的副本写入、读取、读修复与后台修复；
- 并发写入 / 读取与节点短暂不可用同时发生时，已确认写入不丢失、副本计数
  不虚高，节点恢复后各副本收敛一致；
- 内容清单（manifest）版本演进的向后 / 向前兼容，读改写不静默丢信息；
- 清单级与**每个数据块级**的自定义扩展信息（可嵌套、可携带任意大整数），
  在读改写、并发写入与节点故障恢复中逐位保留。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/chunk` | 确定性定长切分、SHA-256 内容标识、载荷校验 |
| `pkg/manifest` | 版本化内容清单（v1/v2），JSON 编解码、兼容规则、顶层与块级扩展/未知字段保留、大整数无损 |
| `pkg/store` | 单节点磁盘存储：原子写、读时重算哈希、可用性开关 |
| `pkg/network` | 多节点副本放置、仲裁写、读修复、后台修复循环 |
| `cmd/cas-demo` | 一键本地端到端演示（写入 → 故障 → 恢复 → 收敛） |

## 1. 内容标识与完整性规则

- **标识算法**：`ID = 小写十六进制(SHA-256(载荷字节))`（算法标识
  `chunk.AlgorithmSHA256 = "sha256"`）。ID 只取决于内容，与写入时间、
  节点、客户端无关。
- **确定性切分**：对象按固定块大小（默认 64 KiB，`chunk.DefaultSplitSize`，
  可经 `network.Options.ChunkSize` 覆盖）做**纯位置切分**。相同输入与块大小
  必然得到相同边界、相同块数与相同 ID；空输入产生 0 个块；最后一块可以较短。
- **写入校验**：`Store.Put` 在落盘前先校验 `ID == SHA-256(Data)`，不一致直接
  拒绝（`store.ErrIDMismatch`，包裹 `chunk.ErrIDMismatch`），坏 ID 永远进不了
  存储。
- **落盘方式**：临时文件 + `fsync` + 原子 `rename`，避免半成品文件。
- **读取校验**：`Store.Get` 每次读取都重算哈希，不信任文件名或缓存。
- **清单标识**：清单本身也按内容寻址——`manifestID = SHA-256(序列化后的
  清单字节)`；`PutObject` 先写全部数据块，再把清单作为一个普通内容块写入。
  相同对象重复写入必然得到相同 manifestID。

### 可区分的失败原因

| 场景 | 错误（`errors.Is` 可判定） |
| --- | --- |
| 写入时声明 ID 与载荷不符 | `chunk.ErrIDMismatch` / `store.ErrIDMismatch` |
| 文件不存在（该节点无副本） | `store.ErrNotFound` |
| 文件存在但哈希不等于 ID（位腐 / 被篡改 / 截断） | `store.ErrCorrupt` |
| 节点处于模拟不可用状态 | `store.ErrUnavailable` |
| 副本确认数达不到副本因子 | `network.ErrNotEnoughReplicas` |
| 无任何可达节点能判定 | `network.ErrQuorumUnavailable` |
| 存在副本但全部校验失败 | `network.ErrAllReplicasBad` |
| 清单 JSON / 结构非法 | `manifest.ErrInvalidManifest` |
| 清单版本新于本程序可理解的上限 | `manifest.ErrUnsupportedVersion` |

损坏（有文件但哈希错）与缺失（无文件）是两类不同错误，二者都不会被当成
有效数据。

## 2. 副本与修复语义

### 确定性副本放置

- 节点按节点 ID 排序；以内容 ID 的 SHA-256 首字节对节点数取模作为起点做
  轮转，得到该内容的**确定性优先序列**。
- 副本集合 = 优先序列的前 `Replication` 个节点。相同内容在任何客户端、任何
  时间写入，都落到同一组节点（见 `TestDeterministicPlacementAndReplicaCount`）。

### 写入（`Network.PutChunk` / `PutObject`）

- 只向副本集合写入；**副本集合全部确认才算成功**。
- 返回值是“真正持久化了通过校验副本的节点数”。节点宕机或拒绝写入**绝不计数**，
  因此副本数不可能被高估（`TestWriteWithNodesDown`、
  `TestConcurrentWritesSameContent` 会断言失败写入的返回值 < 副本因子）。
- 已存在且校验通过的副本幂等返回；已存在但损坏的副本会被验证过的载荷覆盖。

### 读取与读修复（`GetChunk` / `GetObject`）

- 并行联系全部节点，选用**第一个哈希校验通过**的副本返回；损坏、缺失、
  不可用分别记录。
- 读修复：副本集合中“可达但缺失 / 损坏”的节点，会用刚验证过的良好副本
  立即补写；不可用节点不会被假装修复。
- 全部副本损坏 → `ErrAllReplicasBad`；全部不可达 →
  `ErrQuorumUnavailable`；全部可达但都没有 → `store.ErrNotFound`。

### 后台修复（`Repair` / `RepairLoop`）

- `Repair` 汇总所有可达节点已知的内容 ID，对每个 ID：先找到一个**经哈希
  验证的源副本**，再向副本集合中缺失 / 损坏的节点恢复；宕机节点跳过。
- 修复只搬运“已验证”的字节，损坏副本永远不可能被当作修复源。
- `RepairLoop(ctx, interval, opt)` 周期性执行 `Repair`；节点恢复后无需
  客户端介入即收敛回副本因子（`TestBackgroundRepairConvergesAfterReturn`）。
- 每轮修复对每个 ID 返回 `RepairResult{Healthy, Missing, Corrupt,
  Repaired}`，并通过可注入的 `Logger` 打印 **内容 ID、参与节点与判定依据**。

### 并发与故障下的保证

- 节点内按 ID 加锁，写入原子落盘；可用性用原子标志切换（不会与正在进行的
  IO 互相死锁）。
- 并发写同一内容：结论一致，成功即报满副本因子，失败必报真实且偏低的计数；
  并发读只可能拿到哈希验证通过的数据（`TestConcurrentReadWriteMix`，
  `-race` 下反复运行）。
- 已确认写入不覆盖、不删除已有副本；节点短暂不可用期间数据留在磁盘，恢复
  后由读修复 / 后台修复补齐，最终所有副本集合节点收敛一致。

## 3. 版本兼容策略（清单）

清单是 JSON 对象，当前版本常量：

```
manifest.MinVersion = 1, manifest.CurrentVersion = 2, manifest.MaxSupportedVersion = 2
```

| 版本 | 状态 |
| --- | --- |
| v1（`version` 缺失也视为 v1） | 可读；`Upgrade` 后就地升级到 v2 |
| v2（当前） | 本程序读写的版本；条目支持块级扩展 `attrs` |
| v3+ | 拒绝（`manifest.ErrUnsupportedVersion`） |

规则（代码与测试一一对应）：

1. **旧版可读**：`version` 缺失视为 1（遗留数据），`MinVersion <=
   version <= MaxSupportedVersion` 的数据始终可读
   （`TestLegacyManifestReadable`）。
2. **未来版本拒绝**：`version > MaxSupportedVersion` 返回
   `manifest.ErrUnsupportedVersion`，不做“半懂”的解析，更不会静默重写
   （`TestFutureManifestRejected`、`TestAttrsRejectionRules`）。
3. **未知字段保留**：读取时把所有非内建字段原样保存在 `Manifest.Unknown`
   （顶层）和 `Entry.Unknown`（条目内），`Marshal` 时原样输出；
   **读—改—写不会丢弃其他/更新版本写入的信息**
   （`TestUnknownFieldsPreservedOnReadModifyWrite`、
   `TestChunkAttrsLosslessRoundTrip`）。
4. **升级幂等**：`Upgrade` 将受支持的旧版本就地提升到 `CurrentVersion`，
   保留全部条目、块级扩展与未知字段；v1 清单升级到 v2 后重复升级字节完全
   相同（`TestUpgradeIdempotentAndLossless`、
   `TestUpgradeV1ToV2LosslessAndIdempotent`）。
5. **非法输入分类**：JSON 语法错误、字段类型错误、条目缺 ID、负尺寸、
   `attrs` 不是对象等统一归类为 `manifest.ErrInvalidManifest`，错误消息
   说明具体位置。
6. 序列化按键排序，相同清单产生字节稳定的输出，因此清单 ID 稳定。

## 3.1 扩展信息（清单级与块级）

不同上层工具可以往清单里挂自己的附加信息，分两级：

- **清单级**：`manifest.Manifest.Unknown map[string]any`，即清单对象上除
  `version` / `name` / `entries` 之外的任意字段；通过
  `network.Options.Extensions` 在 `PutObject` 时写入。
- **数据块级**：每个条目上的 `attrs` 对象
  （`manifest.Entry.Attrs map[string]any`），与该块一一对应；通过
  `network.Options.ChunkAttrs`（按块顺序、每块一个 map，长度必须与块数
  相同）写入——某个位置给 `nil` 表示该块不带 `attrs` 键，给非 nil 的
  空 map 则写入显式空对象 `{}`。条目上除 `id` / `size` / `attrs` 外的
  其他字段由 `Entry.Unknown` 原样保留。

线上 JSON 形如：

```json
{
  "version": 2,
  "name": "example.txt",
  "x-tool-policy": {"k": 123},
  "entries": [
    {
      "id": "ab12…",
      "size": 65536,
      "attrs": {
        "x-source": {"offset": "0010", "n": 123456789012345678901234567890},
        "tags": ["a", true, null, -7]
      },
      "x-entry-note": "preserved-too"
    }
  ]
}
```

取值与精度规则：

- 值可以是**嵌套对象、数组、字符串、布尔、null**，以及**任意大小的整数**。
- 解析使用 `json.Decoder.UseNumber`：所有数字以原始数字文本
  （`json.Number`）保存，**不经过 float64**，因此远超 2^53 的大整数
  逐位保持不变，不会变成近似值（`TestChunkAttrsLosslessRoundTrip`、
  `TestBigIntFromAPISerializedDigitForDigit`）。
- 程序侧构造扩展值时可以直接放 `*big.Int`、`json.Number` 或内建整数
  类型，`Marshal` 会把它们序列化成**裸 JSON 数字**（不是字符串）；
  不支持 JSON 的类型（如 channel）返回 `ErrInvalidManifest`，不会被
  悄悄改写。
- **块级扩展的三种形态彼此区分并原样往返**：`attrs` 键缺省、
  显式 `"attrs": null`、对象（含空对象 `{}`）是三种不同的线上形态，
  分别对应 `Entry{Attrs: nil, AttrsNull: false}`、
  `Entry{Attrs: nil, AttrsNull: true}` 与 `Entry{Attrs: 非 nil}`。
  只要某处扩展没被主动修改，读改写后序列化字节与清单内容标识都和
  改动前一致、可复现（`TestAttrsFormsDistinctAndPreserved`、
  `TestNullAttrsUnmodifiedSurvivesKnownFieldEdit`）。条目级与清单级的
  未知字段（包括显式 null 值）同样逐位保留。
- 读改写：`GetObject` 拿到的 `*manifest.Manifest` 可直接修改已知字段或
  扩展字段，再用 `network.PutManifest(ctx, man, opt)` 写回。内容寻址
  保证旧版本永不被覆盖；扩展信息（含大整数、嵌套结构、null、空 attrs
  对象）在重新读出后与写入语义一致，重复序列化字节相同，故清单内容
  标识可复现。
- 并发：多个写入方对同一对象做读改写时，应按“读最新清单 → 合并自己的
  扩展 → `PutManifest` → 失败/落后则重试”的乐观 CAS 方式进行；写入只在
  副本集合全部确认后才算成功，未确认不计数、不覆盖。两个写入方基于
  **同一版本**各自确认了新版本时，两个版本以不同内容标识并存、互不
  覆盖；用 `manifest.MergeExtensions(winner, loser)` 把落败方已确认的
  扩展并入胜方（目标侧键冲突时保留目标侧值；条目按相同块 ID 配对合并；
  合并幂等）再写回，即可保证**双方已确认的扩展都不丢失**
  （`TestConcurrentConflictingWritesMergeWithoutLoss`、
  `TestMergeExtensionsUnionsConfirmedEdits`）。已确认的每个清单版本
  及其数据块在节点恢复后由读修复 / 后台修复补齐到副本因子，各副本
  收敛一致（`TestConcurrentReadModifyWritePreservesExtensions`、
  `TestExtensionsSurviveNodeOutageAndRecovery`）。

## 4. 本地验证方法

需要 Go 1.25+（见 `go.mod`），无需任何外部服务 / 网络 / 数据库。

```bash
# 全部单元测试
go test ./...

# 详细日志：内容 ID、参与节点、判定依据
go test ./pkg/network -v

# 并发安全 + 重复运行一致性
go test -race -count=10 ./...

# 只看本次新增的重点用例
go test -v ./pkg/manifest -run 'ChunkAttrs|BigInt|Upgrade|AttrsForms|NullAttrs|Merge'
go test -race -v ./pkg/network -run 'ChunkAttrs|ReadModifyWrite|Outage|ConflictingWrites'

# 一键端到端演示（写入 5 节点 / RF=3，携带清单级与块级扩展和大整数 → 副本损坏+节点下线 → 恢复 → 收敛）
go run ./cmd/cas-demo
```

测试使用 `t.TempDir()`，节点数据彼此隔离、用后即删；测试日志统一包含：

- `id=`：内容标识（SHA-256 hex）；
- `nodes=` / `served-by=` / `source=` / `repaired=`：参与与判定节点；
- `basis=`：判定依据（如 `verified-persistent-copies`、
  `hash-verified-replica`、`restored-from-verified-source`、
  `version>MaxSupportedVersion` 等）。

断言聚焦于**状态收敛**（修复后副本集合上的健康副本数 == 副本因子）与
**错误分类稳定**（固定错误类型，`errors.Is` 判定），不依赖时序，因此重复
运行结果一致。

## 最小使用示例

```go
nodes := // ...store.New(id, dir)...
net := network.New(nodes...)
opt := network.Options{
	Replication: 3,
	ChunkSize:   64 * 1024,
	// Object-level extensions (arbitrary JSON, huge ints via json.Number / *big.Int).
	Extensions: map[string]any{"x-author": "tool-a", "x-epoch": json.Number("1700000000000000")},
	// One attrs map per chunk, in chunk order.
	ChunkAttrs: []map[string]any{{"x-kind": "head"}, {"x-kind": "body"}},
}

manifestID, _, err := net.PutObject(ctx, "file.bin", data, opt)
payload, man, err := net.GetObject(ctx, manifestID, opt)

// Read-modify-write: edit, then persist the manifest as a new content ID;
// all unknown/extension fields survive automatically.
man.Unknown["x-reviewed"] = true
newID, err := net.PutManifest(ctx, man, opt)

go net.RepairLoop(ctx, time.Second, opt) // 后台修复
```
