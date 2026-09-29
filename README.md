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
| `pkg/manifest` | 版本化内容清单（v1/v2），保序 JSON 编解码、兼容规则、顶层与块级扩展/未知字段保留（缺省/null/`{}` 三态区分、键序保留、大整数无损）、三路合并 |
| `pkg/store` | 单节点磁盘存储：原子写、读时重算哈希、可用性开关 |
| `pkg/network` | 多节点副本放置、仲裁写、读修复、后台修复循环、并发读改写的协调式 CAS（lineage 头指针 + 三路合并） |
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
   `manifest.ErrUnsupportedVersion`，不做“半懂”的解析，更不会静默重写；
   即使新版本数据里用了 `"attrs": null` 等本程序认识的形态，也一律拒绝
   （`TestFutureManifestRejected`、
   `TestFutureVersionWithNullAttrsRejected`、`TestAttrsRejectionRules`）。
3. **未知字段保留**：读取时把所有非内建字段原样保存在 `Manifest.Unknown`
   （顶层）和 `Entry.Unknown`（条目内），`Marshal` 时原样输出；
   **读—改—写不会丢弃其他/更新版本写入的信息**
   （`TestUnknownFieldsPreservedOnReadModifyWrite`、
   `TestChunkAttrsLosslessRoundTrip`）。
4. **升级幂等**：`Upgrade` 将受支持的旧版本就地提升到 `CurrentVersion`，
   保留全部条目、块级扩展（含缺省/`null`/`{}` 三种 attrs 形态）与未知
   字段；v1 清单升级到 v2 后重复升级字节完全相同
   （`TestUpgradeIdempotentAndLossless`、
   `TestUpgradeV1ToV2LosslessAndIdempotent`、
   `TestUpgradePreservesAttrsShapes`）。
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
  相同）写入。条目上除 `id` / `size` / `attrs` 外的其他字段由
  `Entry.Unknown` 原样保留。

### 数据块级 attrs 的三种形态（彼此区分、原样往返）

线上条目上的 `attrs` 有三种**语义不同**的形态，编解码时用
`Entry.AttrsForm()`（`AttrsOmitted` / `AttrsNull` / `AttrsObject`）区分，
读改写后不会被混成同一种结果：

| 线上形态 | 含义 | 读改写后 |
| --- | --- | --- |
| 没有 `attrs` 键（缺省） | 该块从未携带扩展 | 仍然完全不带该键 |
| `"attrs": null` | 由其他工具写入的**明确空值** | 仍然原样输出 `"attrs":null` |
| `"attrs": {}` 或带键对象 | 显式空对象 / 有扩展 | 仍然是对象（空对象不塌缩成缺省） |

`attrs` 对象内部某个键的**显式 null 值**（如 `"x":null`）同样逐字保留，
不会因为值是 null 就把该键删掉。程序侧用 `Entry.SetAttr(k, v)` 加键会
自动把缺省/null 形态提升为对象形态；`Entry.SetAttrsForm(...)` 可显式设置
形态（null 形态不允许同时藏有键，否则 `Marshal` 返回
`ErrInvalidManifest`）。

### 键顺序与“逐字节 + 内容标识”保留

- 解码使用自定义的保序 JSON 解码器（数字仍走
  `json.Decoder.UseNumber`），记录每个对象（含 attrs、嵌套对象、清单级
  未知字段）在**线上原始字节中的键顺序**。
- 序列化时内建字段按固定 schema 顺序输出（清单：`version`、`name`、
  `entries`；条目：`id`、`size`、`attrs`），其余所有键**按解码到的原始
  顺序**输出；新加的键按字典序追加在原有键之后。
- 因此，把别的工具写出的清单读出来、**只改一个已知字段再写回**时，所有
  未被改动的部分（包括缺省/`null`/`{}` 三种 attrs 形态、嵌套结构、
  null 值、键顺序）产出的序列化字节与原文一致，清单内容标识
  （`SHA-256(序列化字节)`）也一致、可复现
  （`TestChunkAttrsPresenceByteStableEndToEnd`、
  `TestAttrsPresenceShapesRoundTrip`、
  `TestAttrsNullValueInsideObjectPreserved`）。
- 说明：本程序输出的是**紧凑 JSON**（无多余空白）。对外来清单的“原样”是
  语义与字节布局（键顺序、形态、数字逐位）意义上的原样；若外来字节带
  额外空白，首次经本程序读改写后会规范化为紧凑形式，之后即稳定可复现。

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
- 读改写：`GetObject` 拿到的 `*manifest.Manifest` 可直接修改已知字段或
  扩展字段，再用 `network.PutManifest(ctx, man, opt)` 写回。内容寻址
  保证旧版本永不被覆盖；扩展信息（含大整数、嵌套结构、null、三种 attrs
  形态）在重新读出后与写入语义一致，**未改动时重复序列化字节相同**，故
  清单内容标识可复现。
- **并发读改写（多写入方扩展不互相覆盖）**：两个上层工具若都基于同一
  版本各自加扩展、再直接 `PutManifest`，属于“最后写入者赢”，后写会盖掉
  先写。要在并发下不丢扩展，应使用网络内置的协调式 CAS：
  - `net.ModifyManifest(ctx, baseID, func(m) error{ …只改自己的扩展… }, opt)`
    在每个对象的 lineage 上串行执行“读最新头 → 应用修改 → 确认写回”，
    节点短暂不可用导致写未确认时自动重试，直到该轮写入被副本集合全部
    确认。
  - 已经持有完整清单对象的工具可用
    `net.PutManifestCAS(ctx, baseID, man, opt)`：当 `baseID` 之后已有别的
    工具提交了新版本，本程序以公共基版做**三路合并**
    （`manifest.Merge(base, head, yours)`）——两边各自新增的扩展键都会
    保留，同一个键被两边改成不同值才返回 `manifest.MergeConflict`
    （不会静默二选一）；已知结构（名称、条目数/ID/顺序、size）的不兼容
    并发修改同样报冲突。
  - 头指针可经 `net.HeadManifestID(anyIDinLineage)` 查询。协调状态是
    Network 实例内的进程内机制（本演示网络无外部协调服务）；底层内容
    始终是不可变内容寻址块，未确认的写绝不会推进头指针。
  - 对 `attrs` 三种形态的合并也保持区分：只有一边把缺省/null 改成对象，
    结果就是对象；两边把同一 attrs 改成不同形态才冲突；未被任何一方
    触碰的 `null` 条目保持 `null`。
- 已确认的每个清单版本及其数据块仍是普通内容寻址块，在节点恢复后由读
  修复 / 后台修复补齐到副本因子。**各写入方的扩展字段不会互相覆盖或
  丢失，副本最终收敛一致**
  （`TestCoordinatedRMWMergesConcurrentExtensions`、
  `TestCoordinatedCASExplicitMerge`、
  `TestCoordinatedMergeOnNullAttrs`、
  `TestConcurrentReadModifyWritePreservesExtensions`、
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
go test -v ./pkg/manifest -run 'AttrsPresence|AttrsNull|ChunkAttrs|BigInt|Upgrade|Merge|FutureVersion'
go test -race -v ./pkg/network -run 'ChunkAttrsPresence|Coordinated|ReadModifyWrite|Outage'

# 一键端到端演示（写入 5 节点 / RF=3，携带清单级与块级扩展和大整数 → 两个工具并发读改写叠加扩展（协调 CAS 合并） → 副本损坏+节点下线 → 恢复 → 收敛）
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

// Concurrent read-modify-write from multiple tools: attach only your own
// keys; distinct keys from concurrent writers are three-way merged and
// never overwrite each other. Unconfirmed writes (a node briefly down)
// are retried until the full replica set acknowledges.
newID, err = net.ModifyManifest(ctx, newID, func(m *manifest.Manifest) error {
	m.Unknown["x-tool-a"] = "ok"
	for i := range m.Entries {
		m.Entries[i].SetAttr("x-tool-a", json.Number("42"))
	}
	return nil
}, opt)
head := net.HeadManifestID(newID)

go net.RepairLoop(ctx, time.Second, opt) // 后台修复
```
