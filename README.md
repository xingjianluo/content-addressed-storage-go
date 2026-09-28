# 本地内容寻址存储网络（Go）

一套**无外部服务依赖**、可在本机直接运行的内容寻址存储（Content-Addressed
Storage, CAS）网络，用于验证：

- 内容按确定性方式切分并获得稳定标识；相同内容重复写入结论一致；
- 内容损坏、缺失、标识不匹配会被识别，并给出**可区分的失败原因**；
- 多个本地存储节点之间的副本写入、读取、读修复与后台修复；
- 并发写入 / 读取与节点短暂不可用同时发生时，已确认写入不丢失、副本计数
  不虚高，节点恢复后各副本收敛一致；
- 内容清单（manifest）版本演进的向后 / 向前兼容，读改写不静默丢信息；
- **对象级与数据块级（per-chunk）扩展属性**：不同上层工具写入的任意 JSON
  扩展（嵌套对象、数组、布尔、null、超大整数）在读改写、复制与修复中逐位
  保留，大整数无精度丢失。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/chunk` | 确定性定长切分、SHA-256 内容标识、载荷校验 |
| `pkg/manifest` | 版本化内容清单，JSON 编解码、兼容规则、对象级/块级未知字段保留、大整数逐位解析 |
| `pkg/store` | 单节点磁盘存储：原子写、读时重算哈希、可用性开关 |
| `pkg/network` | 多节点副本放置、仲裁写、读修复、后台修复、对象原子读改写（UpdateObject） |
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
| 对未注册过的对象名调用 `UpdateObject` | `network.ErrObjectNotFound` |

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

## 3. 扩展属性规则（对象级 / 数据块级）

不同的上层工具可以往清单里塞各自的附加信息。扩展分两级，规则完全一致：

- **对象级**：清单 JSON 顶层除 `version` / `name` / `entries` 之外的任意字段，
  读入后保存在 `Manifest.Unknown`。
- **数据块级**：`entries[]` 每个元素除内建的 `id` / `size` 之外的任意字段，
  读入后保存在 `manifest.Entry.Unknown`。即每个数据块都能独立携带一份扩展。

```json
{
  "version": 2,
  "name": "example.bin",
  "entries": [
    {"id": "<sha256-hex>", "size": 123,
     "x-tags": ["hot", "audit"],
     "x-meta": {"nested": {"deep": true}, "list": [1, 2, 3], "empty": null},
     "x-big": 9007199254740993}
  ],
  "x-object-level": {"policy": "keep", "quorum": 7}
}
```

约定与保证：

1. **原样保留**：程序不认识的字段一律原样保留（嵌套对象、数组、布尔、null
   皆可），`Marshal` 时原样输出。读取 → 修改某个已知字段 → 写回后，两级扩展
   中所有未触碰的内容都与原来一致，绝不会因为不认识而被静默删除或改写
   （`TestChunkExtensionsLosslessRoundTrip`、
   `TestChunkExtensionsEndToEndReadModifyWrite`）。
2. **大整数逐位精确**：解析使用 `json.Decoder.UseNumber()`，数字以
   `json.Number` 持有，不经过 `float64`，因此超过 `2^53`（甚至几十位）的
   整数逐位不变、不会变成近似值；序列化时也按原数字文本输出
   （`TestLargeIntegersExactAcrossRewrite`）。需要按整数使用内建 `size` /
   `version` 时才做精确整数转换，非整数一律判为非法。
3. **可复现**：map 键按固定顺序输出；同一份清单重复序列化字节一致，
   “原样再写”与“先读出再写回”产生的字节和清单内容标识（manifest ID）都相同
   （`TestSerializationAndContentIDReproducible`）。
4. **命名空间约定**：内建字段名（顶层 `version`/`name`/`entries`，条目
   `id`/`size`）保留给本格式；保留字段若与内建字段同名会被判为
   `manifest.ErrInvalidManifest`，避免伪装成内建字段。上层工具自定义字段
   建议自带前缀（示例中使用 `x-`）以免与未来内建字段冲突。
5. **扩展随副本复制与修复**：扩展是清单字节的一部分，而清单本身按内容寻址、
   作为普通块参与副本放置、读修复与后台修复；因此节点宕机 / 丢副本恢复后，
   各副本上恢复出的是**逐字节相同**、扩展完整的清单
   （`TestExtensionsSurviveNodeOutageAndRepair`）。

### 并发读改写（`UpdateObject`）

- `network.UpdateObject(ctx, name, opt, mutator)` 在**同一对象名**上串行化：
  内部按对象名加锁，先重新读取该对象最新已提交的清单头（object head），
  应用 `mutator` 改动，校验并把新清单作为新的内容寻址块写入，再推进头指针。
  多个写入方并发读改写时，每个写入方看到的都是此前最新版本，扩展互相追加而
  不会覆盖丢失（`TestConcurrentReadModifyWriteNoLostExtensions`，
  `-race` 下反复运行）。
- 新清单写不满副本因子时返回 `ErrNotEnoughReplicas`、**不推进头指针**；旧的
  已确认清单仍可读，节点恢复后重试即可，扩展不丢
  （`TestUpdateObjectFailureDoesNotAdvanceHead`、
  `TestConcurrentUpdatesDuringNodeFlap`）。
- 头指针注册表是**本网络实例内**的并发控制；跨进程的独立网络实例之间不共享
  头指针（磁盘上的清单块与扩展本身仍然完整、可按 manifest ID 直接读取）。

## 4. 版本兼容策略（清单）

清单是 JSON 对象，当前版本常量：

```
manifest.MinVersion = 1, manifest.CurrentVersion = 2, manifest.MaxSupportedVersion = 2
```

版本含义：v1 = `{version,name,entries:[{id,size}]}`（加顶层保留字段）；
v2 在 v1 基础上允许每个 `entries[]` 元素携带自己的块级扩展字段。规则（代码与测试一一对应）：

1. **旧版可读**：`version` 缺失视为 1（遗留数据），`MinVersion <=
   version <= MaxSupportedVersion` 的数据始终可读
   （`TestLegacyManifestReadable`）。
2. **未来版本拒绝**：`version > MaxSupportedVersion` 返回
   `manifest.ErrUnsupportedVersion`，不做“半懂”的解析，更不会静默重写——即使
   里面的扩展字段看起来眼熟（`TestFutureManifestRejected`、
   `TestFutureVersionStillRejectedWithExtensions`）。
3. **未知字段保留**：读取时把对象级非内建字段保存在 `Manifest.Unknown`、
   块级非内建字段保存在 `Entry.Unknown`，`Marshal` 时原样输出；**读—改—写不
   会丢弃其他/更新版本写入的信息**（顶层：
   `TestUnknownFieldsPreservedOnReadModifyWrite`；块级：
   `TestChunkExtensionsLosslessRoundTrip`）。
4. **升级幂等**：`Upgrade` 将受支持的旧版本就地提升到 `CurrentVersion`，
   保留全部条目与两级扩展；v1 数据升级到 v2 后重复升级结果逐字节相同
   （`TestUpgradeIdempotentAndLossless`、
   `TestUpgradeV1ToV2KeepsEverything`）。
5. **非法输入分类**：JSON 语法错误、字段类型错误、条目缺 ID、负尺寸等
   统一归类为 `manifest.ErrInvalidManifest`，错误消息说明具体位置。
6. 序列化按键排序、数字按原文输出，相同清单产生字节稳定的输出，因此
   清单 ID 稳定。

## 5. 本地验证方法

需要 Go 1.25+（见 `go.mod`），无需任何外部服务 / 网络 / 数据库。

```bash
# 全部单元测试
go test ./...

# 详细日志：内容 ID、参与节点、判定依据
go test ./pkg/network -v

# 并发安全 + 重复运行一致性
go test -race -count=10 ./...

# 只跑本次新增的扩展 / 大整数 / 并发读改写 / 故障恢复用例
go test ./pkg/manifest -v -run 'ChunkExtensions|LargeIntegers|SerializationAndContentID|UpgradeV1|FutureVersionStill'
go test ./pkg/network  -v -run 'ChunkExtensionsEndToEnd|ConcurrentReadModifyWrite|ExtensionsSurviveNodeOutage|UpdateObjectFailure|ConcurrentUpdatesDuringNodeFlap'

# 确认既有用例（读写、修复、版本兼容）无回归
go test -count=1 ./...

# 一键端到端演示（写入 5 节点 / RF=3 → 读改写加块级扩展 → 副本损坏+节点下线 → 恢复 → 收敛）
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
opt := network.Options{Replication: 3, ChunkSize: 64 * 1024}

manifestID, _, err := net.PutObject(ctx, "file.bin", data, opt)
payload, _, err := net.GetObject(ctx, manifestID, opt)

// 原子读改写：给对象和第一个数据块附加扩展（大整数用 json.Number）。
newID, _, err := net.UpdateObject(ctx, "file.bin", opt, func(m *manifest.Manifest) error {
	m.Unknown["x-owner"] = "tool-a"
	m.Entries[0].Unknown["x-checksum"] = json.Number("123456789012345678901234567890")
	return nil
})

go net.RepairLoop(ctx, time.Second, opt) // 后台修复
```
