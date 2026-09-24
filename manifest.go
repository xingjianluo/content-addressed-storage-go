package cas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// CurrentManifestVersion 是当前软件支持的清单格式版本。
const CurrentManifestVersion = 1

// 兼容规则（显式、可测试）：
//
//  1. version 缺失时按最早版本 1 处理（保证最早格式内容可读）。
//  2. 1 <= version <= CurrentManifestVersion：正常读取；未知字段被保留，
//     读改写后原样写回，不静默丢弃。
//  3. version > CurrentManifestVersion：返回 KindManifestTooNew，拒绝猜测。
//  4. 任何未知字段若以 critical_ 前缀开头，表示该字段影响内容语义、
//     不可忽略：返回 KindUnknownField。普通未知字段仅保留、不解释。
const criticalFieldPrefix = "critical_"

// 已知的顶层字段与块级字段（其余按上述兼容规则处理）。
var knownTopFields = map[string]struct{}{
	"version": {}, "blob": {}, "length": {}, "chunks": {},
}

var knownChunkFields = map[string]struct{}{
	"index": {}, "id": {}, "size": {},
}

// Manifest 是一份内容的版本化清单：描述整体标识、长度与块引用。
// unknown 保存解析时遇到的可忽略未知字段，读改写时原样保留。
type Manifest struct {
	Version int
	Blob    ContentID
	Length  int64
	Chunks  []ChunkRef
	unknown unknownBag
}

// unknownBag 保留未知字段（顶层与块级），保证读改写不丢信息。
type unknownBag struct {
	top    map[string]json.RawMessage
	chunks []map[string]json.RawMessage
}

// NewManifest 构造当前版本的清单（由切分结果生成块引用）。
// 调用方通常传入 Split 返回的整体标识；此处不重算，标识语义由
// Split/BlobID 两个确定性函数各自负责。
func NewManifest(blob ContentID, length int64, chunks []Chunk) *Manifest {
	refs := make([]ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = ChunkRef{Index: c.Index, ID: c.ID, Size: len(c.Data)}
	}
	return &Manifest{
		Version: CurrentManifestVersion,
		Blob:    blob,
		Length:  length,
		Chunks:  refs,
	}
}

// UnknownFields 返回保留的未知顶层字段（只读语义，调用方不应修改）。
func (m *Manifest) UnknownFields() map[string]json.RawMessage { return m.unknown.top }

// UnknownChunkFields 返回某块保留的未知字段（只读语义）。
func (m *Manifest) UnknownChunkFields(index int) map[string]json.RawMessage {
	if index < 0 || index >= len(m.unknown.chunks) {
		return nil
	}
	return m.unknown.chunks[index]
}

// ComputedBlobID 按清单中的块引用与长度重算整体标识，供完整性校验。
func (m *Manifest) ComputedBlobID() ContentID {
	return BlobID(m.Chunks, m.Length)
}

// MarshalManifest 序列化清单；字段顺序固定、未知字段按键排序原样写回，
// 因此同一逻辑清单的序列化结果字节确定，重复运行一致。
func MarshalManifest(m *Manifest) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	fmt.Fprintf(&b, `"version":%d,`, m.Version)
	writeJSONString(&b, "blob")
	b.WriteByte(':')
	writeJSONString(&b, string(m.Blob))
	b.WriteByte(',')
	fmt.Fprintf(&b, `"length":%d,`, m.Length)
	b.WriteString(`"chunks":[`)
	for i, r := range m.Chunks {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		fmt.Fprintf(&b, `"index":%d,`, r.Index)
		writeJSONString(&b, "id")
		b.WriteByte(':')
		writeJSONString(&b, string(r.ID))
		b.WriteByte(',')
		fmt.Fprintf(&b, `"size":%d`, r.Size)
		writeUnknownFields(&b, chunkUnknown(m, i))
		b.WriteByte('}')
	}
	b.WriteByte(']')
	writeUnknownFields(&b, m.unknown.top)
	b.WriteByte('}')
	return b.Bytes(), nil
}

func chunkUnknown(m *Manifest, i int) map[string]json.RawMessage {
	if i < len(m.unknown.chunks) {
		return m.unknown.chunks[i]
	}
	return nil
}

// writeUnknownFields 以按键排序的确定性顺序追加 ,"k":raw 字段。
func writeUnknownFields(b *bytes.Buffer, fields map[string]json.RawMessage) {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteByte(',')
		writeJSONString(b, k)
		b.WriteByte(':')
		b.Write(fields[k])
	}
}

// writeJSONString 写出一个 JSON 字符串（含引号），复用 encoding/json 的转义规则。
func writeJSONString(b *bytes.Buffer, s string) {
	raw, err := json.Marshal(s)
	if err != nil {
		// 字符串不可能产生 marshal 错误。
		panic(err)
	}
	b.Write(raw)
}

// UnmarshalManifest 按文档级兼容规则解析清单。
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, fail(KindIDMismatch, "", "", "manifest is not valid JSON", err)
	}

	// 版本处理：缺失视为最早版本 1。
	version := 1
	if raw, ok := top["version"]; ok {
		if err := json.Unmarshal(raw, &version); err != nil {
			return nil, fail(KindManifestTooNew, "", "", "manifest version is not an integer", err)
		}
	}
	if version > CurrentManifestVersion {
		return nil, fail(KindManifestTooNew, "", "",
			fmt.Sprintf("manifest version %d > supported %d", version, CurrentManifestVersion), nil)
	}

	// 顶层未知字段分类：critical_ 前缀不可忽略，其余保留。
	unknownTop := map[string]json.RawMessage{}
	for k, raw := range top {
		if _, known := knownTopFields[k]; known {
			continue
		}
		if isCritical(k) {
			return nil, fail(KindUnknownField, "", "",
				fmt.Sprintf("unknown top-level field %q must be understood before reading", k), nil)
		}
		unknownTop[k] = append(json.RawMessage(nil), raw...)
	}

	m := &Manifest{Version: version, unknown: unknownBag{top: unknownTop}}

	if raw, ok := top["blob"]; ok {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fail(KindIDMismatch, "", "", "manifest blob is not a string", err)
		}
		m.Blob = ContentID(s)
	}
	if raw, ok := top["length"]; ok {
		if err := json.Unmarshal(raw, &m.Length); err != nil {
			return nil, fail(KindIDMismatch, m.Blob, "", "manifest length is not an integer", err)
		}
	}

	if raw, ok := top["chunks"]; ok {
		var rawChunks []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &rawChunks); err != nil {
			return nil, fail(KindIDMismatch, m.Blob, "", "manifest chunks is not an array of objects", err)
		}
		m.Chunks = make([]ChunkRef, len(rawChunks))
		m.unknown.chunks = make([]map[string]json.RawMessage, len(rawChunks))
		for i, rc := range rawChunks {
			unk := map[string]json.RawMessage{}
			for k, v := range rc {
				if _, known := knownChunkFields[k]; known {
					continue
				}
				if isCritical(k) {
					return nil, fail(KindUnknownField, m.Blob, "",
						fmt.Sprintf("unknown chunk field %q must be understood before reading", k), nil)
				}
				unk[k] = append(json.RawMessage(nil), v...)
			}
			m.unknown.chunks[i] = unk
			if v, ok := rc["index"]; ok {
				if err := json.Unmarshal(v, &m.Chunks[i].Index); err != nil {
					return nil, fail(KindIDMismatch, m.Blob, "", "chunk index is not an integer", err)
				}
			}
			if v, ok := rc["id"]; ok {
				var s string
				if err := json.Unmarshal(v, &s); err != nil {
					return nil, fail(KindIDMismatch, m.Blob, "", "chunk id is not a string", err)
				}
				m.Chunks[i].ID = ContentID(s)
			}
			if v, ok := rc["size"]; ok {
				if err := json.Unmarshal(v, &m.Chunks[i].Size); err != nil {
					return nil, fail(KindIDMismatch, m.Blob, "", "chunk size is not an integer", err)
				}
			}
		}
	}

	return m, nil
}

func isCritical(name string) bool {
	return len(name) >= len(criticalFieldPrefix) && name[:len(criticalFieldPrefix)] == criticalFieldPrefix
}
