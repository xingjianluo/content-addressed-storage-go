package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ChunkSize 是确定性切分的固定块大小（64 KiB）。
const ChunkSize = 64 * 1024

// ContentID 是内容的稳定标识：内容字节的 SHA-256 十六进制摘要。
type ContentID string

// ChunkRef 是清单中对一个数据块的引用（不含数据本身）。
type ChunkRef struct {
	Index int       `json:"index"` // 块在原始内容中的序号（从 0 开始）
	ID    ContentID `json:"id"`    // 块字节的 SHA-256
	Size  int       `json:"size"`  // 块字节长度
}

// Chunk 是切分后的一个数据块及其标识。
type Chunk struct {
	Index int
	ID    ContentID
	Data  []byte
}

// ChunkID 返回一块字节的内容标识（SHA-256 十六进制）。
func ChunkID(data []byte) ContentID {
	sum := sha256.Sum256(data)
	return ContentID(hex.EncodeToString(sum[:]))
}

// Split 按固定大小确定性切分内容，返回整体内容标识与有序块列表。
// 切分规则与内容长度共同决定切分边界，因此相同内容必得相同结果；
// 整体标识由有序块引用与长度确定（见 BlobID），逐块校验与引用摘要
// 共同构成完整性依据；空内容产生零个块，仍有确定标识。
func Split(data []byte) (ContentID, []Chunk) {
	n := len(data)
	var chunks []Chunk
	if n > 0 {
		count := (n + ChunkSize - 1) / ChunkSize
		chunks = make([]Chunk, 0, count)
		for off, idx := 0, 0; off < n; off, idx = off+ChunkSize, idx+1 {
			end := off + ChunkSize
			if end > n {
				end = n
			}
			body := make([]byte, end-off)
			copy(body, data[off:end])
			chunks = append(chunks, Chunk{
				Index: idx,
				ID:    ChunkID(body),
				Data:  body,
			})
		}
	}
	refs := make([]ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = ChunkRef{Index: c.Index, ID: c.ID, Size: len(c.Data)}
	}
	return BlobID(refs, int64(n)), chunks
}

// BlobID 根据有序块引用与总长度计算整体内容标识。
// 摘要输入为确定性格式：
//
//	"cas-blob-v1\n" + 十进制长度 + "\n" + 每行 "index:size:id" 的块描述。
//
// 这样清单本身也可被校验，且与逐块重组后重算 SHA-256 互为印证。
func BlobID(refs []ChunkRef, length int64) ContentID {
	h := sha256.New()
	fmt.Fprintf(h, "cas-blob-v1\n%d\n", length)
	for _, r := range refs {
		fmt.Fprintf(h, "%d:%d:%s\n", r.Index, r.Size, r.ID)
	}
	return ContentID(hex.EncodeToString(h.Sum(nil)))
}

// VerifyChunk 校验给定字节是否与标识一致：
//   - data == nil（节点上不存在该内容）：KindMissing；
//   - 实际标识与 id 不一致：KindIDMismatch；
//   - 其余情况：nil。
func VerifyChunk(id ContentID, data []byte) error {
	if data == nil {
		return fail(KindMissing, id, "", "chunk has no stored bytes", nil)
	}
	got := ChunkID(data)
	if got != id {
		return fail(KindIDMismatch, id, "",
			fmt.Sprintf("expected id %s but content hashes to %s", id, got), nil)
	}
	return nil
}
