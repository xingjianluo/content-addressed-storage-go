package cas

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Node 是一个本地存储节点，数据落盘在独立目录中：
//
//	<dir>/chunks/<chunkID>      块字节
//	<dir>/manifests/<blobID>    清单 JSON
//
// 可通过 SetAvailable 模拟节点短暂不可用与恢复；不可用时一切读写
// 立即返回 KindQuorum，不阻塞调用方。
type Node struct {
	id        string
	dir       string
	mu        sync.RWMutex
	available bool
	chunks    map[ContentID]struct{}
	blobs     map[ContentID]struct{}
}

// NewNode 在 dir 目录上创建（或打开）一个标识为 id 的本地节点。
// 目录不存在时创建；已存在时加载其中的现存数据，模拟节点重启后恢复。
func NewNode(id, dir string) (*Node, error) {
	if err := os.MkdirAll(filepath.Join(dir, "chunks"), 0o755); err != nil {
		return nil, fmt.Errorf("cas: create node dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "manifests"), 0o755); err != nil {
		return nil, fmt.Errorf("cas: create node dir: %w", err)
	}
	n := &Node{
		id:        id,
		dir:       dir,
		available: true,
		chunks:    map[ContentID]struct{}{},
		blobs:     map[ContentID]struct{}{},
	}
	if err := n.load(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) load() error {
	entries, err := os.ReadDir(filepath.Join(n.dir, "chunks"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == quarantineSuffix {
			continue // 已隔离的副本不参与正常服务
		}
		n.chunks[ContentID(e.Name())] = struct{}{}
	}
	entries, err = os.ReadDir(filepath.Join(n.dir, "manifests"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == quarantineSuffix {
			continue // 已隔离的副本不参与正常服务
		}
		n.blobs[ContentID(e.Name())] = struct{}{}
	}
	return nil
}

func (n *Node) ID() string { return n.id }

func (n *Node) SetAvailable(ok bool) {
	n.mu.Lock()
	n.available = ok
	n.mu.Unlock()
}

func (n *Node) Available() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.available
}

func (n *Node) unavailable(id ContentID, op string) error {
	return fail(KindQuorum, id, n.id, fmt.Sprintf("node unavailable during %s", op), nil)
}

func (n *Node) chunkPath(id ContentID) string {
	return filepath.Join(n.dir, "chunks", string(id))
}

func (n *Node) manifestPath(blob ContentID) string {
	return filepath.Join(n.dir, "manifests", string(blob))
}

// quarantineSuffix 附加在被隔离文件名后，使其停止对外服务但保留证据。
const quarantineSuffix = ".quarantine"

// quarantine 将子目录（chunks/manifests）中标识为 id 的文件重命名隔离。
// 返回是否实际执行了隔离；操作幂等。
func (n *Node) quarantine(id ContentID, sub string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	src := filepath.Join(n.dir, sub, string(id))
	if _, err := os.Stat(src); err != nil {
		return false
	}
	dst := src + quarantineSuffix
	// 若隔离目标已存在（重复隔离），先移除旧文件，保证重命名可成功。
	if _, err := os.Stat(dst); err == nil {
		_ = os.Remove(dst)
	}
	if err := os.Rename(src, dst); err != nil {
		return false
	}
	if sub == "chunks" {
		delete(n.chunks, id)
	} else {
		delete(n.blobs, id)
	}
	return true
}

// PutChunk 在校验通过后以原子替换方式写入块；
// 标识与数据不匹配返回 KindIDMismatch，且不会触碰已有数据。
func (n *Node) PutChunk(id ContentID, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return n.unavailable(id, "PutChunk")
	}
	if err := VerifyChunk(id, data); err != nil {
		return err
	}
	if err := atomicWrite(n.chunkPath(id), data); err != nil {
		return err
	}
	n.chunks[id] = struct{}{}
	return nil
}

// GetChunk 读取并校验块：
//   - 节点不可用：KindQuorum；
//   - 内容不存在：KindMissing；
//   - 落盘字节与标识不一致：KindCorrupted。
func (n *Node) GetChunk(id ContentID) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.available {
		return nil, n.unavailable(id, "GetChunk")
	}
	if _, ok := n.chunks[id]; !ok {
		return nil, fail(KindMissing, id, n.id, "chunk not found on node", nil)
	}
	data, err := os.ReadFile(n.chunkPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fail(KindMissing, id, n.id, "chunk file disappeared", err)
		}
		return nil, err
	}
	if ChunkID(data) != id {
		return nil, fail(KindCorrupted, id, n.id,
			"stored bytes do not hash to chunk id", nil)
	}
	return data, nil
}

// HasChunk 仅报告块记录是否存在，不保证字节健康（健康判定以 GetChunk 为准）。
func (n *Node) HasChunk(id ContentID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.chunks[id]
	return ok
}

// DeleteChunk 删除块（幂等），用于模拟副本缺失。
func (n *Node) DeleteChunk(id ContentID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return n.unavailable(id, "DeleteChunk")
	}
	err := os.Remove(n.chunkPath(id))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	delete(n.chunks, id)
	return nil
}

// CorruptChunk 确定性破坏块的落盘字节：翻转首字节；
// 对单字节 0x00 的极端情况退化为追加 0xFF。结果稳定且必然改变标识。
func (n *Node) CorruptChunk(id ContentID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return n.unavailable(id, "CorruptChunk")
	}
	if _, ok := n.chunks[id]; !ok {
		return fail(KindMissing, id, n.id, "cannot corrupt absent chunk", nil)
	}
	data, err := os.ReadFile(n.chunkPath(id))
	if err != nil {
		return err
	}
	if len(data) == 0 {
		data = append(data, 0xFF)
	} else {
		data[0] ^= 0xFF
	}
	if err := atomicWrite(n.chunkPath(id), data); err != nil {
		return err
	}
	return nil
}

// PutManifest 写入清单。写入前校验清单声明的 blob 与其块引用
// 重算出的标识一致，不一致返回 KindIDMismatch。
func (n *Node) PutManifest(blob ContentID, m *Manifest) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.available {
		return n.unavailable(blob, "PutManifest")
	}
	if m.Blob != blob {
		return fail(KindIDMismatch, blob, n.id,
			fmt.Sprintf("manifest blob field %s disagrees with write key %s", m.Blob, blob), nil)
	}
	if computed := m.ComputedBlobID(); computed != blob {
		return fail(KindIDMismatch, blob, n.id,
			fmt.Sprintf("manifest chunk refs compute blob id %s, expected %s", computed, blob), nil)
	}
	data, err := MarshalManifest(m)
	if err != nil {
		return err
	}
	if err := atomicWrite(n.manifestPath(blob), data); err != nil {
		return err
	}
	n.blobs[blob] = struct{}{}
	return nil
}

// GetManifest 读取清单并做版本兼容解析；随后校验请求 key、声明 blob、
// 由块引用重算出的 blob 三者一致，否则返回 KindIDMismatch。
func (n *Node) GetManifest(blob ContentID) (*Manifest, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.available {
		return nil, n.unavailable(blob, "GetManifest")
	}
	if _, ok := n.blobs[blob]; !ok {
		return nil, fail(KindMissing, blob, n.id, "manifest not found on node", nil)
	}
	data, err := os.ReadFile(n.manifestPath(blob))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fail(KindMissing, blob, n.id, "manifest file disappeared", err)
		}
		return nil, err
	}
	m, err := UnmarshalManifest(data)
	if err != nil {
		return nil, err
	}
	if m.Blob != blob {
		return nil, fail(KindIDMismatch, blob, n.id,
			fmt.Sprintf("manifest blob field %s disagrees with read key", m.Blob), nil)
	}
	if computed := m.ComputedBlobID(); computed != blob {
		return nil, fail(KindCorrupted, blob, n.id,
			fmt.Sprintf("manifest refs hash to %s, manifest content is corrupt", computed), nil)
	}
	return m, nil
}

func (n *Node) HasManifest(blob ContentID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.blobs[blob]
	return ok
}

// StoredChunkIDs 以字典序返回节点记录的块标识，顺序确定。
func (n *Node) StoredChunkIDs() []ContentID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]ContentID, 0, len(n.chunks))
	for id := range n.chunks {
		out = append(out, id)
	}
	sortContentIDs(out)
	return out
}

// StoredBlobIDs 以字典序返回节点记录的清单标识，顺序确定。
func (n *Node) StoredBlobIDs() []ContentID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]ContentID, 0, len(n.blobs))
	for id := range n.blobs {
		out = append(out, id)
	}
	sortContentIDs(out)
	return out
}

func sortContentIDs(ids []ContentID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

// atomicWrite 先写临时文件再重命名，保证节点上的文件要么是旧完整内容、
// 要么是新完整内容，不会出现写入中途的半截数据。
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
