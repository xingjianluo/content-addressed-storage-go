package cas

import (
	"bytes"
	"errors"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte{1, 2, 3}, 1000)
	blob, chunks := Split(data)
	m := NewManifest(blob, int64(len(data)), chunks)

	raw, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// 序列化必须确定：同一清单重复编码字节一致。
	raw2, _ := MarshalManifest(m)
	if !bytes.Equal(raw, raw2) {
		t.Fatal("manifest marshalling is not byte-deterministic")
	}

	got, err := UnmarshalManifest(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Version != CurrentManifestVersion || got.Blob != blob || got.Length != int64(len(data)) {
		t.Fatalf("manifest fields mismatch: %+v", got)
	}
	if got.ComputedBlobID() != blob {
		t.Fatal("recomputed blob id does not match")
	}
	logVerdict(t, "manifest", string(blob), nil, "version=%d chunks=%d roundtrip stable", got.Version, len(got.Chunks))
}

func TestManifestUnknownFieldsPreserved(t *testing.T) {
	// 模拟一个带可忽略未知字段（顶层 + 块级）的旧版本清单：
	// 读改写后未知字段必须原样保留，不静默丢弃。
	in := []byte(`{
		"version":1,
		"blob":"deadbeef",
		"length":3,
		"chunks":[{"index":0,"id":"aa01","size":3,"future_hint":"keep-me"}],
		"experimental":"also-keep"
	}`)
	m, err := UnmarshalManifest(in)
	if err != nil {
		t.Fatalf("unknown benign fields must be accepted: %v", err)
	}
	if string(m.UnknownFields()["experimental"]) != `"also-keep"` {
		t.Fatalf("top-level unknown field not preserved: %v", m.UnknownFields())
	}
	if string(m.UnknownChunkFields(0)["future_hint"]) != `"keep-me"` {
		t.Fatalf("chunk unknown field not preserved: %v", m.UnknownChunkFields(0))
	}

	out, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	m2, err := UnmarshalManifest(out)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if string(m2.UnknownFields()["experimental"]) != `"also-keep"` ||
		string(m2.UnknownChunkFields(0)["future_hint"]) != `"keep-me"` {
		t.Fatalf("read-modify-write dropped unknown fields: %s", out)
	}
	logVerdict(t, "compat", "deadbeef", nil,
		"benign unknown top/chunk fields preserved through read-modify-write")
}

func TestManifestNewerVersionRejected(t *testing.T) {
	in := []byte(`{"version":99,"blob":"x","length":0,"chunks":[]}`)
	_, err := UnmarshalManifest(in)
	if !errors.Is(err, ErrManifestTooNew) || !IsKind(err, KindManifestTooNew) {
		t.Fatalf("unsupported new version must be KindManifestTooNew, got %v", err)
	}
	logVerdict(t, "compat", "x", nil, "version=99 rejected with manifest_too_new: %v", err)
}

func TestManifestMissingVersionDefaultsToV1(t *testing.T) {
	// 最早格式没有 version 字段：缺失时按版本 1 读取（向后兼容）。
	blob, chunks := Split([]byte("abc"))
	m := NewManifest(blob, 3, chunks)
	raw, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.Replace(raw, []byte(`"version":1,`), nil, 1)
	got, err := UnmarshalManifest(raw)
	if err != nil {
		t.Fatalf("missing version must default to 1 and stay readable: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("expected default version 1, got %d", got.Version)
	}
	logVerdict(t, "compat", string(blob), nil, "missing version field read as v1")
}

func TestManifestCriticalUnknownFieldRejected(t *testing.T) {
	in := []byte(`{"version":1,"blob":"x","length":0,"chunks":[],"critical_encryption":"v2"}`)
	_, err := UnmarshalManifest(in)
	if !errors.Is(err, ErrUnknownField) || !IsKind(err, KindUnknownField) {
		t.Fatalf("critical unknown field must yield KindUnknownField, got %v", err)
	}
	inChunk := []byte(`{"version":1,"blob":"x","length":1,"chunks":[{"index":0,"id":"a","size":1,"critical_codec":"z"}]}`)
	if _, err := UnmarshalManifest(inChunk); !IsKind(err, KindUnknownField) {
		t.Fatalf("critical unknown chunk field must yield KindUnknownField, got %v", err)
	}
	logVerdict(t, "compat", "x", nil, "critical_* fields rejected rather than ignored")
}

func TestManifestTamperedBlobID(t *testing.T) {
	data := []byte("integrity-check-payload")
	blob, chunks := Split(data)
	m := NewManifest(blob, int64(len(data)), chunks)
	m.Blob = ChunkID([]byte("different"))
	_, _, _ = blob, chunks, m
	// 直接篡改块引用长度会使重算标识偏离声明标识。
	m2 := NewManifest(blob, int64(len(data)), chunks)
	m2.Length++
	if m2.ComputedBlobID() == blob {
		t.Fatal("tampering manifest length must change computed blob id")
	}
	logVerdict(t, "manifest", string(blob), nil, "tampered length yields different computed blob id %s", m2.ComputedBlobID())
}
