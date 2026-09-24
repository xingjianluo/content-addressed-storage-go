package cas

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestChunkIDStable(t *testing.T) {
	data := []byte("content-addressed-storage")
	id1 := ChunkID(data)
	id2 := ChunkID(append([]byte(nil), data...))
	if id1 != id2 {
		t.Fatalf("identical content produced different ids: %s vs %s", id1, id2)
	}
	if ChunkID([]byte("other")) == id1 {
		t.Fatal("distinct content produced identical id")
	}
	logVerdict(t, "hash", string(id1), nil, "sha256 stable across repeated writes")
}

func TestSplitDeterministic(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 2*ChunkSize+123)
	id1, c1 := Split(data)
	id2, c2 := Split(append([]byte(nil), data...))
	if id1 != id2 || len(c1) != len(c2) {
		t.Fatalf("Split is not deterministic: id %s vs %s, chunks %d vs %d", id1, id2, len(c1), len(c2))
	}
	if len(c1) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(c1))
	}
	var reassembled bytes.Buffer
	var nodes []string
	for i, c := range c1 {
		if c.Index != i {
			t.Fatalf("chunk index mismatch at %d: %d", i, c.Index)
		}
		if c.ID != c2[i].ID {
			t.Fatalf("chunk %d id differs between splits", i)
		}
		if c.ID != ChunkID(c.Data) {
			t.Fatalf("chunk %d id does not match its bytes", i)
		}
		reassembled.Write(c.Data)
		nodes = nil
		logVerdict(t, "split", string(c.ID), nodes, "chunk %d size=%d sha256 verified", i, len(c.Data))
	}
	if !bytes.Equal(reassembled.Bytes(), data) {
		t.Fatal("chunks do not reassemble to original content")
	}
	logVerdict(t, "blob", string(id1), nil, "3 chunks reassemble to original, blob id verified")
}

func TestSplitEmpty(t *testing.T) {
	id, chunks := Split(nil)
	if len(chunks) != 0 {
		t.Fatalf("empty content must yield zero chunks, got %d", len(chunks))
	}
	want := BlobID(nil, 0)
	if id != want {
		t.Fatalf("empty blob id mismatch: got %s want %s", id, want)
	}
	// 空内容重复切分必须得到相同结论。
	id2, _ := Split([]byte{})
	if id != id2 {
		t.Fatalf("empty split unstable: %s vs %s", id, id2)
	}
}

func TestVerifyChunkErrorClassification(t *testing.T) {
	id := ChunkID([]byte("hello"))

	if err := VerifyChunk(id, []byte("hello")); err != nil {
		t.Fatalf("valid chunk failed verification: %v", err)
	}

	errMissing := VerifyChunk(id, nil)
	if !errors.Is(errMissing, ErrMissingContent) || !IsKind(errMissing, KindMissing) {
		t.Fatalf("nil data must classify as missing, got %v", errMissing)
	}

	errMismatch := VerifyChunk(id, []byte("hellp"))
	if !errors.Is(errMismatch, ErrIDMismatch) || !IsKind(errMismatch, KindIDMismatch) {
		t.Fatalf("tampered data must classify as id_mismatch, got %v", errMismatch)
	}
	if f, ok := errMissing.(*Failure); !ok || !strings.Contains(f.Error(), string(KindMissing)) {
		t.Fatalf("failure message must carry stable kind, got %v", errMissing)
	}
	logVerdict(t, "verify", string(id), nil,
		"missing=%v mismatch=%v (error kinds stable)", IsKind(errMissing, KindMissing), IsKind(errMismatch, KindIDMismatch))
}
