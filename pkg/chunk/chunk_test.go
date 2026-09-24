package chunk

import (
	"errors"
	"fmt"
	"testing"
)

func TestComputeIDStable(t *testing.T) {
	data := []byte("the quick brown fox")
	id1 := ComputeID(data)
	id2 := ComputeID(append([]byte(nil), data...))
	if id1 != id2 {
		t.Fatalf("id not stable: %s vs %s", id1, id2)
	}
	want := ID("9ecb36561341d18eb65484e833efea61edc74b84cf5e6ae1b81c63533e25fc8f")
	if id1 != want {
		t.Fatalf("unexpected id: got=%s want=%s basis=hex(sha256(payload))", id1, want)
	}
	t.Logf("content=%q id=%s basis=hex(sha256(payload))", data, id1)
}

func TestSplitDeterministicAndJoin(t *testing.T) {
	data := make([]byte, 250)
	for i := range data {
		data[i] = byte(i % 7)
	}
	c1 := Split(data, 64)
	c2 := Split(append([]byte(nil), data...), 64)
	if len(c1) != 4 || len(c2) != 4 {
		t.Fatalf("expected 4 chunks, got %d/%d", len(c1), len(c2))
	}
	for i := range c1 {
		if c1[i].ID != c2[i].ID {
			t.Fatalf("chunk %d id differs across splits: %s vs %s", i, c1[i].ID, c2[i].ID)
		}
		if err := Verify(c1[i]); err != nil {
			t.Fatalf("chunk %d verify: %v", i, err)
		}
		t.Logf("chunk index=%d id=%s size=%d basis=fixed-64B-positional-boundary",
			i, c1[i].ID, len(c1[i].Data))
	}
	if got := Join(c1); string(got) != string(data) {
		t.Fatalf("join round-trip mismatch")
	}
	// Empty input => no chunks, deterministically.
	if got := Split(nil, 64); len(got) != 0 {
		t.Fatalf("empty input should yield no chunks, got %d", len(got))
	}
}

func TestVerifyRejectsMismatch(t *testing.T) {
	good := Chunk{ID: ComputeID([]byte("payload")), Data: []byte("payload")}
	if err := Verify(good); err != nil {
		t.Fatalf("good chunk rejected: %v", err)
	}
	bad := Chunk{ID: good.ID, Data: []byte("tampered payload")}
	err := Verify(bad)
	if !errors.Is(err, ErrIDMismatch) {
		t.Fatalf("want ErrIDMismatch, got %v", err)
	}
	t.Logf("claimed-id=%s actual-id=%s reason=%v basis=hash-recomputed-on-payload",
		good.ID, ComputeID(bad.Data), err)
}

func TestDuplicateWriteSameConclusion(t *testing.T) {
	data := []byte(fmt.Sprintf("same-content-%d", 42))
	a := Split(data, 16)
	b := Split(data, 16)
	if len(a) != len(b) {
		t.Fatalf("chunk count differs")
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("duplicate write produced different id at %d", i)
		}
	}
}
