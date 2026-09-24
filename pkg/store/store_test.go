package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

func newTestStore(t *testing.T, id string) *Store {
	t.Helper()
	root := filepath.Join(t.TempDir(), id)
	s, err := New(id, root)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutGetVerifyAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "n1")
	c := chunk.Chunk{ID: chunk.ComputeID([]byte("hello-store")), Data: []byte("hello-store")}

	if err := s.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, c); err != nil {
		t.Fatalf("duplicate put must be idempotent: %v", err)
	}
	got, err := s.Get(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != string(c.Data) {
		t.Fatalf("data mismatch")
	}
	ok, err := s.Has(ctx, c.ID)
	if !ok || err != nil {
		t.Fatalf("Has = %v,%v", ok, err)
	}
	ids, _ := s.List(ctx)
	if len(ids) != 1 || ids[0] != c.ID {
		t.Fatalf("List = %v", ids)
	}
	t.Logf("node=%s id=%s basis=atomic-write+read-time-hash-verified", s.ID(), c.ID)
}

func TestPutRejectsWrongID(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "n1")
	c := chunk.Chunk{ID: chunk.ComputeID([]byte("other")), Data: []byte("actual-data")}
	err := s.Put(ctx, c)
	if !errors.Is(err, ErrIDMismatch) {
		t.Fatalf("want ErrIDMismatch, got %v", err)
	}
	t.Logf("node=%s claimed-id=%s reason=%v", s.ID(), c.ID, err)
}

func TestGetClassifiesMissingAndCorrupt(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "n1")
	missing := chunk.ComputeID([]byte("never-written"))

	if _, err := s.Get(ctx, missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	t.Logf("node=%s id=%s reason=not-found basis=no-file", s.ID(), missing)

	c := chunk.Chunk{ID: chunk.ComputeID([]byte("payload-x")), Data: []byte("payload-x")}
	if err := s.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.Corrupt(ctx, c.ID, []byte("garbage")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, c.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	ok, err := s.Has(ctx, c.ID)
	if ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt copy must not count as present: ok=%v err=%v", ok, err)
	}
	t.Logf("node=%s id=%s reason=corrupt basis=file-exists-but-hash-mismatch", s.ID(), c.ID)

	// A verified rewrite repairs the corrupt slot.
	if err := s.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Has(ctx, c.ID); !ok {
		t.Fatalf("corrupt slot was not restored by verified put")
	}
}

func TestUnavailableFailsAllOps(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "n1")
	c := chunk.Chunk{ID: chunk.ComputeID([]byte("z")), Data: []byte("z")}
	if err := s.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	s.SetAvailable(false)
	if _, err := s.Get(ctx, c.ID); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	if err := s.Put(ctx, c); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	if _, err := s.List(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
	s.SetAvailable(true)
	if _, err := s.Get(ctx, c.ID); err != nil {
		t.Fatalf("data must survive outage: %v", err)
	}
	t.Logf("node=%s id=%s basis=availability-flag-without-touching-disk", s.ID(), c.ID)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "n1")
	c := chunk.Chunk{ID: chunk.ComputeID([]byte("del")), Data: []byte("del")}
	if err := s.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, c.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
}
