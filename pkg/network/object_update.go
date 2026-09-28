// Object-level atomic read-modify-write support.
//
// Upper-layer tools update an object's manifest extensions by reading the
// current manifest, changing a known field, and writing it back. To keep
// concurrent writers from overwriting each other's extensions ("lost
// update"), updates of the same named object are serialized by this
// network: the latest committed manifest for each object name is recorded
// as the object head, and UpdateObject re-reads that head inside a
// per-object critical section before applying the change.
package network

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
)

// ObjectMutator applies a read-modify-write change to a manifest. It is
// invoked with the freshly read current manifest; mutating it in place is
// expected. Returning an error aborts the update without writing anything.
type ObjectMutator func(m *manifest.Manifest) error

// heads tracks the latest committed manifest per object name together with
// a lock used to serialize updates of that object.
type heads struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
	head  map[string]chunk.ID
}

func newHeads() *heads {
	return &heads{
		locks: map[string]*sync.Mutex{},
		head:  map[string]chunk.ID{},
	}
}

// lockFor returns the per-object mutex serializing updates of name.
func (h *heads) lockFor(name string) *sync.Mutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.locks[name]
	if !ok {
		m = &sync.Mutex{}
		h.locks[name] = m
	}
	return m
}

// commit records id as the current manifest for name.
func (h *heads) commit(name string, id chunk.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.head[name] = id
}

// Current returns the recorded head manifest ID for name.
func (h *heads) Current(name string) (chunk.ID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id, ok := h.head[name]
	return id, ok
}

// UpdateObject atomically reads the current manifest of the named object,
// applies mutate, validates and writes the new manifest as a new
// content-addressed chunk, and advances the object head. Referenced data
// chunks are not touched.
//
// Concurrency: updates of the same object name are serialized by an
// in-process per-object lock and always re-read the latest committed head,
// so concurrent read-modify-write writers cannot overwrite each other's
// object- or chunk-level extensions. A write that cannot reach the
// configured replica factor fails (ErrNotEnoughReplicas) and does not
// advance the head; the old committed manifest remains readable, and a
// later update retries from it. When missing nodes return, background
// repair restores every committed manifest chunk from verified copies.
func (n *Network) UpdateObject(ctx context.Context, name string, opt Options, mutate ObjectMutator) (chunk.ID, *manifest.Manifest, error) {
	if mutate == nil {
		return "", nil, errors.New("network: nil mutator")
	}
	mu := n.heads.lockFor(name)
	mu.Lock()
	defer mu.Unlock()

	headID, ok := n.heads.Current(name)
	if !ok {
		return "", nil, fmt.Errorf("%w: object=%s", ErrObjectNotFound, name)
	}

	// Re-read the current head so mutations stack on the newest manifest;
	// GetChunk also performs read repair on reachable replica nodes.
	mc, err := n.GetChunk(ctx, headID, opt)
	if err != nil {
		return "", nil, err
	}
	m, err := manifest.Unmarshal(mc.Data)
	if err != nil {
		return "", nil, err
	}
	// Supported old versions are upgraded to the current version as part
	// of the read-modify-write; extensions are carried over unchanged.
	m = manifest.Upgrade(m)
	if name != "" {
		m.Name = name
	}

	if err := mutate(m); err != nil {
		return "", nil, fmt.Errorf("network: object=%s mutate: %w", name, err)
	}

	raw, err := manifest.Marshal(m)
	if err != nil {
		return "", nil, err
	}
	// If nothing actually changed, the content ID is identical: no new
	// write is needed and the head stays the same.
	newID := chunk.ComputeID(raw)
	if newID == headID {
		n.logger.Logf("update object=%s manifest=%s basis=no-op-same-content", name, newID)
		return newID, m, nil
	}
	newMC := chunk.Chunk{ID: newID, Data: raw}
	if _, err := n.PutChunk(ctx, newMC, opt); err != nil {
		return "", nil, err
	}
	n.heads.commit(name, newMC.ID)
	n.logger.Logf("update object=%s old-manifest=%s new-manifest=%s basis=atomic-read-modify-write",
		name, headID, newMC.ID)
	return newMC.ID, m, nil
}

// CurrentObject returns the latest committed manifest ID for a name.
func (n *Network) CurrentObject(name string) (chunk.ID, bool) {
	return n.heads.Current(name)
}
