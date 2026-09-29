package network

import (
	"context"
	"sync"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
)

// casCoordinator serializes coordinated read-modify-write cycles per
// manifest lineage and remembers the newest confirmed manifest ID of each
// lineage, so two writers starting from the same base produce one merged
// successor containing both writers' extensions instead of overwriting
// each other.
type casCoordinator struct {
	mu sync.Mutex
	// roots maps every manifest ID seen as a base to the root ID of its
	// lineage (path-compressed union-find).
	roots map[chunk.ID]chunk.ID
	// heads maps a lineage root to its newest confirmed manifest ID.
	heads map[chunk.ID]chunk.ID
	// locks serializes all coordinated writes of one lineage; different
	// lineages (different objects) remain fully parallel.
	locks map[chunk.ID]*sync.Mutex
}

func newCASCoordinator() *casCoordinator {
	return &casCoordinator{
		roots: map[chunk.ID]chunk.ID{},
		heads: map[chunk.ID]chunk.ID{},
		locks: map[chunk.ID]*sync.Mutex{},
	}
}

// lineageLocked returns (root, head, lock) for id, registering id as a new
// self-pointing lineage when it is encountered the first time. Caller
// holds c.mu.
func (c *casCoordinator) lineageLocked(id chunk.ID) (chunk.ID, chunk.ID, *sync.Mutex) {
	root, ok := c.roots[id]
	if !ok {
		c.roots[id] = id
		c.heads[id] = id
		l := &sync.Mutex{}
		c.locks[id] = l
		return id, id, l
	}
	// Path compression.
	for c.roots[root] != root {
		root = c.roots[root]
	}
	c.roots[id] = root
	return root, c.heads[root], c.locks[root]
}

// lineageLockedHead returns the current head and lineage lock for id.
// Caller holds c.mu.
func (c *casCoordinator) lineageLockedHead(id chunk.ID) (chunk.ID, *sync.Mutex) {
	_, head, lock := c.lineageLocked(id)
	return head, lock
}

// begin serializes against all other coordinated operations on id's
// lineage and returns the current head manifest ID.
func (c *casCoordinator) begin(id chunk.ID) (head chunk.ID, lock *sync.Mutex) {
	c.mu.Lock()
	head, lock = c.lineageLockedHead(id)
	c.mu.Unlock()
	lock.Lock()
	c.mu.Lock()
	_, head, _ = c.lineageLocked(head)
	c.mu.Unlock()
	return head, lock
}

// commit records newID as the successor of baseID in its lineage. It must
// be called while holding the lineage lock returned by begin.
func (c *casCoordinator) commit(baseID, newID chunk.ID) chunk.ID {
	c.mu.Lock()
	defer c.mu.Unlock()
	root, _, _ := c.lineageLocked(baseID)
	c.roots[newID] = root
	c.heads[root] = newID
	return newID
}

// HeadManifestID returns the newest coordinated manifest ID in the
// lineage that contains id. A plain id that was never part of a
// coordinated update is returned unchanged (it is the head of itself).
func (n *Network) HeadManifestID(id chunk.ID) chunk.ID {
	n.cas.mu.Lock()
	defer n.cas.mu.Unlock()
	_, head, _ := n.cas.lineageLocked(id)
	return head
}

// PutManifestCAS is the coordinated write side of a read-modify-write
// cycle. baseID is the manifest ID the caller read before editing man;
// man is the edited manifest. If another writer already committed a
// successor of baseID, the caller's edit is three-way merged with that
// successor (against the common base) and the merged result is written,
// so distinct extension keys from both writers are kept. An incompatible
// edit to the same key/field is reported as manifest.MergeConflict.
//
// The write still follows the normal replica rules: the returned ID is
// acknowledged only after every replica-set node persisted a verified
// copy; an unconfirmed write is not recorded as a lineage head.
func (n *Network) PutManifestCAS(ctx context.Context, baseID chunk.ID, man *manifest.Manifest, opt Options) (chunk.ID, error) {
	head, lock := n.cas.begin(baseID)
	defer lock.Unlock()
	return n.casWriteLocked(ctx, baseID, head, man, opt)
}

// ModifyManifest runs a coordinated read-modify-write loop. It loads the
// newest manifest of baseID's lineage, invokes mut to attach this
// caller's extensions, then commits via PutManifestCAS; if the commit
// races with another writer it transparently retries against the merged
// state until it wins or the context ends. mut must only add/edit this
// caller's own extension keys. Returns the new lineage head ID.
func (n *Network) ModifyManifest(ctx context.Context, baseID chunk.ID, mut func(*manifest.Manifest) error, opt Options) (chunk.ID, error) {
	return n.modifyLoop(ctx, baseID, mut, opt)
}

// casWriteLocked implements PutManifestCAS while the caller already holds
// the lineage lock: it uses the given current head, merges the caller's
// edit with any committed successor of baseID, writes the merged manifest,
// and advances the head only after the write is confirmed by the full
// replica set.
func (n *Network) casWriteLocked(ctx context.Context, baseID, head chunk.ID, man *manifest.Manifest, opt Options) (chunk.ID, error) {
	final := man
	if head != baseID {
		// Another writer already committed a successor: merge with it
		// against the common base so neither side's extensions are lost.
		_, baseMan, err := n.readManifestChunk(ctx, baseID, opt)
		if err != nil {
			return "", err
		}
		_, headMan, err := n.readManifestChunk(ctx, head, opt)
		if err != nil {
			return "", err
		}
		merged, err := manifest.Merge(baseMan, headMan, man)
		if err != nil {
			return "", err
		}
		final = merged
		n.logger.Logf("cas base=%s head=%s basis=three-way-merge-distinct-extensions-kept", baseID, head)
	}

	id, err := n.PutManifest(ctx, final, opt)
	if err != nil {
		// Unconfirmed: the lineage head is deliberately not advanced, so a
		// retry merges against the same committed predecessor.
		return "", err
	}
	n.cas.commit(baseID, id)
	n.logger.Logf("cas commit base=%s new=%s basis=full-replica-ack-then-head-advances", baseID, id)
	return id, nil
}

// readManifestChunk fetches and decodes a single manifest chunk without
// pulling its data chunks.
func (n *Network) readManifestChunk(ctx context.Context, id chunk.ID, opt Options) (chunk.ID, *manifest.Manifest, error) {
	mc, err := n.GetChunk(ctx, id, opt)
	if err != nil {
		return "", nil, err
	}
	m, err := manifest.Unmarshal(mc.Data)
	if err != nil {
		return "", nil, err
	}
	return id, m, nil
}

// modifyLoop implements ModifyManifest with transparent retries: read the
// current lineage head, let the caller mutate a copy, then CAS-commit; a
// merge conflict or an unconfirmed write (e.g. a node briefly down) is
// retried against the newest head.
func (n *Network) modifyLoop(ctx context.Context, baseID chunk.ID, mut func(*manifest.Manifest) error, opt Options) (chunk.ID, error) {
	const maxAttempts = 1000
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		head, lock := n.cas.begin(baseID)
		_, m, err := n.readManifestChunk(ctx, head, opt)
		if err != nil {
			lock.Unlock()
			lastErr = err
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Millisecond):
			}
			continue
		}
		if err := mut(m); err != nil {
			lock.Unlock()
			return "", err
		}
		newID, err := n.casWriteLocked(ctx, head, head, m, opt)
		lock.Unlock()
		if err != nil {
			// Unconfirmed write (e.g. a replica briefly unavailable) or a
			// structural merge conflict: retry against the newest head.
			lastErr = err
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Millisecond):
			}
			continue
		}
		return newID, nil
	}
	return "", lastErr
}
