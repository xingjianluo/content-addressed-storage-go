// Package network ties local storage nodes together and provides
// replicated writes, reads, and background repair.
//
// Placement is deterministic: nodes are sorted by ID and rotated by a
// hash of the content ID, so the same content always prefers the same
// node set regardless of which client writes it or when. Writes are
// acknowledged only by nodes that persisted a verified copy; the returned
// replica count is the number of such confirmations and is therefore
// never overstated.
package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/store"
)

// Network is a set of local storage nodes.
type Network struct {
	nodes []*store.Store
	// sorted holds nodes sorted by ID; placement is derived from it.
	sorted []*store.Store

	logger Logger
}

// Logger receives human-readable, test-inspectable decisions.
type Logger interface {
	Logf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Logf(string, ...any) {}

// New creates a network over the given nodes.
func New(nodes ...*store.Store) *Network {
	sorted := append([]*store.Store(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID() < sorted[j].ID() })
	return &Network{nodes: nodes, sorted: sorted, logger: discardLogger{}}
}

// SetLogger installs a decision logger (e.g. a test logger).
func (n *Network) SetLogger(l Logger) {
	if l == nil {
		l = discardLogger{}
	}
	n.logger = l
}

// Options controls replication behavior.
type Options struct {
	// Replication is the number of nodes that must acknowledge a write.
	Replication int
	// ChunkSize overrides the default split size for objects.
	ChunkSize int
	// Extensions holds object-level custom extension fields stored on the
	// manifest itself. Nested objects/arrays, booleans, nil and large
	// integers (*big.Int / json.Number) are preserved losslessly.
	Extensions map[string]any
	// ChunkAttrs holds per-chunk extension fields; it must either be empty
	// or have exactly one entry per produced chunk (in chunk order).
	ChunkAttrs []map[string]any
}

func (o Options) replication() int {
	if o.Replication <= 0 {
		return 1
	}
	return o.Replication
}

// Errors with stable, distinguishable failure reasons.
var (
	// ErrNotEnoughReplicas means fewer than Replication nodes confirmed.
	ErrNotEnoughReplicas = errors.New("network: not enough replicas acknowledged")
	// ErrQuorumUnavailable means too few nodes were reachable to decide.
	ErrQuorumUnavailable = errors.New("network: quorum unavailable")
	// ErrAllReplicasBad means every reachable replica failed verification.
	ErrAllReplicasBad = errors.New("network: all replicas failed integrity check")
)

// placementOrder returns nodes in the deterministic preference order for id.
func (n *Network) placementOrder(id chunk.ID) []*store.Store {
	if len(n.sorted) == 0 {
		return nil
	}
	h := sha256.Sum256([]byte(id))
	start := int(h[0]) % len(n.sorted)
	order := make([]*store.Store, 0, len(n.sorted))
	for k := 0; k < len(n.sorted); k++ {
		order = append(order, n.sorted[(start+k)%len(n.sorted)])
	}
	return order
}

// preferredNodes returns the first r nodes of the placement order.
func (n *Network) preferredNodes(id chunk.ID, r int) []*store.Store {
	order := n.placementOrder(id)
	if r > len(order) {
		r = len(order)
	}
	return order[:r]
}

// PreferredNodes exposes the deterministic replica set for id. It is
// intended for tooling, demos and tests.
func (n *Network) PreferredNodes(id chunk.ID, r int) []*store.Store {
	return n.preferredNodes(id, r)
}

// PutResult reports a write decision and is returned for observability.
type PutResult struct {
	ID          chunk.ID
	AckedNodes  []string
	FailedNodes map[string]error
}

// PutChunk writes a chunk to exactly its deterministic replica set: the
// first Replication nodes in placement order. Success requires all of
// those nodes to confirm; if one is down or rejects the data the write
// fails with ErrNotEnoughReplicas and the returned count is the number of
// nodes that actually persisted a verified copy. Writes therefore never
// spread beyond the replica set, and the count is never overstated;
// Repair restores missing members of the set after they return.
func (n *Network) PutChunk(ctx context.Context, c chunk.Chunk, opt Options) (int, error) {
	r := opt.replication()
	if r > len(n.sorted) {
		return 0, fmt.Errorf("%w: id=%s want=%d nodes=%d",
			ErrNotEnoughReplicas, c.ID, r, len(n.sorted))
	}
	targets := n.preferredNodes(c.ID, r)
	results := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, nd := range targets {
		wg.Add(1)
		go func(i int, nd *store.Store) {
			defer wg.Done()
			results[i] = nd.Put(ctx, c)
		}(i, nd)
	}
	wg.Wait()

	acked := 0
	failed := map[string]error{}
	for i, err := range results {
		nid := targets[i].ID()
		if err == nil {
			acked++
			continue
		}
		failed[nid] = err
	}
	n.logger.Logf("put id=%s nodes=%v acked=%d failed=%v basis=verified-persistent-copies",
		c.ID, nodeIDs(targets), acked, failed)

	if acked < r {
		return acked, fmt.Errorf("%w: id=%s confirmed=%d want=%d failed=%v",
			ErrNotEnoughReplicas, c.ID, acked, r, failed)
	}
	return acked, nil
}

// GetChunk reads from a verified replica and performs read repair: any
// preferred node that is reachable but missing/corrupt receives a fresh
// verified copy. Failure classification:
//   - ErrAllReplicasBad: every reachable node holding the ID was corrupt;
//   - ErrQuorumUnavailable: no reachable node could serve a good copy and
//     none reported corruption either (all missing or unavailable);
//   - store.ErrNotFound: no node holds the chunk.
func (n *Network) GetChunk(ctx context.Context, id chunk.ID, opt Options) (chunk.Chunk, error) {
	r := opt.replication()
	order := n.placementOrder(id)

	type outcome struct {
		node  *store.Store
		c     chunk.Chunk
		err   error
		found bool
	}
	outcomes := make([]outcome, len(order))
	var wg sync.WaitGroup
	for i, nd := range order {
		wg.Add(1)
		go func(i int, nd *store.Store) {
			defer wg.Done()
			c, err := nd.Get(ctx, id)
			outcomes[i] = outcome{node: nd, c: c, err: err, found: err == nil}
		}(i, nd)
	}
	wg.Wait()

	var good *chunk.Chunk
	var goodNode string
	corruptNodes, unavailableNodes, missingNodes := []string{}, []string{}, []string{}
	contacted := 0
	for i := range outcomes {
		o := outcomes[i]
		err := o.err
		switch {
		case err == nil:
			contacted++
			if good == nil {
				cc := o.c
				good = &cc
				goodNode = o.node.ID()
			}
		case errors.Is(err, store.ErrCorrupt):
			contacted++
			corruptNodes = append(corruptNodes, o.node.ID())
		case errors.Is(err, store.ErrUnavailable):
			unavailableNodes = append(unavailableNodes, o.node.ID())
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// A canceled/deadline request never proves the node is up;
			// treat it like an unreachable node, not like "missing".
			unavailableNodes = append(unavailableNodes, o.node.ID())
		case errors.Is(err, store.ErrNotFound):
			contacted++
			missingNodes = append(missingNodes, o.node.ID())
		default:
			contacted++
			missingNodes = append(missingNodes, o.node.ID())
		}
	}

	if good == nil {
		basis := fmt.Sprintf("corrupt=%v unavailable=%v missing=%v",
			corruptNodes, unavailableNodes, missingNodes)
		// Corruption is more specific than absence: if any reachable node
		// held (a bad copy of) the ID, the content exists but no verified
		// copy is reachable — classify as all-bad, never as missing.
		if len(corruptNodes) > 0 {
			return chunk.Chunk{}, fmt.Errorf("%w: id=%s corrupt-nodes=%v basis=%s",
				ErrAllReplicasBad, id, corruptNodes, basis)
		}
		if contacted == 0 {
			return chunk.Chunk{}, fmt.Errorf("%w: id=%s nodes=%v basis=%s",
				ErrQuorumUnavailable, id, nodeIDs(order), basis)
		}
		return chunk.Chunk{}, fmt.Errorf("%w: id=%s basis=%s", store.ErrNotFound, id, basis)
	}

	// Read repair: restore missing/corrupt preferred nodes that are up.
	repaired := []string{}
	repairs := r
	if repairs > len(order) {
		repairs = len(order)
	}
	for i := 0; i < repairs; i++ {
		o := outcomes[i]
		if o.err == nil {
			continue
		}
		if errors.Is(o.err, store.ErrUnavailable) {
			continue
		}
		if err := o.node.Put(ctx, *good); err == nil {
			repaired = append(repaired, o.node.ID())
		}
	}
	n.logger.Logf("get id=%s nodes=%v served-by=%s corrupt=%v missing=%v unavailable=%v repaired=%v basis=hash-verified-replica",
		id, nodeIDs(order), goodNode, corruptNodes, missingNodes, unavailableNodes, repaired)

	return *good, nil
}

// PutObject splits content deterministically, writes every chunk with
// replication, then writes the manifest itself as a content-addressed
// chunk. The returned ID identifies the serialized manifest.
//
// opt.Extensions (object-level) and opt.ChunkAttrs (one attrs object per
// chunk in order) are attached to the manifest and preserved losslessly.
func (n *Network) PutObject(ctx context.Context, name string, data []byte, opt Options) (chunk.ID, *manifest.Manifest, error) {
	size := opt.ChunkSize
	if size <= 0 {
		size = chunk.DefaultSplitSize
	}
	parts := chunk.Split(data, size)
	if len(opt.ChunkAttrs) > 0 && len(opt.ChunkAttrs) != len(parts) {
		return "", nil, fmt.Errorf("network: ChunkAttrs count %d does not match chunk count %d",
			len(opt.ChunkAttrs), len(parts))
	}

	man := &manifest.Manifest{
		Version: manifest.CurrentVersion,
		Name:    name,
		Entries: make([]manifest.Entry, 0, len(parts)),
		Unknown: cloneExt(opt.Extensions),
	}
	for i, c := range parts {
		if _, err := n.PutChunk(ctx, c, opt); err != nil {
			return "", nil, err
		}
		e := manifest.Entry{ID: c.ID, Size: len(c.Data)}
		if i < len(opt.ChunkAttrs) && opt.ChunkAttrs[i] != nil {
			// A non-nil (even empty) attrs map is an explicit object form
			// and is preserved as such; only a nil map means "absent".
			e.Attrs = cloneExt(opt.ChunkAttrs[i])
		}
		man.Entries = append(man.Entries, e)
	}

	id, err := n.PutManifest(ctx, man, opt)
	if err != nil {
		return "", nil, err
	}
	return id, man, nil
}

// PutManifest serializes and writes an already-built manifest (including
// any object-level and per-chunk extensions) as a content-addressed
// chunk. It is the write side of a read-modify-write cycle: fetch with
// GetObject, edit the returned manifest, then persist via PutManifest.
func (n *Network) PutManifest(ctx context.Context, man *manifest.Manifest, opt Options) (chunk.ID, error) {
	raw, err := manifest.Marshal(man)
	if err != nil {
		return "", err
	}
	mc := chunk.Chunk{ID: chunk.ComputeID(raw), Data: raw}
	if _, err := n.PutChunk(ctx, mc, opt); err != nil {
		return "", err
	}
	return mc.ID, nil
}

// cloneExt shallow-copies an extension map so caller-owned maps are never
// shared/mutated; nested values are treated as immutable after handoff.
// A non-nil empty map stays a non-nil empty map so the explicit empty
// object form is distinguishable from "absent".
func cloneExt(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// GetObject fetches the manifest chunk, applies manifest compatibility
// rules, then fetches and verifies every referenced chunk in order.
func (n *Network) GetObject(ctx context.Context, manifestID chunk.ID, opt Options) ([]byte, *manifest.Manifest, error) {
	mc, err := n.GetChunk(ctx, manifestID, opt)
	if err != nil {
		return nil, nil, err
	}
	m, err := manifest.Unmarshal(mc.Data)
	if err != nil {
		return nil, nil, err
	}
	parts := make([]chunk.Chunk, 0, len(m.Entries))
	for _, e := range m.Entries {
		c, err := n.GetChunk(ctx, e.ID, opt)
		if err != nil {
			return nil, m, err
		}
		if len(c.Data) != e.Size {
			return nil, m, fmt.Errorf("%w: id=%s expected-size=%d actual-size=%d",
				chunk.ErrCorrupt, e.ID, e.Size, len(c.Data))
		}
		parts = append(parts, c)
	}
	return chunk.Join(parts), m, nil
}

// RepairResult reports what one repair pass decided for one chunk ID.
type RepairResult struct {
	ID       chunk.ID
	Healthy  int
	Missing  []string
	Corrupt  []string
	Repaired []string
}

// Repair scans the union of chunks known to every reachable node and
// restores replication on preferred nodes that were down or hold a bad
// copy. It only copies bytes that were verified against their ID, so a
// corrupt slot can never be "repaired" from another corrupt slot.
func (n *Network) Repair(ctx context.Context, opt Options) ([]RepairResult, error) {
	r := opt.replication()

	known := map[chunk.ID]struct{}{}
	for _, nd := range n.sorted {
		ids, err := nd.List(ctx)
		if err != nil {
			if errors.Is(err, store.ErrUnavailable) {
				continue
			}
			return nil, err
		}
		for _, id := range ids {
			known[id] = struct{}{}
		}
	}

	all := make([]chunk.ID, 0, len(known))
	for id := range known {
		all = append(all, id)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	results := make([]RepairResult, 0, len(all))
	for _, id := range all {
		res := RepairResult{ID: id}

		// Locate a verified source.
		var source chunk.Chunk
		var sourceNode string
		for _, nd := range n.placementOrder(id) {
			c, err := nd.Get(ctx, id)
			if err == nil {
				source = c
				sourceNode = nd.ID()
				break
			}
		}

		targets := n.preferredNodes(id, r)
		targetSet := map[string]bool{}
		for _, nd := range targets {
			targetSet[nd.ID()] = true
		}
		for _, nd := range n.placementOrder(id) {
			if !targetSet[nd.ID()] {
				continue
			}
			ok, err := nd.Has(ctx, id)
			switch {
			case err == nil && ok:
				res.Healthy++
			case errors.Is(err, store.ErrCorrupt):
				res.Corrupt = append(res.Corrupt, nd.ID())
			case errors.Is(err, store.ErrUnavailable):
				res.Missing = append(res.Missing, nd.ID())
			default:
				res.Missing = append(res.Missing, nd.ID())
			}
		}

		if sourceNode == "" {
			n.logger.Logf("repair id=%s healthy=%d corrupt=%v missing=%v repaired=[] basis=no-verified-source",
				id, res.Healthy, res.Corrupt, res.Missing)
			results = append(results, res)
			continue
		}

		for _, nd := range targets {
			ok, err := nd.Has(ctx, id)
			if err == nil && ok {
				continue
			}
			if errors.Is(err, store.ErrUnavailable) {
				continue
			}
			if putErr := nd.Put(ctx, source); putErr == nil {
				// A previously corrupt or missing slot is now a verified copy.
				res.Repaired = append(res.Repaired, nd.ID())
			}
		}
		// Recompute healthy from the union of previously healthy and repaired.
		res.Healthy = 0
		for _, nd := range targets {
			if ok, err := nd.Has(ctx, id); err == nil && ok {
				res.Healthy++
			}
		}
		n.logger.Logf("repair id=%s source=%s healthy=%d corrupt=%v missing=%v repaired=%v basis=verified-copy-restore",
			id, sourceNode, res.Healthy, res.Corrupt, res.Missing, res.Repaired)
		results = append(results, res)
	}
	return results, nil
}

func nodeIDs(nodes []*store.Store) []string {
	out := make([]string, 0, len(nodes))
	for _, nd := range nodes {
		out = append(out, nd.ID())
	}
	return out
}
