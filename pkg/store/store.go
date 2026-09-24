// Package store implements a single local content-addressed storage node.
//
// Chunks are stored as regular files named by their content ID under
// <root>/objects. Writes are atomic (temp file + rename) and every read
// recomputes the hash, so bit rot, truncation or a tampered file is
// detected and reported distinctly from a missing chunk.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

// idPattern matches a lowercase SHA-256 hex identifier (64 chars).
var idPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Store is one storage node holding chunks keyed by their content ID.
type Store struct {
	id   string
	root string

	available    atomic.Bool
	writeLocks   map[string]*sync.Mutex
	writeLocksMu sync.Mutex
}

// New creates a node with the given node ID and on-disk root directory.
// The directory (and its objects subdirectory) is created if missing.
func New(id, root string) (*Store, error) {
	if id == "" {
		return nil, errors.New("store: empty node id")
	}
	if err := os.MkdirAll(filepath.Join(root, "objects"), 0o755); err != nil {
		return nil, fmt.Errorf("store: init: %w", err)
	}
	s := &Store{
		id:         id,
		root:       root,
		writeLocks: map[string]*sync.Mutex{},
	}
	s.available.Store(true)
	return s, nil
}

// Errors with stable, distinguishable failure reasons.
var (
	// ErrNotFound means no copy of the chunk exists on this node.
	ErrNotFound = errors.New("store: chunk not found")
	// ErrCorrupt means a stored payload does not hash to its ID.
	ErrCorrupt = errors.New("store: chunk corrupt")
	// ErrIDMismatch means a Put claimed an ID that does not match its data.
	ErrIDMismatch = errors.New("store: id mismatch")
	// ErrUnavailable means the node is simulating an outage.
	ErrUnavailable = errors.New("store: node unavailable")
)

// ID returns the node ID.
func (s *Store) ID() string { return s.id }

// SetAvailable toggles node availability to simulate outages. An
// unavailable node fails every operation with ErrUnavailable; its
// on-disk data is untouched and becomes readable again when re-enabled.
func (s *Store) SetAvailable(ok bool) {
	s.available.Store(ok)
}

func (s *Store) checkAvailable() error {
	if !s.available.Load() {
		return fmt.Errorf("%w: node=%s", ErrUnavailable, s.id)
	}
	return nil
}

func (s *Store) path(id chunk.ID) string {
	return filepath.Join(s.root, "objects", string(id))
}

// lockFor serializes concurrent writes to the same ID within the node.
func (s *Store) lockFor(id chunk.ID) *sync.Mutex {
	s.writeLocksMu.Lock()
	defer s.writeLocksMu.Unlock()
	m, ok := s.writeLocks[string(id)]
	if !ok {
		m = &sync.Mutex{}
		s.writeLocks[string(id)] = m
	}
	return m
}

// Put writes a chunk after verifying that its ID matches its payload.
// Writing the same content again is idempotent and yields the same ID.
// A corrupt existing copy is replaced by the verified payload.
func (s *Store) Put(ctx context.Context, c chunk.Chunk) error {
	if err := s.checkAvailable(); err != nil {
		return err
	}
	if err := chunk.Verify(c); err != nil {
		return fmt.Errorf("%w: node=%s id=%s: %v", ErrIDMismatch, s.id, c.ID, err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	mu := s.lockFor(c.ID)
	mu.Lock()
	defer mu.Unlock()

	target := s.path(c.ID)
	if data, err := os.ReadFile(target); err == nil {
		if chunk.ComputeID(data) == c.ID {
			return nil // already present and valid: idempotent success
		}
		// Existing copy is corrupt; fall through and replace it.
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: node=%s id=%s inspect: %w", s.id, c.ID, err)
	}

	tmp, err := os.CreateTemp(s.root, ".tmp-*")
	if err != nil {
		return fmt.Errorf("store: node=%s id=%s temp: %w", s.id, c.ID, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(c.Data); err != nil {
		tmp.Close()
		return fmt.Errorf("store: node=%s id=%s write: %w", s.id, c.ID, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("store: node=%s id=%s sync: %w", s.id, c.ID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("store: node=%s id=%s close: %w", s.id, c.ID, err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("store: node=%s id=%s rename: %w", s.id, c.ID, err)
	}
	return nil
}

// Get reads and verifies a chunk. A missing file is ErrNotFound; a file
// whose payload does not hash to id is ErrCorrupt.
func (s *Store) Get(ctx context.Context, id chunk.ID) (chunk.Chunk, error) {
	if err := s.checkAvailable(); err != nil {
		return chunk.Chunk{}, err
	}
	select {
	case <-ctx.Done():
		return chunk.Chunk{}, ctx.Err()
	default:
	}
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return chunk.Chunk{}, fmt.Errorf("%w: node=%s id=%s", ErrNotFound, s.id, id)
		}
		return chunk.Chunk{}, fmt.Errorf("store: node=%s id=%s read: %w", s.id, id, err)
	}
	if chunk.ComputeID(data) != id {
		return chunk.Chunk{}, fmt.Errorf("%w: node=%s id=%s actual=%s",
			ErrCorrupt, s.id, id, chunk.ComputeID(data))
	}
	return chunk.Chunk{ID: id, Data: data}, nil
}

// Has reports whether the node holds a verified copy of the chunk.
// A corrupt copy is reported as (false, ErrCorrupt), never as present.
func (s *Store) Has(ctx context.Context, id chunk.ID) (bool, error) {
	if err := s.checkAvailable(); err != nil {
		return false, err
	}
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("store: node=%s id=%s: %w", s.id, id, err)
	}
	if chunk.ComputeID(data) != id {
		return false, fmt.Errorf("%w: node=%s id=%s", ErrCorrupt, s.id, id)
	}
	return true, nil
}

// List returns the chunk IDs named on this node, sorted. IDs are derived
// from file names; corrupt payloads are still listed so repair can find
// their slots and overwrite them from healthy replicas.
func (s *Store) List(ctx context.Context) ([]chunk.ID, error) {
	if err := s.checkAvailable(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return nil, fmt.Errorf("store: node=%s list: %w", s.id, err)
	}
	var ids []chunk.ID
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if idPattern.MatchString(name) {
			ids = append(ids, chunk.ID(name))
		}
	}
	sortIDs(ids)
	return ids, nil
}

// Delete removes a chunk (used by tests to simulate loss).
func (s *Store) Delete(ctx context.Context, id chunk.ID) error {
	if err := s.checkAvailable(); err != nil {
		return err
	}
	err := os.Remove(s.path(id))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: node=%s id=%s delete: %w", s.id, id, err)
	}
	return nil
}

// Corrupt overwrites a chunk payload with garbage while keeping the
// content-ID file name. Test helper for integrity-failure scenarios.
func (s *Store) Corrupt(ctx context.Context, id chunk.ID, garbage []byte) error {
	if err := s.checkAvailable(); err != nil {
		return err
	}
	if err := os.WriteFile(s.path(id), garbage, 0o644); err != nil {
		return fmt.Errorf("store: node=%s id=%s corrupt: %w", s.id, id, err)
	}
	return nil
}

func sortIDs(ids []chunk.ID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}
