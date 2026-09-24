package network

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/store"
)

// seedManifestChunk writes an arbitrary, already-serialized manifest as a
// chunk onto the replica set, bypassing PutObject.
func seedManifestChunk(t *testing.T, ctx context.Context, net *Network, raw []byte, opt Options) chunk.ID {
	t.Helper()
	id := chunk.ComputeID(raw)
	c := chunk.Chunk{ID: id, Data: raw}
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestLegacyManifestReadable: a manifest with no explicit version field
// (legacy data) is stored and read back, and its object is reconstructable.
func TestLegacyManifestReadable(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	opt := Options{Replication: 2}

	part := chunkFor("legacy-payload")
	if _, err := net.PutChunk(ctx, part, opt); err != nil {
		t.Fatal(err)
	}
	legacy := `{"name":"legacy-obj","entries":[{"id":"` + string(part.ID) + `","size":` +
		strconv.Itoa(len(part.Data)) + `}]}`
	id := seedManifestChunk(t, ctx, net, []byte(legacy), opt)

	data, man, err := net.GetObject(ctx, id, opt)
	if err != nil {
		t.Fatalf("legacy manifest must be readable: %v", err)
	}
	if man.Version != 1 {
		t.Fatalf("legacy version interpreted as %d", man.Version)
	}
	if string(data) != string(part.Data) {
		t.Fatal("legacy object payload mismatch")
	}
	t.Logf("manifest-id=%s version=missing->1 chunks=1 basis=legacy-default-version-readable", id)
}

// TestUnknownFieldsSurviveReadModifyWrite: read an object whose manifest
// carries future/unknown fields, upgrade it, and write back; the unknown
// fields and the data must all be preserved.
func TestUnknownFieldsSurviveReadModifyWrite(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	opt := Options{Replication: 2}

	part := chunkFor("future-payload")
	if _, err := net.PutChunk(ctx, part, opt); err != nil {
		t.Fatal(err)
	}
	raw := `{"version":1,"name":"future-obj","entries":[{"id":"` + string(part.ID) +
		`","size":` + strconv.Itoa(len(part.Data)) + `}],"x-encryption":"aes-9000","x-quorum":7}`
	id := seedManifestChunk(t, ctx, net, []byte(raw), opt)

	_, man, err := net.GetObject(ctx, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	if man.Unknown["x-encryption"] != "aes-9000" {
		t.Fatalf("unknown field lost on read: %+v", man.Unknown)
	}

	// Read-modify-write: upgrade (no-op today) and persist under the same
	// content (entries unchanged -> stable manifest ID).
	man = manifest.Upgrade(man)
	out, err := manifest.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	newID := chunk.ComputeID(out)
	if _, err := net.PutChunk(ctx, chunk.Chunk{ID: newID, Data: out}, opt); err != nil {
		t.Fatal(err)
	}
	re, err := manifest.Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if re.Unknown["x-encryption"] != "aes-9000" || re.Unknown["x-quorum"].(float64) != 7 {
		t.Fatalf("read-modify-write silently dropped fields: %s", out)
	}
	t.Logf("manifest-id=%s preserved-fields=%v basis=unknown-fields-verbatim-round-trip",
		id, re.Unknown)
}

// TestFutureManifestRejected: an object whose manifest claims a version
// this build does not understand must fail with a distinguishable error
// and never be treated as valid data.
func TestFutureManifestRejected(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	opt := Options{Replication: 2}

	future := `{"version":42,"entries":[]}`
	id := seedManifestChunk(t, ctx, net, []byte(future), opt)

	_, _, err := net.GetObject(ctx, id, opt)
	if !errors.Is(err, manifest.ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got %v", err)
	}
	t.Logf("manifest-id=%s version=42 reason=%v basis=version>MaxSupportedVersion", id, err)
}

// TestCorruptManifestRejected: a manifest chunk whose bytes were tampered
// with after writing fails integrity verification at the chunk layer.
func TestCorruptManifestRejected(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	opt := Options{Replication: 3}

	part := chunkFor("m-payload")
	if _, err := net.PutChunk(ctx, part, opt); err != nil {
		t.Fatal(err)
	}
	man := &manifest.Manifest{
		Version: manifest.CurrentVersion,
		Entries: []manifest.Entry{{ID: part.ID, Size: len(part.Data)}},
	}
	raw, _ := manifest.Marshal(man)
	id := seedManifestChunk(t, ctx, net, raw, opt)

	for _, nd := range net.preferredNodes(id, 3) {
		if err := nd.Corrupt(ctx, id, []byte("{broken")); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err := net.GetObject(ctx, id, opt)
	if !errors.Is(err, store.ErrCorrupt) &&
		!errors.Is(err, ErrAllReplicasBad) && !errors.Is(err, manifest.ErrInvalidManifest) {
		t.Fatalf("tampered manifest accepted or misclassified: %v", err)
	}
	t.Logf("manifest-id=%s tampered=true reason=%v basis=chunk-hash-or-json-validation", id, err)
}
