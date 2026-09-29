package network

import (
	"context"
	"strings"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
)

// TestChunkAttrsPresenceByteStableEndToEnd: an external manifest carrying
// all three attrs shapes (absent, explicit null, empty object) and a null
// attr *value* is read through GetObject; only a known field (name) is
// changed... actually we first assert the no-touch round trip keeps the
// exact same bytes/content ID, then touch a known field and assert the
// untouched attrs shapes still survive byte-identically for that part.
func TestChunkAttrsPresenceByteStableEndToEnd(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3, ChunkSize: 64}

	// One real data chunk referenced by all entries.
	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i + 1)
	}
	c := chunk.Chunk{ID: chunk.ComputeID(payload), Data: payload}
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	cid := string(c.ID)

	raw := []byte(`{"version":2,"name":"ext","entries":[` +
		`{"id":"` + cid + `","size":64},` +
		`{"id":"` + cid + `","size":64,"attrs":null},` +
		`{"id":"` + cid + `","size":64,"attrs":{}},` +
		`{"id":"` + cid + `","size":64,"attrs":{"x":null,"big":` + huge + `}}` +
		`],"x-tool":{"k":null}}`)
	mid := seedManifestChunk(t, ctx, net, raw, opt)

	// Read it back without touching anything: re-serialized bytes must
	// equal the external bytes and the content ID must be identical.
	_, m, err := net.GetObject(ctx, mid, opt)
	if err != nil {
		t.Fatal(err)
	}
	forms := []manifest.AttrsForm{manifest.AttrsOmitted, manifest.AttrsNull, manifest.AttrsObject, manifest.AttrsObject}
	for i, want := range forms {
		if got := m.Entries[i].AttrsForm(); got != want {
			t.Fatalf("entry %d shape=%d want %d", i, got, want)
		}
	}
	out, err := manifest.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(raw) {
		t.Fatalf("untouched RMW changed external bytes:\n%s\n%s", raw, out)
	}
	if got := chunk.ComputeID(out); got != mid {
		t.Fatalf("content id changed after untouched RMW: %s vs %s", got, mid)
	}
	newID, err := net.PutManifest(ctx, m, opt)
	if err != nil {
		t.Fatal(err)
	}
	if newID != mid {
		t.Fatalf("re-writing untouched manifest produced a different ID: %s vs %s", newID, mid)
	}

	// Now touch only a known field (name); attrs shapes and the null/big
	// attr values must still all survive in the new bytes.
	m.Name = "ext-renamed"
	edited, err := manifest.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	editedID, err := net.PutManifest(ctx, m, opt)
	if err != nil {
		t.Fatal(err)
	}
	if editedID != chunk.ComputeID(edited) {
		t.Fatal("edited manifest ID mismatch")
	}
	if !strings.Contains(string(edited), `"attrs":null`) ||
		!strings.Contains(string(edited), `"attrs":{}`) ||
		!strings.Contains(string(edited), `"x":null`) ||
		!strings.Contains(string(edited), `"big":`+huge) ||
		!strings.Contains(string(edited), `"k":null`) {
		t.Fatalf("attrs shapes/values lost after known-field edit: %s", edited)
	}
	// Edited manifest is stable on a second round trip.
	_, m2, err := net.GetObject(ctx, editedID, opt)
	if err != nil {
		t.Fatal(err)
	}
	out2, _ := manifest.Marshal(m2)
	if string(out2) != string(edited) {
		t.Fatalf("edited manifest not byte-stable:\n%s\n%s", edited, out2)
	}
	t.Logf("external=%s untouched-rewrite=%s edited=%s basis=three-attrs-shapes-preserved-e2e",
		mid, newID, editedID)
}

// TestPutObjectEmptyVsNilAttrsPreserved: ChunkAttrs entries that are an
// explicit non-nil empty map serialize as "attrs": {} and round-trip as an
// object shape; a nil entry stays absent.
func TestPutObjectEmptyVsNilAttrsPreserved(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{
		Replication: 3,
		ChunkSize:   64,
		ChunkAttrs: []map[string]any{
			nil,
			{}, // explicit empty object
			{"k": "v"},
		},
	}
	payload := make([]byte, 64*3)
	id, _, err := net.PutObject(ctx, "shapes.bin", payload, opt)
	if err != nil {
		t.Fatal(err)
	}
	// Wire bytes must contain one explicit empty attrs object.
	mc, err := net.GetChunk(ctx, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mc.Data), `"attrs":{}`) {
		t.Fatalf("empty attrs object not written: %s", mc.Data)
	}
	_, m, err := net.GetObject(ctx, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	want := []manifest.AttrsForm{manifest.AttrsOmitted, manifest.AttrsObject, manifest.AttrsObject}
	for i, w := range want {
		if got := m.Entries[i].AttrsForm(); got != w {
			t.Fatalf("entry %d form=%d want %d", i, got, w)
		}
	}
	if m.Entries[1].Attrs == nil {
		t.Fatal("empty attrs map must be non-nil after read")
	}
	t.Logf("shapes manifest=%s basis=nil-vs-empty-object-distinct-on-write", id)
}
