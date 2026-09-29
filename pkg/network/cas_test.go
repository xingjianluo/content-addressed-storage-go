package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
)

// TestCoordinatedRMWMergesConcurrentExtensions: independent writers each
// read the same base manifest and attach their own top-level and
// per-chunk extension keys. With coordinated CAS writes the final lineage
// head must contain every writer's keys (no last-writer-wins loss), the
// payload must be unchanged, and every confirmed version must converge at
// full replication after a flapping node recovers.
func TestCoordinatedRMWMergesConcurrentExtensions(t *testing.T) {
	ctx := context.Background()
	net, nodes := newTestNetwork(t, 6)
	opt := Options{Replication: 3, ChunkSize: 64}
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i*3 + 2)
	}
	baseOpt := Options{
		Replication: 3,
		ChunkSize:   64,
		Extensions:  map[string]any{"init": json.Number(huge)},
		ChunkAttrs: []map[string]any{
			{"init": json.Number(huge)},
			{"init": json.Number(huge)},
			{"init": json.Number(huge)},
			{"init": json.Number(huge)},
		},
	}
	baseID, _, err := net.PutObject(ctx, "shared.bin", payload, baseOpt)
	if err != nil {
		t.Fatal(err)
	}

	// Flap one replica-set node for the whole concurrent phase.
	stop := make(chan struct{})
	var flapWG sync.WaitGroup
	flapWG.Add(1)
	go func() {
		defer flapWG.Done()
		nd := nodes[4]
		up := true
		for {
			select {
			case <-stop:
				nd.SetAvailable(true)
				return
			default:
				up = !up
				nd.SetAvailable(up)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	const writers = 8
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("tool-%02d", w)
			val := json.Number(fmt.Sprintf("9200000000000000000000000000000000%04d", w))
			err := func() error {
				for attempt := 0; attempt < 200; attempt++ {
					_, e := net.ModifyManifest(ctx, baseID, func(m *manifest.Manifest) error {
						m.Unknown[key] = val
						for i := range m.Entries {
							m.Entries[i].SetAttr(key, val)
						}
						return nil
					}, opt)
					if e == nil {
						return nil
					}
					if errors.Is(e, manifest.MergeConflict) {
						return e // distinct keys must never conflict
					}
					time.Sleep(time.Millisecond) // node flapping: retry
				}
				return fmt.Errorf("writer %d exhausted retries", w)
			}()
			if err != nil {
				t.Errorf("writer %d: %v", w, err)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	flapWG.Wait()

	head := net.HeadManifestID(baseID)

	// Recovered nodes: converge every chunk at its replica set.
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}

	got, final, err := net.GetObject(ctx, head, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("payload altered by coordinated RMW")
	}
	if final.Unknown["init"] != json.Number(huge) {
		t.Fatalf("initial object extension lost: %#v", final.Unknown)
	}
	for w := 0; w < writers; w++ {
		key := fmt.Sprintf("tool-%02d", w)
		want := json.Number(fmt.Sprintf("9200000000000000000000000000000000%04d", w))
		if final.Unknown[key] != want {
			t.Fatalf("object extension from %s missing: %#v", key, final.Unknown)
		}
		for i, e := range final.Entries {
			if e.Attrs[key] != want {
				t.Fatalf("chunk %d attr from %s missing: %#v", i, key, e.Attrs)
			}
		}
	}

	// Final manifest and every data chunk at full replication on recovery.
	if h, names := countHealthy(t, ctx, net.preferredNodes(head, 3), head); h != 3 {
		t.Fatalf("head manifest at %d/3 replicas %v", h, names)
	}
	for _, e := range final.Entries {
		if h, names := countHealthy(t, ctx, net.preferredNodes(e.ID, 3), e.ID); h != 3 {
			t.Fatalf("chunk %s at %d/3 %v", e.ID, h, names)
		}
	}
	t.Logf("writers=%d head=%s basis=coordinated-cas-merge+repair-converges", writers, head)
}

// TestCoordinatedCASExplicitMerge: two tools independently derive edits
// from the same base and both call PutManifestCAS with the same base; the
// second commit must observe the first successor and produce a merged
// manifest (both keys present), returned as the new lineage head.
func TestCoordinatedCASExplicitMerge(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3, ChunkSize: 64}
	payload := []byte("hello world, deterministic chunks here we go padding")
	baseID, baseMan, err := net.PutObject(ctx, "o.bin", payload, opt)
	if err != nil {
		t.Fatal(err)
	}

	// Tool A derives from base.
	a := cloneManifest(baseMan)
	if a.Unknown == nil {
		a.Unknown = map[string]any{}
	}
	a.Unknown["a"] = "1"
	a.Entries[0].SetAttr("a", json.Number(huge))
	idA, err := net.PutManifestCAS(ctx, baseID, a, opt)
	if err != nil {
		t.Fatal(err)
	}

	// Tool B still holds the base and commits its own edit: must merge.
	b := cloneManifest(baseMan)
	if b.Unknown == nil {
		b.Unknown = map[string]any{}
	}
	b.Unknown["b"] = "2"
	b.Entries[0].SetAttr("b", nil)
	idB, err := net.PutManifestCAS(ctx, baseID, b, opt)
	if err != nil {
		t.Fatalf("stale-base CAS must merge, got %v", err)
	}
	if idB == idA {
		t.Fatal("merged commit must be a new manifest ID")
	}
	if net.HeadManifestID(baseID) != idB {
		t.Fatal("head must point at merged commit")
	}

	_, final, err := net.GetObject(ctx, idB, opt)
	if err != nil {
		t.Fatal(err)
	}
	if final.Unknown["a"] != "1" || final.Unknown["b"] != "2" {
		t.Fatalf("extensions not merged: %#v", final.Unknown)
	}
	attrs := final.Entries[0].Attrs
	if attrs["a"] != json.Number(huge) {
		t.Fatalf("tool-a attr lost on merge: %#v", attrs)
	}
	if v, ok := attrs["b"]; !ok || v != nil {
		t.Fatalf("tool-b null attr lost on merge: %#v", attrs)
	}
	t.Logf("base=%s a=%s merged=%s basis=stale-base-merged-not-overwritten", baseID, idA, idB)
}

func cloneManifest(m *manifest.Manifest) *manifest.Manifest {
	cp := *m
	cp.Unknown = cloneMap(m.Unknown)
	cp.Entries = make([]manifest.Entry, len(m.Entries))
	for i, e := range m.Entries {
		ne := e
		ne.Attrs = cloneMap(e.Attrs)
		ne.Unknown = cloneMap(e.Unknown)
		cp.Entries[i] = ne
	}
	return &cp
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// TestCoordinatedMergeOnNullAttrs: a base manifest whose entry carries an
// explicit "attrs": null; two writers concurrently attach distinct keys
// to that entry. The coordinated merge must promote it to an object
// containing both keys (not lose either), while other entries' untouched
// null attrs stay explicit null.
func TestCoordinatedMergeOnNullAttrs(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3, ChunkSize: 64}
	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i)
	}
	c := chunkFor(string(payload))
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"version":2,"name":"n","entries":[` +
		`{"id":"` + string(c.ID) + `","size":64,"attrs":null},` +
		`{"id":"` + string(c.ID) + `","size":64,"attrs":null}]}`)
	baseID := seedManifestChunk(t, ctx, net, raw, opt)

	var wg sync.WaitGroup
	for _, key := range []string{"tool-a", "tool-b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, err := net.ModifyManifest(ctx, baseID, func(m *manifest.Manifest) error {
				m.Entries[0].SetAttr(key, "ok")
				return nil
			}, opt)
			if err != nil {
				t.Errorf("modify %s: %v", key, err)
			}
		}(key)
	}
	wg.Wait()

	head := net.HeadManifestID(baseID)
	_, final, err := net.GetObject(ctx, head, opt)
	if err != nil {
		t.Fatal(err)
	}
	a0 := final.Entries[0]
	if a0.AttrsForm() != manifest.AttrsObject {
		t.Fatalf("null attrs edited by both must merge to object, got %d", a0.AttrsForm())
	}
	if a0.Attrs["tool-a"] != "ok" || a0.Attrs["tool-b"] != "ok" {
		t.Fatalf("one writer's attr lost: %#v", a0.Attrs)
	}
	// Entry 1 was never touched: stays explicit null.
	if final.Entries[1].AttrsForm() != manifest.AttrsNull {
		t.Fatalf("untouched null attrs must stay null, got %d", final.Entries[1].AttrsForm())
	}
}
