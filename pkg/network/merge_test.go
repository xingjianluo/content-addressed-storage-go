package network

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
)

// TestConcurrentConflictingWritesMergeWithoutLoss: two upper-layer tools
// read the same manifest version concurrently, each adds its own
// object-level and per-chunk extensions, and both write back while a node
// flaps. Content addressing means both confirmed versions survive as
// distinct IDs; reconciling them with manifest.MergeExtensions and
// re-persisting yields a final version in which neither tool's confirmed
// extensions are lost, and after the flapping node returns every replica
// set converges.
func TestConcurrentConflictingWritesMergeWithoutLoss(t *testing.T) {
	ctx := context.Background()
	net, nodes := newTestNetwork(t, 5)
	opt := Options{Replication: 3, ChunkSize: 64}

	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i*3 + 1)
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
	baseID, baseMan, err := net.PutObject(ctx, "shared.bin", payload, baseOpt)
	if err != nil {
		t.Fatal(err)
	}

	// Flap one node for the whole exercise.
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

	// Both tools start from the same base version simultaneously.
	type result struct {
		tool string
		id   chunk.ID
		man  *manifest.Manifest
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	toolValue := func(tool string) json.Number {
		// Distinct huge integers per tool, beyond float64 precision.
		if tool == "tool-a" {
			return json.Number("9300000000000000000000000000000000000001")
		}
		return json.Number("9300000000000000000000000000000000000002")
	}
	for _, tool := range []string{"tool-a", "tool-b"} {
		go func(tool string) {
			<-start
			var res result
			res.tool = tool
			for attempt := 0; attempt < 200; attempt++ {
				_, man, gerr := net.GetObject(ctx, baseID, opt)
				if gerr != nil {
					time.Sleep(time.Millisecond)
					continue // node flapping: retry, never write blindly
				}
				if man.Unknown == nil {
					man.Unknown = map[string]any{}
				}
				man.Unknown[tool] = toolValue(tool)
				for i := range man.Entries {
					if man.Entries[i].Attrs == nil {
						man.Entries[i].Attrs = map[string]any{}
					}
					man.Entries[i].Attrs[tool] = toolValue(tool)
				}
				id, perr := net.PutManifest(ctx, man, opt)
				if perr != nil {
					t.Logf("tool %s put failed: %v", tool, perr)
					time.Sleep(time.Millisecond)
					continue
				}
				res.id, res.man, res.err = id, man, nil
				results <- res
				return
			}
			res.err = context.DeadlineExceeded
			results <- res
		}(tool)
	}
	close(start)
	a, b := <-results, <-results
	close(stop)
	flapWG.Wait()
	for _, r := range []result{a, b} {
		if r.err != nil {
			t.Fatalf("tool %s never confirmed a write: %v", r.tool, r.err)
		}
	}
	if a.id == b.id {
		t.Fatalf("both tools produced the same manifest id %s; test setup broken", a.id)
	}
	t.Logf("confirmed divergent versions %s=%s %s=%s basis=content-addressed-no-overwrite",
		a.tool, a.id, b.tool, b.id)

	// Reconcile: merge the loser's confirmed extensions into the winner's
	// manifest and persist the union as the new latest version.
	winner, loser := a, b
	if b.id < a.id { // deterministic winner: smaller content ID
		winner, loser = b, a
	}
	if !manifest.MergeExtensions(winner.man, loser.man) {
		t.Fatal("merge reported no change; loser's extensions were not folded in")
	}
	mergedID, err := net.PutManifest(ctx, winner.man, opt)
	if err != nil {
		t.Fatal(err)
	}
	// Merging again is a no-op: re-persisting yields the same content ID.
	if manifest.MergeExtensions(winner.man, loser.man) {
		t.Fatal("second merge not idempotent")
	}
	againID, err := net.PutManifest(ctx, winner.man, opt)
	if err != nil {
		t.Fatal(err)
	}
	if againID != mergedID {
		t.Fatalf("merged manifest not reproducible: %s vs %s", mergedID, againID)
	}

	// Node recovered: converge every confirmed version at its replica set.
	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}

	// The final read of the merged version carries BOTH tools' extensions.
	got, final, err := net.GetObject(ctx, mergedID, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("payload altered by concurrent writes")
	}
	for _, tool := range []string{"tool-a", "tool-b"} {
		if final.Unknown[tool] != toolValue(tool) {
			t.Fatalf("object extension of %s lost: %#v", tool, final.Unknown)
		}
		for i, e := range final.Entries {
			if e.Attrs[tool] != toolValue(tool) {
				t.Fatalf("chunk %d attr of %s lost: %#v", i, tool, e.Attrs)
			}
			if e.Attrs["init"] != json.Number(huge) {
				t.Fatalf("base chunk %d attr lost: %#v", i, e.Attrs)
			}
		}
	}
	if final.Unknown["init"] != json.Number(huge) {
		t.Fatalf("base object extension lost: %#v", final.Unknown)
	}

	// Every confirmed version (base, both divergent, merged) converges to
	// full replication after the flapping node returned.
	confirmed := []chunk.ID{baseID, a.id, b.id, mergedID}
	for _, mid := range confirmed {
		if healthy, names := countHealthy(t, ctx, net.preferredNodes(mid, 3), mid); healthy != 3 {
			t.Fatalf("confirmed manifest %s at %d/3 replicas %v", mid, healthy, names)
		}
	}
	for _, e := range baseMan.Entries {
		if healthy, names := countHealthy(t, ctx, net.preferredNodes(e.ID, 3), e.ID); healthy != 3 {
			t.Fatalf("data chunk %s at %d/3 replicas %v", e.ID, healthy, names)
		}
	}
	t.Logf("merged=%s confirmed=%d basis=merge-extensions-union+verified-repair-converges",
		mergedID, len(confirmed))
}
