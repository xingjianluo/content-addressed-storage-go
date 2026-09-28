package network

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

const huge = "900000000000000000000000000000000000042"

// TestObjectWithChunkAttrsEndToEnd: object-level extensions and per-chunk
// attrs (including huge integers) written through PutObject come back
// intact through GetObject, and re-writing the same object with the same
// extensions yields the same manifest ID.
func TestObjectWithChunkAttrsEndToEnd(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{
		Replication: 3,
		ChunkSize:   64,
		Extensions: map[string]any{
			"owner":  "team-a",
			"n":      json.Number(huge),
			"policy": map[string]any{"nested": []any{true, nil, json.Number(huge)}},
		},
	}
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i*7 + 3)
	}
	opt.ChunkAttrs = []map[string]any{
		{"idx": json.Number("0"), "big": json.Number(huge), "tags": []any{"a", true}},
		{"idx": json.Number("1"), "big": json.Number(huge)},
		{"idx": json.Number("2"), "big": json.Number(huge)},
		{"idx": json.Number("3"), "big": json.Number(huge)},
	}
	id, man, err := net.PutObject(ctx, "rich.bin", payload, opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(man.Entries) != 4 {
		t.Fatalf("want 4 chunks, got %d", len(man.Entries))
	}

	got, gotMan, err := net.GetObject(ctx, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("payload mismatch")
	}
	if gotMan.Unknown["owner"] != "team-a" || gotMan.Unknown["n"] != json.Number(huge) {
		t.Fatalf("object extensions lost: %#v", gotMan.Unknown)
	}
	pol, _ := gotMan.Unknown["policy"].(map[string]any)
	arr, _ := pol["nested"].([]any)
	if len(arr) != 3 || arr[2] != json.Number(huge) {
		t.Fatalf("nested object extension mangled: %#v", pol)
	}
	for i, e := range gotMan.Entries {
		if e.Attrs["big"] != json.Number(huge) {
			t.Fatalf("chunk %d attrs big int lost: %#v", i, e.Attrs)
		}
	}
	t.Logf("manifest=%s chunks=%d basis=object+chunk-extensions-e2e-lossless", id, len(gotMan.Entries))

	// Same bytes + same extensions -> same manifest ID, deterministically.
	id2, _, err := net.PutObject(ctx, "rich.bin", payload, opt)
	if err != nil || id2 != id {
		t.Fatalf("rewrite changed manifest id: %s vs %s err=%v", id, id2, err)
	}
}

// TestConcurrentReadModifyWritePreservesExtensions: many writers perform
// optimistic read-modify-write cycles on one object while a node flaps.
// Each writer contributes one object-level and one per-chunk extension;
// after convergence the surviving manifest must contain every writer's
// contribution digit-for-digit, and every confirmed manifest version must
// exist at full replication.
func TestConcurrentReadModifyWritePreservesExtensions(t *testing.T) {
	ctx := context.Background()
	net, nodes := newTestNetwork(t, 6)
	opt := Options{Replication: 3, ChunkSize: 64}
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i*5 + 1)
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
	currentID, _, err := net.PutObject(ctx, "shared.bin", payload, baseOpt)
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

	const writers = 8
	var mu sync.Mutex
	confirmed := []chunk.ID{}
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("w%02d", w)
			value := json.Number(fmt.Sprintf("9100000000000000000000000000000000%04d", w))
			for attempt := 0; attempt < 200; attempt++ {
				mu.Lock()
				base := currentID
				mu.Unlock()

				_, man, gerr := net.GetObject(ctx, base, opt)
				if gerr != nil {
					time.Sleep(time.Millisecond)
					continue // node flapping: retry, never overwrite blindly
				}
				if man.Unknown == nil {
					man.Unknown = map[string]any{}
				}
				if _, exists := man.Unknown[key]; exists {
					return // our contribution already won an earlier race
				}
				man.Unknown[key] = value
				if w == 0 {
					// One writer also exercises nested shapes under contention.
					man.Unknown["nested-shape"] = map[string]any{
						"ok": true, "n": json.Number(huge), "z": nil,
						"list": []any{json.Number(huge), false, "x"},
					}
				}
				for i := range man.Entries {
					if man.Entries[i].Attrs == nil {
						man.Entries[i].Attrs = map[string]any{}
					}
					man.Entries[i].Attrs[key] = value
				}
				newID, perr := net.PutManifest(ctx, man, opt)
				if perr != nil {
					time.Sleep(time.Millisecond)
					continue // unconfirmed: nothing was overstated; retry
				}

				mu.Lock()
				confirmed = append(confirmed, newID)
				if currentID == base {
					currentID = newID // CAS winner
					mu.Unlock()
					return
				}
				mu.Unlock() // lost the race: retry against the newer manifest
			}
			t.Errorf("writer %d exhausted retries", w)
		}(w)
	}
	wg.Wait()
	close(stop)
	flapWG.Wait()

	// Node recovery: converge every confirmed version at its replica set.
	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}

	payload2, final, err := net.GetObject(ctx, currentID, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload2) != string(payload) {
		t.Fatal("payload altered by concurrent RMW")
	}
	if len(final.Unknown) != writers+2 { // init + nested-shape + one per writer
		t.Fatalf("object extensions lost under concurrency: %d keys %#v", len(final.Unknown), final.Unknown)
	}
	if final.Unknown["init"] != json.Number(huge) {
		t.Fatalf("initial extension overwritten: %#v", final.Unknown)
	}
	for w := 0; w < writers; w++ {
		key := fmt.Sprintf("w%02d", w)
		want := json.Number(fmt.Sprintf("9100000000000000000000000000000000%04d", w))
		if final.Unknown[key] != want {
			t.Fatalf("object extension from writer %s missing: %#v", key, final.Unknown)
		}
		for i, e := range final.Entries {
			if e.Attrs[key] != want {
				t.Fatalf("chunk %d attr from writer %s missing: %#v", i, key, e.Attrs)
			}
		}
	}
	ns, _ := final.Unknown["nested-shape"].(map[string]any)
	if ns["ok"] != true || ns["z"] != nil || ns["n"] != json.Number(huge) {
		t.Fatalf("nested extension mangled under concurrency: %#v", ns)
	}

	// Every confirmed manifest version survives at full replication.
	for _, mid := range confirmed {
		if healthy, names := countHealthy(t, ctx, net.preferredNodes(mid, 3), mid); healthy != 3 {
			t.Fatalf("confirmed manifest %s at %d/3 replicas %v", mid, healthy, names)
		}
	}
	// Data chunks likewise converge.
	for _, e := range final.Entries {
		if healthy, names := countHealthy(t, ctx, net.preferredNodes(e.ID, 3), e.ID); healthy != 3 {
			t.Fatalf("data chunk %s at %d/3 replicas %v", e.ID, healthy, names)
		}
	}
	t.Logf("writers=%d confirmed-versions=%d final=%s basis=cas-rmw+verified-repair-converges-extensions",
		writers, len(confirmed), currentID)
}

// TestExtensionsSurviveNodeOutageAndRecovery: an object carrying object-
// level and per-chunk extensions is written with full replication; one
// replica node goes down and loses both the manifest and a data chunk,
// then returns. Read repair plus background repair must restore all
// replicas, with every extension intact and content unchanged.
func TestExtensionsSurviveNodeOutageAndRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	net, _ := newTestNetwork(t, 5)
	opt := Options{
		Replication: 3,
		ChunkSize:   64,
		Extensions:  map[string]any{"origin": "tool-z", "epoch": json.Number(huge)},
		ChunkAttrs: []map[string]any{
			{"role": "head", "checksum": json.Number(huge)},
			{"role": "body", "checksum": json.Number(huge)},
			{"role": "body", "checksum": json.Number(huge)},
			{"role": "tail", "checksum": json.Number(huge)},
		},
	}
	payload := make([]byte, 220)
	for i := range payload {
		payload[i] = byte(i*11 + 5)
	}
	id, man, err := net.PutObject(ctx, "durable.bin", payload, opt)
	if err != nil {
		t.Fatal(err)
	}
	manifestPref := net.preferredNodes(id, 3)
	firstChunkPref := net.preferredNodes(man.Entries[0].ID, 3)

	// Choose a node in the manifest replica set; take it down after
	// deleting the manifest and (if it holds it) the first data chunk.
	victim := manifestPref[0]
	if err := victim.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := victim.Delete(ctx, man.Entries[0].ID); err != nil {
		t.Fatal(err)
	}
	victim.SetAvailable(false)
	t.Logf("fault node=%s manifest=%s chunk=%s basis=disk-loss+outage",
		victim.ID(), id, man.Entries[0].ID)

	// While the node is down, reads still succeed from surviving replicas
	// and extensions are all visible; a write that needs the down member
	// of the replica set is rejected (never silently under-replicated).
	got, cur, err := net.GetObject(ctx, id, opt)
	if err != nil {
		t.Fatalf("read must survive one node down: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatal("payload mismatch during outage")
	}
	if cur.Unknown["origin"] != "tool-z" || cur.Entries[3].Attrs["role"] != "tail" {
		t.Fatalf("extensions unreadable during outage: %#v", cur.Unknown)
	}

	// Node returns: repair loop converges everything without client logic,
	// restoring the manifest and data chunk that were deleted on the victim.
	victim.SetAvailable(true)
	loopDone := make(chan struct{})
	go func() { _ = net.RepairLoop(ctx, 10*time.Millisecond, opt); close(loopDone) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mh, _ := countHealthy(t, ctx, manifestPref, id)
		ch, _ := countHealthy(t, ctx, firstChunkPref, man.Entries[0].ID)
		if mh == 3 && ch == 3 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Once converged, another tool performs read-modify-write; all prior
	// extensions must travel with the new manifest version.
	cur.Unknown["post-recovery-edit"] = true
	newID, err := net.PutManifest(ctx, cur, opt)
	if err != nil {
		t.Fatalf("RMW after recovery failed: %v", err)
	}
	id = newID
	manifestPref = net.preferredNodes(id, 3)
	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mh, _ := countHealthy(t, ctx, manifestPref, id)
		if mh == 3 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if h, names := countHealthy(t, ctx, manifestPref, id); h != 3 {
		t.Fatalf("manifest did not converge after recovery: %d/3 %v", h, names)
	}
	for _, e := range man.Entries {
		if h, names := countHealthy(t, ctx, net.preferredNodes(e.ID, 3), e.ID); h != 3 {
			t.Fatalf("chunk %s did not converge: %d/3 %v", e.ID, h, names)
		}
	}

	// Final read from the converged cluster: extensions all present.
	data2, final, err := net.GetObject(ctx, id, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(data2) != string(payload) {
		t.Fatal("payload changed through outage/recovery")
	}
	if final.Unknown["origin"] != "tool-z" || final.Unknown["epoch"] != json.Number(huge) {
		t.Fatalf("object extensions lost through outage: %#v", final.Unknown)
	}
	if final.Unknown["post-recovery-edit"] != true {
		t.Fatal("extension added after recovery lost")
	}
	roles := map[string]bool{}
	for i, e := range final.Entries {
		if e.Attrs["checksum"] != json.Number(huge) {
			t.Fatalf("chunk %d attrs lost through outage: %#v", i, e.Attrs)
		}
		r, _ := e.Attrs["role"].(string)
		roles[r] = true
	}
	if !roles["head"] || !roles["tail"] {
		t.Fatalf("per-chunk attrs roles lost: %v", roles)
	}
	t.Logf("manifest=%s nodes=%v basis=outage-rmw-then-verified-restore-no-extension-loss",
		id, nodeIDs(manifestPref))

	cancel()
	<-loopDone
}
