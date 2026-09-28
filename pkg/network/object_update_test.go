package network

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/manifest"
)

// seedNamedObject writes an arbitrary serialized manifest as a chunk and
// registers it as the current head of name (after ensuring its referenced
// payload chunks already exist).
func seedNamedObject(t *testing.T, ctx context.Context, net *Network, name string,
	opt Options, rawManifest []byte) chunk.ID {
	t.Helper()
	id := seedManifestChunk(t, ctx, net, rawManifest, opt)
	net.heads.commit(name, id)
	return id
}

func entryJSON(c chunk.Chunk) string {
	return `{"id":"` + string(c.ID) + `","size":` + strconv.Itoa(len(c.Data))
}

// TestChunkExtensionsEndToEndReadModifyWrite: an object with rich chunk-
// and object-level extensions is stored, one known extension is changed
// through UpdateObject, and the object is fetched by the new head; every
// untouched extension at both levels must survive exactly and the object
// bytes reconstruct.
func TestChunkExtensionsEndToEndReadModifyWrite(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3, ChunkSize: 64}

	c1, c2 := chunkFor("first chunk payload!!"), chunkFor("second chunk payload differs")
	for _, c := range []chunk.Chunk{c1, c2} {
		if _, err := net.PutChunk(ctx, c, opt); err != nil {
			t.Fatal(err)
		}
	}
	raw := `{"version":2,"name":"rich-obj","entries":[
		` + entryJSON(c1) + `,"x-tags":["a","b"],"x-meta":{"deep":{"n":9007199254740993},"ok":true,"nil":null}},
		` + entryJSON(c2) + `,"x-offset":123456789012345678901234567890,"x-only-two":"keep"}
	],"x-object":{"p":"v"},"x-global-big":987654321098765432109876543210}`
	head := seedNamedObject(t, ctx, net, "rich-obj", opt, []byte(raw))

	// Read-modify-write: change one known extension of entry 0 and bump one
	// object-level field; everything else must remain untouched.
	newHead, _, err := net.UpdateObject(ctx, "rich-obj", opt, func(m *manifest.Manifest) error {
		m.Entries[0].Unknown["x-tags"] = []any{"a", "b", "c"}
		m.Unknown["x-object"] = map[string]any{"p": "v2"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if newHead == head {
		t.Fatalf("manifest content id should change when extensions change")
	}
	if cur, ok := net.CurrentObject("rich-obj"); !ok || cur != newHead {
		t.Fatalf("head not advanced: cur=%s want=%s ok=%v", cur, newHead, ok)
	}

	// Fetch through the normal object path by the new head.
	data, got, err := net.GetObject(ctx, newHead, opt)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(append(append([]byte{}, c1.Data...), c2.Data...)) {
		t.Fatalf("object payload reconstruction changed")
	}
	if got.Version != manifest.CurrentVersion || len(got.Entries) != 2 {
		t.Fatalf("manifest structure wrong: %+v", got)
	}

	// The modified fields reflect the change.
	tags, _ := got.Entries[0].Unknown["x-tags"].([]any)
	if len(tags) != 3 {
		t.Fatalf("modified chunk extension wrong: %#v", got.Entries[0].Unknown["x-tags"])
	}
	if obj, _ := got.Unknown["x-object"].(map[string]any); obj["p"] != "v2" {
		t.Fatalf("modified object extension wrong: %#v", got.Unknown["x-object"])
	}

	// All untouched extensions — including big integers at both levels —
	// are preserved exactly.
	meta := got.Entries[0].Unknown["x-meta"].(map[string]any)
	deep := meta["deep"].(map[string]any)
	if deep["n"].(json.Number).String() != "9007199254740993" {
		t.Fatalf("nested chunk big int lost: %v", deep["n"])
	}
	if meta["ok"] != true || meta["nil"] != nil {
		t.Fatalf("bool/null chunk extension changed: %#v", meta)
	}
	if got := got.Entries[1].Unknown["x-offset"].(json.Number).String(); got != "123456789012345678901234567890" {
		t.Fatalf("entry1 big int lost digits: %s", got)
	}
	if got.Entries[1].Unknown["x-only-two"] != "keep" {
		t.Fatalf("entry1 untouched string extension lost: %#v", got.Entries[1].Unknown)
	}
	if got := got.Unknown["x-global-big"].(json.Number).String(); got != "987654321098765432109876543210" {
		t.Fatalf("object-level big int lost digits: %s", got)
	}
	t.Logf("object=rich-obj old-head=%s new-head=%s basis=read-modify-write-preserves-all-extensions", head, newHead)
}

// TestConcurrentReadModifyWriteNoLostExtensions: many writers each add a
// distinct marker to object-level extensions and to one chunk's extensions.
// After all updates complete every marker from every writer must be
// present: concurrent RMW must not overwrite or lose extensions.
func TestConcurrentReadModifyWriteNoLostExtensions(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 6)
	opt := Options{Replication: 3, ChunkSize: 64}

	payload := make([]byte, 100)
	head0, _, err := net.PutObject(ctx, "shared", payload, opt)
	if err != nil {
		t.Fatal(err)
	}

	const writers = 24
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			marker := fmt.Sprintf("writer-%02d", w)
			_, _, uerr := net.UpdateObject(ctx, "shared", opt, func(m *manifest.Manifest) error {
				if m.Unknown == nil {
					m.Unknown = map[string]any{}
				}
				bag, _ := m.Unknown["x-writers"].([]any)
				// Each writer appends its own marker; serialization ensures
				// a writer always observes all earlier markers.
				m.Unknown["x-writers"] = append(bag, marker)
				e := &m.Entries[0]
				if e.Unknown == nil {
					e.Unknown = map[string]any{}
				}
				cbag, _ := e.Unknown["x-chunk-writers"].([]any)
				e.Unknown["x-chunk-writers"] = append(cbag, marker)
				return nil
			})
			if uerr != nil {
				errs <- uerr
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent update failed: %v", e)
	}

	head, ok := net.CurrentObject("shared")
	if !ok || head == head0 {
		t.Fatalf("head did not advance: ok=%v head=%s", ok, head)
	}
	_, man, err := net.GetObject(ctx, head, opt)
	if err != nil {
		t.Fatal(err)
	}
	objWriters, _ := man.Unknown["x-writers"].([]any)
	chunkWriters, _ := man.Entries[0].Unknown["x-chunk-writers"].([]any)
	if len(objWriters) != writers || len(chunkWriters) != writers {
		t.Fatalf("lost updates: object markers=%d chunk markers=%d want=%d",
			len(objWriters), len(chunkWriters), writers)
	}
	seen := map[string]bool{}
	for _, v := range objWriters {
		seen[v.(string)] = true
	}
	if len(seen) != writers {
		t.Fatalf("duplicate/missing object markers: %v", objWriters)
	}
	seenC := map[string]bool{}
	for _, v := range chunkWriters {
		seenC[v.(string)] = true
	}
	if len(seenC) != writers {
		t.Fatalf("duplicate/missing chunk markers: %v", chunkWriters)
	}
	t.Logf("object=shared writers=%d final-head=%s basis=serialized-rmw-no-lost-update", writers, head)
}

// TestExtensionsSurviveNodeOutageAndRepair: a committed manifest carrying
// extensions survives a preferred node losing its copy and going offline;
// after the node returns, background repair restores it from a verified
// replica and all replicas converge to the exact same extension-bearing
// bytes.
func TestExtensionsSurviveNodeOutageAndRepair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	net, _ := newTestNetwork(t, 5)
	opt := Options{Replication: 3}
	c := chunkFor("ext-payload")
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	raw := `{"version":2,"name":"ext-obj","entries":[` + entryJSON(c) +
		`,"x-tags":["keep"],"x-big":123456789012345678901234567890}],"x-top":{"q":7}}`
	head := seedNamedObject(t, ctx, net, "ext-obj", opt, []byte(raw))
	preferred := net.preferredNodes(head, 3)

	loopDone := make(chan struct{})
	go func() {
		_ = net.RepairLoop(ctx, 10*time.Millisecond, opt)
		close(loopDone)
	}()

	// Disk loss + outage on one preferred node.
	if err := preferred[0].Delete(ctx, head); err != nil {
		t.Fatal(err)
	}
	preferred[0].SetAvailable(false)

	// While it is away, reads must still serve the exact extensions from
	// another verified replica.
	time.Sleep(30 * time.Millisecond)
	_, man, err := net.GetObject(ctx, head, opt)
	if err != nil {
		t.Fatalf("read during outage: %v", err)
	}
	if got := man.Entries[0].Unknown["x-big"].(json.Number).String(); got != "123456789012345678901234567890" {
		t.Fatalf("big int changed while a node was down: %s", got)
	}
	if tags, _ := man.Entries[0].Unknown["x-tags"].([]any); len(tags) != 1 {
		t.Fatalf("chunk extensions served wrong during outage: %#v", man.Entries[0].Unknown)
	}

	// Node returns: repair converges and its restored copy is the exact
	// extension-bearing bytes.
	preferred[0].SetAvailable(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		healthy, _ := countHealthy(t, ctx, preferred, head)
		if healthy == 3 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	restored, err := preferred[0].Get(ctx, head)
	if err != nil {
		t.Fatalf("restored copy unreadable: %v", err)
	}
	if restored.ID != head || !bytes.Equal(restored.Data, []byte(raw)) {
		t.Fatalf("restored manifest diverged from committed bytes:\nwant=%s\ngot =%s", raw, restored.Data)
	}
	re, err := manifest.Unmarshal(restored.Data)
	if err != nil {
		t.Fatal(err)
	}
	if got := re.Entries[0].Unknown["x-big"].(json.Number).String(); got != "123456789012345678901234567890" {
		t.Fatalf("restored copy lost big int digits: %s", got)
	}
	if re.Unknown["x-top"].(map[string]any)["q"].(json.Number).String() != "7" {
		t.Fatalf("restored copy lost object-level extension: %#v", re.Unknown)
	}
	t.Logf("manifest=%s converged-nodes=3 basis=restored-verified-extension-bytes-after-outage", head)
	cancel()
	<-loopDone
}

// TestUpdateObjectFailureDoesNotAdvanceHead: when the new manifest cannot
// be written with the required replica factor (preferred nodes down),
// UpdateObject fails, the head stays at the old committed manifest, and a
// retry after nodes return builds on that head without losing extensions.
func TestUpdateObjectFailureDoesNotAdvanceHead(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 4)
	opt := Options{Replication: 3}
	c := chunkFor("failover-payload")
	if _, err := net.PutChunk(ctx, c, opt); err != nil {
		t.Fatal(err)
	}
	raw := `{"version":2,"name":"f","entries":[` + entryJSON(c) + `,"x-a":"keep"}]}`
	head := seedNamedObject(t, ctx, net, "f", opt, []byte(raw))

	// Take down two of the three manifest-replica nodes: the new manifest
	// cannot reach RF=3.
	pref := net.preferredNodes(head, 3)
	pref[0].SetAvailable(false)
	pref[1].SetAvailable(false)
	_, _, err := net.UpdateObject(ctx, "f", opt, func(m *manifest.Manifest) error {
		m.Unknown["x-new"] = "v"
		return nil
	})
	if !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("want ErrNotEnoughReplicas, got %v", err)
	}
	if cur, ok := net.CurrentObject("f"); !ok || cur != head {
		t.Fatalf("head advanced despite failed write: cur=%s old=%s", cur, head)
	}

	// Nodes return: a retry must start from the old (extension-bearing)
	// head and still preserve its chunk extension.
	pref[0].SetAvailable(true)
	pref[1].SetAvailable(true)
	newHead, _, err := net.UpdateObject(ctx, "f", opt, func(m *manifest.Manifest) error {
		m.Unknown["x-new"] = "v"
		return nil
	})
	if err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	_, man, err := net.GetObject(ctx, newHead, opt)
	if err != nil {
		t.Fatal(err)
	}
	if man.Entries[0].Unknown["x-a"] != "keep" {
		t.Fatalf("old chunk extension lost after failed-then-retry update: %#v", man.Entries[0].Unknown)
	}
	if man.Unknown["x-new"] != "v" {
		t.Fatalf("retrying update did not apply: %#v", man.Unknown)
	}
	t.Logf("old-head=%s new-head=%s basis=failed-write-keeps-committed-head", head, newHead)
}

// TestConcurrentUpdatesDuringNodeFlap: writers update object- and
// chunk-level extensions while one preferred node flaps; all confirmed
// writes remain, no update loses an earlier marker, and after repair the
// final head is present on every replica with identical bytes.
func TestConcurrentUpdatesDuringNodeFlap(t *testing.T) {
	ctx := context.Background()
	net, _ := newTestNetwork(t, 6)
	opt := Options{Replication: 3}
	if _, _, err := net.PutObject(ctx, "flap-obj", make([]byte, 40), opt); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var flapWG sync.WaitGroup
	flapWG.Add(1)
	go func() {
		defer flapWG.Done()
		nd := net.sorted[2]
		up := true
		for {
			select {
			case <-stop:
				nd.SetAvailable(true)
				return
			default:
				up = !up
				nd.SetAvailable(up)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	const writers = 20
	var wg sync.WaitGroup
	var failed int64
	var mu sync.Mutex
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			marker := fmt.Sprintf("m%02d", w)
			// Retry loop: a writer retries its marker append until its
			// update is confirmed, re-reading the head every attempt.
			for {
				_, _, err := net.UpdateObject(ctx, "flap-obj", opt, func(m *manifest.Manifest) error {
					bag, _ := m.Unknown["x-ms"].([]any)
					for _, v := range bag {
						if v == marker {
							return nil // already appended by a retried attempt
						}
					}
					m.Unknown["x-ms"] = append(bag, marker)
					m.Entries[0].Unknown["x-c"] = json.Number(strconv.Itoa(w))
					return nil
				})
				if err == nil {
					return
				}
				mu.Lock()
				failed++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	flapWG.Wait()

	head, ok := net.CurrentObject("flap-obj")
	if !ok {
		t.Fatal("no head")
	}
	// Repair until RF replicas hold the final head.
	if _, err := net.Repair(ctx, opt); err != nil {
		t.Fatal(err)
	}
	pref := net.preferredNodes(head, 3)
	healthy, names := countHealthy(t, ctx, pref, head)
	if healthy != 3 {
		t.Fatalf("final head not converged to 3: healthy=%d names=%v retries=%d", healthy, names, failed)
	}
	_, man, err := net.GetObject(ctx, head, opt)
	if err != nil {
		t.Fatal(err)
	}
	markers, _ := man.Unknown["x-ms"].([]any)
	if len(markers) != writers {
		t.Fatalf("markers lost during flap: got %d want %d: %v", len(markers), writers, markers)
	}
	seen := map[string]bool{}
	for _, v := range markers {
		seen[v.(string)] = true
	}
	if len(seen) != writers {
		t.Fatalf("duplicate markers: %v", markers)
	}
	// The final head's chunk-level extension is intact as well.
	if n := man.Entries[0].Unknown["x-c"].(json.Number); n.String() == "" {
		t.Fatal("chunk extension missing on final head")
	}
	t.Logf("final-head=%s markers=%d retried-updates=%d converged=%v basis=confirmed-writes-survive-flap",
		head, writers, failed, names)
}
