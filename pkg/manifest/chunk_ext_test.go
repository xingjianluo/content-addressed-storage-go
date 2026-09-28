package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// parseTree decodes JSON with UseNumber so large integers are compared
// digit-exactly rather than as float64 approximations.
func parseTree(t *testing.T, raw []byte) any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		t.Fatalf("parse tree: %v: %s", err, raw)
	}
	return tree
}

// extV2 is a manifest exercising every supported extension shape: nested
// objects, arrays, booleans, null and integers beyond float64 precision —
// at both object level and chunk (entry) level.
const extV2 = `{
  "version": 2,
  "name": "rich",
  "entries": [
    {"id": "aa00000000000000000000000000000000000000000000000000000000000001", "size": 3,
     "x-tags": ["hot", "audit"],
     "x-meta": {"nested": {"deep": true}, "list": [1, 2, 3], "flag": false, "empty": null},
     "x-big": 9007199254740993},
    {"id": "bb00000000000000000000000000000000000000000000000000000000000002", "size": 5,
     "x-offset": 123456789012345678901234567890,
     "x-flags": [true, false, null],
     "x-huge-neg": -987654321098765432109876543210}
  ],
  "x-object-level": {"policy": "keep", "quorum": 7, "note": null},
  "x-global-big": 1234567890123456789012345678901234567890
}`

// TestChunkExtensionsLosslessRoundTrip: chunk-level extensions of every
// JSON shape must survive unmarshal -> modify a built-in field -> marshal
// -> re-unmarshal untouched, at both object and chunk level.
func TestChunkExtensionsLosslessRoundTrip(t *testing.T) {
	m, err := Unmarshal([]byte(extV2))
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 2 {
		t.Fatalf("want version 2, got %d", m.Version)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(m.Entries))
	}

	// Spot-check the parsed shapes.
	e0 := m.Entries[0].Unknown
	if tags, _ := e0["x-tags"].([]any); len(tags) != 2 {
		t.Fatalf("array extension lost: %#v", e0["x-tags"])
	}
	meta, _ := e0["x-meta"].(map[string]any)
	if nested, _ := meta["nested"].(map[string]any); nested["deep"] != true {
		t.Fatalf("nested object extension lost: %#v", meta)
	}
	if meta["empty"] != nil {
		t.Fatalf("null extension must decode to nil, got %#v", meta["empty"])
	}
	e1 := m.Entries[1].Unknown
	if got := e1["x-offset"].(json.Number).String(); got != "123456789012345678901234567890" {
		t.Fatalf("chunk big integer lost digits: %s", got)
	}
	objLevel := m.Unknown["x-object-level"].(map[string]any)
	if objLevel["policy"] != "keep" {
		t.Fatalf("object-level extension lost: %#v", m.Unknown)
	}
	if got := m.Unknown["x-global-big"].(json.Number).String(); got != "1234567890123456789012345678901234567890" {
		t.Fatalf("object big integer lost digits: %s", got)
	}

	// Read-modify-write: touch only a built-in field and a known extension,
	// everything else must come back identical.
	m.Name = "rich-renamed"
	m.Entries[0].Unknown["x-tags"] = []any{"hot", "audit", "extra"}
	out, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if re.Name != "rich-renamed" {
		t.Fatalf("built-in field change lost")
	}

	// Compare the full JSON trees: every untouched extension must match.
	want := parseTree(t, []byte(extV2))
	got := parseTree(t, out)
	wantMap, gotMap := want.(map[string]any), got.(map[string]any)
	if !reflect.DeepEqual(wantMap["x-object-level"], gotMap["x-object-level"]) {
		t.Fatalf("object-level extension changed:\nwant=%v\ngot =%v",
			wantMap["x-object-level"], gotMap["x-object-level"])
	}
	wantEntries := wantMap["entries"].([]any)
	gotEntries := gotMap["entries"].([]any)
	// entry 0: only x-tags changed; everything else identical.
	we0, ge0 := wantEntries[0].(map[string]any), gotEntries[0].(map[string]any)
	for k, v := range we0 {
		if k == "x-tags" {
			continue
		}
		if !reflect.DeepEqual(v, ge0[k]) {
			t.Fatalf("entry0 extension %q changed:\nwant=%v\ngot =%v", k, v, ge0[k])
		}
	}
	// entry 1: nothing changed at all.
	if !reflect.DeepEqual(wantEntries[1], gotEntries[1]) {
		t.Fatalf("untouched entry changed:\nwant=%v\ngot =%v", wantEntries[1], gotEntries[1])
	}
	t.Logf("entries=%d chunk-extension-shapes=nested/array/bool/null/bigint basis=verbatim-preservation", len(m.Entries))
}

// TestLargeIntegersExactAcrossRewrite: integers beyond 2^53 (and below
// -2^53), at both object and chunk level, keep every digit through a pure
// round trip (no modification at all).
func TestLargeIntegersExactAcrossRewrite(t *testing.T) {
	cases := []string{
		`9007199254740993`,                       // 2^53 + 1
		`-9007199254740993`,                      // -(2^53 + 1)
		`12345678901234567890123456789012345678`, // 38 digits
		`0`,
	}
	for _, digits := range cases {
		in := `{"version":2,"name":"big","entries":[{"id":"aa00000000000000000000000000000000000000000000000000000000000001","size":1,"x-n":` +
			digits + `}],"x-top-n":` + digits + `}`
		m, err := Unmarshal([]byte(in))
		if err != nil {
			t.Fatalf("digits=%s unmarshal: %v", digits, err)
		}
		out, err := Marshal(m)
		if err != nil {
			t.Fatalf("digits=%s marshal: %v", digits, err)
		}
		re, err := Unmarshal(out)
		if err != nil {
			t.Fatalf("digits=%s re-unmarshal: %v", digits, err)
		}
		if got := re.Entries[0].Unknown["x-n"].(json.Number).String(); got != digits {
			t.Fatalf("chunk big int: want %s got %s", digits, got)
		}
		if got := re.Unknown["x-top-n"].(json.Number).String(); got != digits {
			t.Fatalf("object big int: want %s got %s", digits, got)
		}
		if !strings.Contains(string(out), digits) {
			t.Fatalf("serialized bytes lost exact digits %s: %s", digits, out)
		}
	}
	t.Logf("basis=json.Number-digit-exact no-float64-approximation")
}

// TestSerializationAndContentIDReproducible: the same manifest marshals to
// identical bytes repeatedly; unmarshal-then-marshal without changes
// reproduces the bytes and therefore the content ID.
func TestSerializationAndContentIDReproducible(t *testing.T) {
	m, err := Unmarshal([]byte(extV2))
	if err != nil {
		t.Fatal(err)
	}
	a, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("marshal non-deterministic:\n%s\n%s", a, b)
	}
	// Re-read the bytes and serialize again: must be byte-identical.
	re, err := Unmarshal(a)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Marshal(re)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, c) {
		t.Fatalf("read-then-write changed bytes:\nbefore=%s\nafter =%s", a, c)
	}
	id1, err := ContentID(m)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := ContentID(re)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("content id diverged: %s vs %s", id1, id2)
	}
	t.Logf("manifest-id=%s basis=stable-key-order+verbatim-numbers", id1)
}

// TestUpgradeV1ToV2KeepsEverything: supported old data (v1, including
// legacy missing-version) reads, upgrades to the current version, and
// loses no entries or extensions; upgrading twice is byte-identical.
func TestUpgradeV1ToV2KeepsEverything(t *testing.T) {
	// A v1 manifest that already happens to carry chunk-level extensions
	// written by some other tool: they must be kept too.
	v1 := `{"version":1,"name":"old","entries":[
		{"id":"aa00000000000000000000000000000000000000000000000000000000000001","size":4,"x-tool":{"a":[1,true,null]}}
	],"x-top":"keep-me","x-big":123456789012345678901234567890}`
	m, err := Unmarshal([]byte(v1))
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 {
		t.Fatalf("want v1, got %d", m.Version)
	}
	up := Upgrade(m)
	if up.Version != CurrentVersion {
		t.Fatalf("want upgraded v%d, got %d", CurrentVersion, up.Version)
	}
	out1, err := Marshal(up)
	if err != nil {
		t.Fatal(err)
	}
	re, err := Unmarshal(out1)
	if err != nil {
		t.Fatal(err)
	}
	if re.Unknown["x-top"] != "keep-me" {
		t.Fatalf("top-level extension lost in upgrade")
	}
	if got := re.Unknown["x-big"].(json.Number).String(); got != "123456789012345678901234567890" {
		t.Fatalf("big int lost in upgrade: %s", got)
	}
	chunkExt := re.Entries[0].Unknown["x-tool"].(map[string]any)
	if list, _ := chunkExt["a"].([]any); len(list) != 3 {
		t.Fatalf("chunk-level extension lost in upgrade: %#v", chunkExt)
	}
	// Idempotent: upgrading again yields identical bytes.
	out2, err := Marshal(Upgrade(re))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out1, out2) {
		t.Fatalf("repeated upgrade not identical:\n%s\n%s", out1, out2)
	}
	t.Logf("upgrade v1->v2 entries=%d basis=idempotent-lossless-upgrade", len(re.Entries))
}

// TestFutureVersionStillRejectedWithExtensions: new versions are rejected
// outright even when extensions look familiar — no partial understanding.
func TestFutureVersionStillRejectedWithExtensions(t *testing.T) {
	future := `{"version":99,"name":"x","entries":[{"id":"aa00000000000000000000000000000000000000000000000000000000000001","size":1,"x-known-looking":true}]}`
	m, err := Unmarshal([]byte(future))
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got m=%+v err=%v", m, err)
	}
	if m != nil {
		t.Fatalf("future manifest must be nil")
	}
	t.Logf("reason=%v basis=version>MaxSupportedVersion-before-any-field-use", err)
}
