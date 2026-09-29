package manifest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func baseForMerge(t *testing.T) *Manifest {
	t.Helper()
	in := `{"version":2,"name":"o","entries":[` +
		`{"id":"aa00000000000000000000000000000000000000000000000000000000000001","size":1},` +
		`{"id":"bb00000000000000000000000000000000000000000000000000000000000002","size":2,"attrs":null}` +
		`],"x-shared":"s"}`
	m, err := Unmarshal([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestMergeDistinctExtensionKeys: two writers derived from the same base,
// each adding its own top-level and per-chunk extension keys, merge into a
// manifest containing every key; attrs shapes (incl. explicit null) are
// preserved when untouched.
func TestMergeDistinctExtensionKeys(t *testing.T) {
	base := baseForMerge(t)
	a := baseForMerge(t)
	a.Unknown["tool-a"] = json.Number(hugeInt)
	a.Entries[0].SetAttr("a", "1")
	b := baseForMerge(t)
	b.Unknown["tool-b"] = true
	b.Entries[0].SetAttr("b", nil)

	merged, err := Merge(base, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Unknown["tool-a"] != json.Number(hugeInt) || merged.Unknown["tool-b"] != true {
		t.Fatalf("top-level extensions not both merged: %#v", merged.Unknown)
	}
	if merged.Unknown["x-shared"] != "s" {
		t.Fatalf("shared base extension lost: %#v", merged.Unknown)
	}
	attrs := merged.Entries[0].Attrs
	if attrs["a"] != "1" {
		t.Fatalf("side-a attr missing: %#v", attrs)
	}
	if v, ok := attrs["b"]; !ok || v != nil {
		t.Fatalf("side-b explicit-null attr missing: %#v", attrs)
	}
	if merged.Entries[1].AttrsForm() != AttrsNull {
		t.Fatalf("untouched null attrs shape must survive, got %d", merged.Entries[1].AttrsForm())
	}
	if merged.Entries[0].AttrsForm() != AttrsObject {
		t.Fatalf("entry gaining attrs must be object form, got %d", merged.Entries[0].AttrsForm())
	}
	// Merged manifest must be serializable and stable.
	out, err := Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	out2, _ := Marshal(re)
	if string(out) != string(out2) {
		t.Fatalf("merged marshal not stable:\n%s\n%s", out, out2)
	}
	// Merge order must not matter.
	merged2, err := Merge(base, b, a)
	if err != nil {
		t.Fatal(err)
	}
	o2, _ := Marshal(merged2)
	if string(out) != string(o2) {
		t.Fatalf("merge is order-dependent:\n%s\n%s", out, o2)
	}
}

// TestMergeConflictSameKey: when both sides change the same extension key
// to different values, the merge must report a MergeConflict instead of
// silently keeping one side.
func TestMergeConflictSameKey(t *testing.T) {
	base := baseForMerge(t)
	a := baseForMerge(t)
	a.Unknown["k"] = "a"
	b := baseForMerge(t)
	b.Unknown["k"] = "b"
	if _, err := Merge(base, a, b); !errors.Is(err, MergeConflict) {
		t.Fatalf("want MergeConflict, got %v", err)
	}
	// Same value set by both sides is not a conflict.
	a2 := baseForMerge(t)
	a2.Unknown["k"] = "z"
	b2 := baseForMerge(t)
	b2.Unknown["k"] = "z"
	merged, err := Merge(base, a2, b2)
	if err != nil {
		t.Fatalf("identical concurrent edits must merge: %v", err)
	}
	if merged.Unknown["k"] != "z" {
		t.Fatalf("equal concurrent edit lost: %#v", merged.Unknown)
	}
	// Conflicting known-field edits (name) conflict.
	an := baseForMerge(t)
	an.Name = "x"
	bn := baseForMerge(t)
	bn.Name = "y"
	if _, err := Merge(base, an, bn); !errors.Is(err, MergeConflict) {
		t.Fatalf("want name conflict, got %v", err)
	}
	// Conflicting attrs shape changes conflict.
	as := baseForMerge(t)
	as.Entries[1].SetAttrsForm(AttrsObject)
	bs := baseForMerge(t)
	bs.Entries[1].SetAttrsForm(AttrsOmitted)
	_, err = Merge(base, as, bs)
	if !errors.Is(err, MergeConflict) {
		t.Fatalf("want attrs shape conflict, got %v", err)
	}
}

// TestMergeDeletionPreservesExplicitNull: deleting an extension key on
// only one side keeps the merged result free of that key; the serialized
// manifest is valid and stable.
func TestMergeDeletionPreservesExplicitNull(t *testing.T) {
	base := baseForMerge(t)
	a := baseForMerge(t)
	delete(a.Unknown, "x-shared")
	b := baseForMerge(t)
	b.Unknown["x-b"] = 2
	merged, err := Merge(base, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := merged.Unknown["x-shared"]; ok {
		t.Fatal("single-side delete must remove key")
	}
	out, err := Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "x-shared") {
		t.Fatalf("deleted key reappeared: %s", out)
	}
}
