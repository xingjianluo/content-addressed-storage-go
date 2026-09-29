package manifest

import (
	"encoding/json"
	"testing"
)

const (
	mergeID1 = "aa00000000000000000000000000000000000000000000000000000000000001"
	mergeID2 = "bb00000000000000000000000000000000000000000000000000000000000002"
)

func mergeBase() *Manifest {
	return &Manifest{
		Version: CurrentVersion,
		Name:    "shared",
		Entries: []Entry{
			{ID: mergeID1, Size: 3, Attrs: map[string]any{"init": json.Number("1")}},
			{ID: mergeID2, Size: 4, AttrsNull: true},
		},
		Unknown: map[string]any{"init": json.Number("1")},
	}
}

// TestMergeExtensionsUnionsConfirmedEdits: two writers that both edited
// the same base version each keep their confirmed extensions after a
// merge; conflicting keys resolve deterministically to dst; merging is
// idempotent.
func TestMergeExtensionsUnionsConfirmedEdits(t *testing.T) {
	// Writer A's confirmed manifest: adds object-level "a" and per-chunk "a".
	a := mergeBase()
	a.Unknown["a"] = json.Number("10")
	a.Entries[0].Attrs["a"] = json.Number("10")
	a.Entries[0].Unknown = map[string]any{"a-note": "A"}

	// Writer B's confirmed manifest: adds "b" everywhere, and races on "a".
	b := mergeBase()
	b.Unknown["b"] = json.Number("20")
	b.Unknown["a"] = json.Number("99") // conflicting key: dst (A) must win
	b.Entries[0].Attrs["b"] = json.Number("20")
	b.Entries[0].Attrs["a"] = json.Number("99")
	b.Entries[0].Unknown = map[string]any{"b-note": "B"}
	// B replaced the explicit null attrs on entry 1 with a real object.
	b.Entries[1].AttrsNull = false
	b.Entries[1].Attrs = map[string]any{"b": json.Number("20")}

	if !MergeExtensions(a, b) {
		t.Fatal("merge reported no change")
	}
	// Object-level union, dst wins conflicts.
	if a.Unknown["init"] != json.Number("1") ||
		a.Unknown["a"] != json.Number("10") ||
		a.Unknown["b"] != json.Number("20") {
		t.Fatalf("object-level merge wrong: %#v", a.Unknown)
	}
	// Per-chunk union, dst wins conflicts.
	if a.Entries[0].Attrs["a"] != json.Number("10") ||
		a.Entries[0].Attrs["b"] != json.Number("20") ||
		a.Entries[0].Attrs["init"] != json.Number("1") {
		t.Fatalf("chunk attrs merge wrong: %#v", a.Entries[0].Attrs)
	}
	// Entry-level unknown fields unioned.
	if a.Entries[0].Unknown["a-note"] != "A" || a.Entries[0].Unknown["b-note"] != "B" {
		t.Fatalf("entry unknown merge wrong: %#v", a.Entries[0].Unknown)
	}
	// dst's explicit null form is never silently replaced by src's object.
	if a.Entries[1].Attrs != nil || !a.Entries[1].AttrsNull {
		t.Fatalf("explicit null attrs overwritten by merge: %+v", a.Entries[1])
	}

	// Idempotent: merging B again changes nothing.
	if MergeExtensions(a, b) {
		t.Fatal("second merge not idempotent")
	}
	// And the merged manifest still marshals deterministically.
	out1 := mustMarshal(t, a)
	re, err := Unmarshal(out1)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(mustMarshal(t, re)); got != string(out1) {
		t.Fatalf("merged manifest not not stable:\n%s\n%s", out1, got)
	}
	t.Logf("merge union ok basis=dst-wins+idempotent")
}

// TestMergeExtensionsAdoptsMissingForm: a dst entry that carries no attrs
// form at all adopts src's form (object or explicit null); entries with
// different chunk IDs are never mixed.
func TestMergeExtensionsAdoptsMissingForm(t *testing.T) {
	dst := mergeBase()
	dst.Entries[0].Attrs = nil // no form at all on entry 0
	src := mergeBase()
	src.Entries[0].Attrs = map[string]any{"tool": "x"}

	if !MergeExtensions(dst, src) {
		t.Fatal("expected adoption of src attrs form")
	}
	if dst.Entries[0].Attrs["tool"] != "x" || len(dst.Entries[0].Attrs) != 1 {
		t.Fatalf("attrs form not adopted verbatim: %#v", dst.Entries[0].Attrs)
	}

	// Explicit null is adoptable too.
	dst2 := mergeBase()
	dst2.Entries[1].AttrsNull = false
	if !MergeExtensions(dst2, mergeBase()) {
		t.Fatal("expected adoption of null form")
	}
	if !dst2.Entries[1].AttrsNull || dst2.Entries[1].Attrs != nil {
		t.Fatalf("null form not adopted: %+v", dst2.Entries[1])
	}

	// Different chunk IDs at the same index: never mix.
	dst3 := mergeBase()
	src3 := mergeBase()
	src3.Entries[0].ID = "ff0000000000000000000000000000000000000000000000000000000000000f"
	src3.Entries[0].Attrs = map[string]any{"foreign": true}
	before := mustMarshal(t, dst3)
	if MergeExtensions(dst3, src3) {
		t.Fatal("merge mixed entries with different chunk IDs")
	}
	if got := mustMarshal(t, dst3); string(got) != string(before) {
		t.Fatal("dst changed by mismatched-ID merge")
	}
	t.Logf("basis=form-adoption+chunk-id-guard")
}
