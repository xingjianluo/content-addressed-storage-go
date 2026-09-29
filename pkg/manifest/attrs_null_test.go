package manifest

import (
	"strings"
	"testing"
)

// TestAttrsFormsDistinctAndPreserved: the three wire forms of per-chunk
// extension info — key absent, explicit null, explicit empty object —
// must stay distinct and survive a read-modify-write cycle byte-for-byte,
// so the serialized bytes and the content ID are reproducible.
func TestAttrsFormsDistinctAndPreserved(t *testing.T) {
	const id1 = "aa00000000000000000000000000000000000000000000000000000000000001"
	const id2 = "bb00000000000000000000000000000000000000000000000000000000000002"
	const id3 = "cc00000000000000000000000000000000000000000000000000000000000003"
	in := `{"version":2,"name":"forms","entries":[` +
		`{"id":"` + id1 + `","size":1},` + // attrs absent
		`{"id":"` + id2 + `","size":2,"attrs":null},` + // explicit null
		`{"id":"` + id3 + `","size":3,"attrs":{}}]}` // explicit empty object

	m, err := Unmarshal([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	// The three forms decode to three distinguishable states.
	if m.Entries[0].Attrs != nil || m.Entries[0].AttrsNull {
		t.Fatalf("absent attrs decoded as present: %+v", m.Entries[0])
	}
	if m.Entries[1].Attrs != nil || !m.Entries[1].AttrsNull {
		t.Fatalf("explicit null attrs lost: %+v", m.Entries[1])
	}
	if m.Entries[2].Attrs == nil || len(m.Entries[2].Attrs) != 0 || m.Entries[2].AttrsNull {
		t.Fatalf("empty attrs object lost: %+v", m.Entries[2])
	}

	// Read-modify-write: touch only a known field.
	m.Name = "forms-renamed"
	out := mustMarshal(t, m)

	// Each form is still present in its original shape.
	if strings.Contains(string(out), `"id":"`+id1+`","size":1,"attrs"`) {
		t.Fatalf("absent attrs materialized: %s", out)
	}
	if !strings.Contains(string(out), `"attrs":null,"id":"`+id2+`"`) {
		t.Fatalf("explicit null attrs not re-emitted: %s", out)
	}
	if !strings.Contains(string(out), `"attrs":{},"id":"`+id3+`"`) {
		t.Fatalf("empty attrs object not re-emitted: %s", out)
	}

	// Re-reading the output yields the same states, and a second marshal
	// is byte-identical, so the content ID is reproducible.
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if re.Entries[1].Attrs != nil || !re.Entries[1].AttrsNull {
		t.Fatalf("null attrs form not stable across RMW: %+v", re.Entries[1])
	}
	if re.Entries[2].Attrs == nil || len(re.Entries[2].Attrs) != 0 {
		t.Fatalf("empty attrs form not stable across RMW: %+v", re.Entries[2])
	}
	out2 := mustMarshal(t, re)
	if string(out) != string(out2) {
		t.Fatalf("marshal not reproducible:\n%s\n%s", out, out2)
	}
	idA, err := ContentID(re)
	if err != nil {
		t.Fatal(err)
	}
	re2, err := Unmarshal(out2)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := ContentID(re2)
	if err != nil {
		t.Fatal(err)
	}
	if idA != idB {
		t.Fatalf("content id changed across RMW: %s vs %s", idA, idB)
	}

	// The three forms really are three different serializations (top-level
	// keys are emitted in sorted order, so the canonical form lists
	// "entries" before "version").
	for _, single := range []string{
		`{"entries":[{"attrs":null,"id":"` + id1 + `","size":1}],"version":2}`,
		`{"entries":[{"attrs":{},"id":"` + id1 + `","size":1}],"version":2}`,
	} {
		sm, err := Unmarshal([]byte(single))
		if err != nil {
			t.Fatal(err)
		}
		if got := string(mustMarshal(t, sm)); got != single {
			t.Fatalf("form %s not byte-stable: got %s", single, got)
		}
	}
	absent := `{"entries":[{"id":"` + id1 + `","size":1}],"version":2}`
	am, err := Unmarshal([]byte(absent))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(mustMarshal(t, am)); got != absent {
		t.Fatalf("absent form not byte-stable: got %s", got)
	}
	t.Logf("forms absent/null/empty-object distinct, byte-stable, id=%s basis=tri-state-attrs-preservation", idA)
}

// TestNullAttrsUnmodifiedSurvivesKnownFieldEdit: a manifest written by
// another tool with an explicit null attrs, read and written back after
// editing only a known field, must serialize to bytes identical to the
// original except for the edited field — the null extension is never
// dropped.
func TestNullAttrsUnmodifiedSurvivesKnownFieldEdit(t *testing.T) {
	const id1 = "aa00000000000000000000000000000000000000000000000000000000000001"
	in := `{"entries":[{"attrs":null,"id":"` + id1 + `","size":7}],"name":"orig","version":2}`
	beforeID, err := func() (string, error) {
		m, err := Unmarshal([]byte(in))
		if err != nil {
			return "", err
		}
		cid, err := ContentID(m)
		return string(cid), err
	}()
	if err != nil {
		t.Fatal(err)
	}

	m, err := Unmarshal([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	m.Entries[0].Size = 8 // the only intended change
	out := mustMarshal(t, m)
	want := `{"entries":[{"attrs":null,"id":"` + id1 + `","size":8}],"name":"orig","version":2}`
	if string(out) != want {
		t.Fatalf("null attrs not preserved verbatim:\nwant %s\ngot  %s", want, out)
	}

	// Editing nothing at all reproduces the original bytes and content ID.
	pristine, err := Unmarshal([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(mustMarshal(t, pristine)); got != in {
		t.Fatalf("unmodified RMW changed bytes:\nwant %s\ngot  %s", in, got)
	}
	afterID, err := ContentID(pristine)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterID) != beforeID {
		t.Fatalf("content id changed without edits: %s vs %s", beforeID, afterID)
	}
	t.Logf("null-attrs RMW byte-identical id=%s basis=unmodified-extension-verbatim", afterID)
}
