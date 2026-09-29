package manifest

import (
	"errors"
	"strings"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

// TestAttrsPresenceShapesRoundTrip: the three legal on-wire shapes of the
// per-entry attrs field (absent, explicit null, empty object) must stay
// distinct through unmarshal -> touch an unrelated known field -> marshal,
// and the re-serialized bytes must be stable across repeated marshaling
// (hence the manifest content ID is reproducible).
func TestAttrsPresenceShapesRoundTrip(t *testing.T) {
	id := "aa00000000000000000000000000000000000000000000000000000000000001"
	cases := map[string]string{
		"absent": `{"version":2,"name":"s","entries":[{"id":"` + id + `","size":1}]}`,
		"null":   `{"version":2,"name":"s","entries":[{"id":"` + id + `","size":1,"attrs":null}]}`,
		"empty":  `{"version":2,"name":"s","entries":[{"id":"` + id + `","size":1,"attrs":{}}]}`,
	}
	wantForm := map[string]AttrsForm{
		"absent": AttrsOmitted,
		"null":   AttrsNull,
		"empty":  AttrsObject,
	}
	for name, in := range cases {
		m, err := Unmarshal([]byte(in))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		e := m.Entries[0]
		if got := e.AttrsForm(); got != wantForm[name] {
			t.Fatalf("%s: attrs form=%d want %d", name, got, wantForm[name])
		}
		if name == "empty" && e.Attrs == nil {
			t.Fatalf("%s: empty object decoded as nil", name)
		}
		// Read-modify-write touching only a known field must not reshape attrs.
		m.Name = "s-renamed"
		out, err := Marshal(m)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		re, err := Unmarshal(out)
		if err != nil {
			t.Fatalf("%s: remarshal: %v", name, err)
		}
		if got := re.Entries[0].AttrsForm(); got != wantForm[name] {
			t.Fatalf("%s: attrs form after RMW=%d want %d; bytes=%s", name, got, wantForm[name], out)
		}
		// Serialization must be byte-stable on repeat.
		out2, err := Marshal(re)
		if err != nil {
			t.Fatalf("%s: second marshal: %v", name, err)
		}
		if string(out) != string(out2) {
			t.Fatalf("%s: non-stable bytes:\n%s\n%s", name, out, out2)
		}
		// Content ID reproducible across the cycle.
		id1, err := ContentID(m)
		if err != nil {
			t.Fatal(err)
		}
		id2, err := ContentID(re)
		if err != nil {
			t.Fatal(err)
		}
		if id1 != id2 {
			t.Fatalf("%s: content id changed: %s vs %s", name, id1, id2)
		}
	}
}

// TestAttrsNullValueInsideObjectPreserved: an explicit null *value* of an
// extension key (e.g. produced by another tool) must not be dropped by a
// read-modify-write cycle, and its manifest content ID stays reproducible.
func TestAttrsNullValueInsideObjectPreserved(t *testing.T) {
	id := "aa00000000000000000000000000000000000000000000000000000000000001"
	in := `{"version":2,"name":"n","entries":[{"id":"` + id + `","size":2,` +
		`"attrs":{"tool-x":null,"keep":"v"}}]}`
	m, err := Unmarshal([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := m.Entries[0].Attrs["tool-x"]; !ok || v != nil {
		t.Fatalf("explicit null attr value lost: %#v", m.Entries[0].Attrs)
	}
	// Touch only a known field; the null value must survive verbatim.
	m.Name = "n2"
	out, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"tool-x":null`) {
		t.Fatalf("null attr value not re-emitted: %s", out)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := re.Entries[0].Attrs["tool-x"]; !ok || v != nil {
		t.Fatalf("null attr value did not survive RMW: %#v", re.Entries[0].Attrs)
	}
	c1, _ := ContentID(m)
	c2, _ := ContentID(re)
	if c1 != c2 {
		t.Fatalf("content id not reproducible: %s vs %s", c1, c2)
	}
}

// TestAttrsFormTransitions: the exported form controls and SetAttr behave
// as documented for callers building manifests through the API.
func TestAttrsFormTransitions(t *testing.T) {
	id := chunkIDForTest()
	// Omitted -> stays omitted.
	e := Entry{ID: id, Size: 1}
	if e.AttrsForm() != AttrsOmitted {
		t.Fatal("default form must be omitted")
	}
	// Explicit null round-trips.
	e.SetAttrsForm(AttrsNull)
	b, err := Marshal(&Manifest{Version: CurrentVersion, Entries: []Entry{e}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"attrs":null`) {
		t.Fatalf("null form not emitted: %s", b)
	}
	// Null form with keys is rejected, not silently flattened.
	e.Attrs = map[string]any{"x": 1}
	if _, err := Marshal(&Manifest{Version: CurrentVersion, Entries: []Entry{e}}); err == nil {
		t.Fatal("null form with keys must be rejected")
	}
	// SetAttr upgrades null/omitted to an object and preserves keys.
	e2 := Entry{ID: id, Size: 1}
	e2.SetAttrsForm(AttrsNull)
	e2.SetAttr("a", "b")
	if e2.AttrsForm() != AttrsObject {
		t.Fatalf("SetAttr must switch to object form, got %d", e2.AttrsForm())
	}
	// Empty object form emits {}.
	e3 := Entry{ID: id, Size: 1}
	e3.SetAttrsForm(AttrsObject)
	b3, err := Marshal(&Manifest{Version: CurrentVersion, Entries: []Entry{e3}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b3), `"attrs":{}`) {
		t.Fatalf("empty object form not emitted: %s", b3)
	}
}

func chunkIDForTest() chunk.ID {
	return "aa00000000000000000000000000000000000000000000000000000000000001"
}

// TestFutureVersionWithNullAttrsRejected: a newer-than-supported manifest
// is rejected even when its per-chunk attrs use the explicit null shape;
// the program must not partially understand or silently rewrite it.
func TestFutureVersionWithNullAttrsRejected(t *testing.T) {
	id := "aa00000000000000000000000000000000000000000000000000000000000001"
	future := `{"version":3,"name":"s","entries":[` +
		`{"id":"` + id + `","size":1,"attrs":null},` +
		`{"id":"` + id + `","size":1,"attrs":{"future":null}}` +
		`],"x-future":null}`
	m, err := Unmarshal([]byte(future))
	if err == nil || !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got m=%#v err=%v", m, err)
	}
	if m != nil {
		t.Fatal("no manifest may be returned for an unsupported version")
	}
}
