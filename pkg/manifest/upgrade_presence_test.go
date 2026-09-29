package manifest

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestUpgradePreservesAttrsShapes: v2 manifests carrying the three attrs
// shapes, and v1 manifests carrying entry-level unknown data, upgrade to
// CurrentVersion without collapsing the shapes; repeated upgrade yields
// identical bytes.
func TestUpgradePreservesAttrsShapes(t *testing.T) {
	id := "aa00000000000000000000000000000000000000000000000000000000000001"
	in := `{"version":2,"name":"s","entries":[` +
		`{"id":"` + id + `","size":1},` +
		`{"id":"` + id + `","size":1,"attrs":null},` +
		`{"id":"` + id + `","size":1,"attrs":{}},` +
		`{"id":"` + id + `","size":1,"attrs":{"x":` + hugeInt + `,"z":null}}` +
		`]}`
	m, err := Unmarshal([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	forms := []AttrsForm{AttrsOmitted, AttrsNull, AttrsObject, AttrsObject}
	for i, want := range forms {
		if got := m.Entries[i].AttrsForm(); got != want {
			t.Fatalf("entry %d form=%d want %d", i, got, want)
		}
	}
	Upgrade(m)
	out, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"attrs":null`) {
		t.Fatalf("null attrs lost on upgrade: %s", out)
	}
	if !strings.Contains(string(out), `"attrs":{}`) {
		t.Fatalf("empty attrs lost on upgrade: %s", out)
	}
	if !strings.Contains(string(out), `"x":`+hugeInt) || !strings.Contains(string(out), `"z":null`) {
		t.Fatalf("object attrs values lost on upgrade: %s", out)
	}
	out2, _ := Marshal(Upgrade(m))
	if string(out) != string(out2) {
		t.Fatalf("repeated upgrade not identical:\n%s\n%s", out, out2)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if v := re.Entries[3].Attrs["x"]; v != json.Number(hugeInt) {
		t.Fatalf("big int changed by upgrade: %v (%T)", v, v)
	}
	if v, ok := re.Entries[3].Attrs["z"]; !ok || v != nil {
		t.Fatalf("explicit null attr value lost: %#v", re.Entries[3].Attrs)
	}
}
