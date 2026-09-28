package manifest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

func sample() *Manifest {
	return &Manifest{
		Version: CurrentVersion,
		Name:    "obj-a",
		Entries: []Entry{
			{ID: chunk.ID("aa00000000000000000000000000000000000000000000000000000000000001"), Size: 3},
			{ID: chunk.ID("bb00000000000000000000000000000000000000000000000000000000000002"), Size: 5},
		},
	}
}

func TestRoundTripStable(t *testing.T) {
	m := sample()
	d1, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	d2, _ := Marshal(m)
	if string(d1) != string(d2) {
		t.Fatalf("marshal not deterministic:\n%s\n%s", d1, d2)
	}
	got, err := Unmarshal(d1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != m.Name || len(got.Entries) != 2 || got.Version != CurrentVersion {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	id1, _ := ContentID(m)
	id2, _ := ContentID(got)
	if id1 != id2 {
		t.Fatalf("content id changed across round trip: %s vs %s", id1, id2)
	}
	t.Logf("manifest name=%s id=%s entries=2 basis=stable-json+sha256", m.Name, id1)
}

func TestLegacyVersionReadable(t *testing.T) {
	// Data with no explicit version field is legacy and must read as v1.
	legacy := `{"name":"old","entries":[{"id":"cc00000000000000000000000000000000000000000000000000000000000003","size":7}]}`
	m, err := Unmarshal([]byte(legacy))
	if err != nil {
		t.Fatalf("legacy manifest rejected: %v", err)
	}
	if m.Version != 1 || m.Name != "old" {
		t.Fatalf("legacy decode wrong: %+v", m)
	}
	t.Logf("legacy-json parsed version=%d name=%s basis=missing-version-defaults-to-1", m.Version, m.Name)
}

func TestUnsupportedFutureVersionRejected(t *testing.T) {
	future := `{"version":99,"entries":[],"futurePolicy":"erase-on-read-would-be-wrong"}`
	m, err := Unmarshal([]byte(future))
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got m=%+v err=%v", m, err)
	}
	if m != nil {
		t.Fatalf("manifest must be nil on unsupported version")
	}
	t.Logf("version=99 reason=%v basis=version>MaxSupportedVersion", err)

	// Re-marshaling current data must never bump into the future rule.
	if _, err := Marshal(sample()); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownFieldsPreservedOnReadModifyWrite(t *testing.T) {
	future := `{"version":1,"name":"x","entries":[],"x-new-policy":"keep-me","x-replicas":5}`
	m, err := Unmarshal([]byte(future))
	if err != nil {
		t.Fatal(err)
	}
	if m.Unknown["x-new-policy"] != "keep-me" {
		t.Fatalf("unknown string field lost: %+v", m.Unknown)
	}
	if m.Unknown["x-replicas"] != json.Number("5") {
		t.Fatalf("unknown number field lost: %+v", m.Unknown)
	}
	// Modify a known field, then re-marshal: unknown fields must survive.
	m.Name = "x-renamed"
	out, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if re.Unknown["x-new-policy"] != "keep-me" || re.Name != "x-renamed" {
		t.Fatalf("read-modify-write dropped info: %s", out)
	}
	if !strings.Contains(string(out), "x-new-policy") || !strings.Contains(string(out), "x-replicas") {
		t.Fatalf("re-marshaled bytes lost unknown fields: %s", out)
	}
	t.Logf("preserved=%v basis=unknown-fields-round-tripped-verbatim", re.Unknown)
}

func TestInvalidManifestsClassified(t *testing.T) {
	cases := map[string]string{
		"malformed json":     `{not-json`,
		"wrong version type": `{"version":"one","entries":[]}`,
		"entry missing id":   `{"version":1,"entries":[{"size":1}]}`,
		"negative size":      `{"version":1,"entries":[{"id":"dd00000000000000000000000000000000000000000000000000000000000004","size":-1}]}`,
	}
	for name, in := range cases {
		if _, err := Unmarshal([]byte(in)); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("case %q: want ErrInvalidManifest, got %v", name, err)
		} else {
			t.Logf("case=%q reason=%v basis=structural-validation", name, err)
		}
	}
}

func TestUpgradeIdempotentAndLossless(t *testing.T) {
	m, _ := Unmarshal([]byte(`{"version":1,"entries":[],"keep":1}`))
	up := Upgrade(m)
	if up.Version != CurrentVersion {
		t.Fatalf("upgrade version=%d", up.Version)
	}
	if up.Unknown["keep"] != json.Number("1") {
		t.Fatalf("upgrade dropped unknown field")
	}
	again := Upgrade(up)
	if again.Version != CurrentVersion {
		t.Fatalf("upgrade not idempotent")
	}
}
