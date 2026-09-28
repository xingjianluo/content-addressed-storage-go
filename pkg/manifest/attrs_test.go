package manifest

import (
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
)

const hugeInt = "1234567890123456789012345678901234567890123456789012345678901234567890"

// v2 manifest with per-chunk attrs covering every JSON shape, including
// integers far beyond float64 precision.
const attrsJSON = `{"version":2,"name":"rich","entries":[` +
	`{"id":"aa00000000000000000000000000000000000000000000000000000000000001","size":3,` +
	`"attrs":{"tool-a":{"offset":"0010","n":` + hugeInt + `,"tags":["x",true,null,42,-7],` +
	`"nested":{"flag":false,"bignum":` + hugeInt + `}}}},` +
	`{"id":"bb00000000000000000000000000000000000000000000000000000000000002","size":5,` +
	`"attrs":{},"x-entry-future":[1,{"keep":` + hugeInt + `}]}],` +
	`"x-object-policy":{"a":` + hugeInt + `},"x-note":"keep"}`

// TestChunkAttrsLosslessRoundTrip: per-chunk extension data (nested
// objects, arrays, bool, null, very large integers) must survive
// unmarshal -> modify a known field -> marshal -> unmarshal byte-value
// intact. Unknown entry-level fields are preserved the same way.
func TestChunkAttrsLosslessRoundTrip(t *testing.T) {
	m, err := Unmarshal([]byte(attrsJSON))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(m.Entries))
	}
	a := m.Entries[0].Attrs
	if a == nil {
		t.Fatal("attrs[0] lost")
	}
	tool, _ := a["tool-a"].(map[string]any)
	if tool == nil || tool["offset"] != "0010" {
		t.Fatalf("nested attr lost: %#v", a)
	}
	tags, _ := tool["tags"].([]any)
	if len(tags) != 5 || tags[0] != "x" || tags[1] != true || tags[2] != nil ||
		tags[3] != json.Number("42") || tags[4] != json.Number("-7") {
		t.Fatalf("array attr lost/mangled: %#v", tags)
	}
	if got := tool["n"]; got != json.Number(hugeInt) {
		t.Fatalf("huge int lost precision: got %v (%T)", got, got)
	}
	nested, _ := tool["nested"].(map[string]any)
	if nested["flag"] != false || nested["bignum"] != json.Number(hugeInt) {
		t.Fatalf("nested object mangled: %#v", nested)
	}
	// Explicit empty attrs object must remain present and non-nil.
	if m.Entries[1].Attrs == nil {
		t.Fatal("explicit empty attrs {} became nil")
	}
	// Entry-level unknown field preserved, including its large integer.
	fut, _ := m.Entries[1].Unknown["x-entry-future"].([]any)
	if len(fut) != 2 {
		t.Fatalf("entry unknown lost: %#v", m.Entries[1].Unknown)
	}
	inner, _ := fut[1].(map[string]any)
	if inner["keep"] != json.Number(hugeInt) {
		t.Fatalf("entry unknown huge int mangled: %#v", inner)
	}
	if m.Unknown["x-note"] != "keep" {
		t.Fatalf("object-level unknown lost: %#v", m.Unknown)
	}

	// Read-modify-write: touch a known field; everything else persists.
	m.Name = "rich-renamed"
	m.Entries[0].Size = 99
	out, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if re.Name != "rich-renamed" || re.Entries[0].Size != 99 {
		t.Fatalf("known field edit not applied: %+v", re)
	}
	if string(out) != string(mustMarshal(t, re)) {
		t.Fatalf("second marshal not stable:\n%s\n%s", out, mustMarshal(t, re))
	}
	tool2, _ := re.Entries[0].Attrs["tool-a"].(map[string]any)
	if tool2["n"] != json.Number(hugeInt) {
		t.Fatalf("huge int altered by read-modify-write: %v", tool2["n"])
	}
	tags2, _ := tool2["tags"].([]any)
	if len(tags2) != 5 || tags2[2] != nil || tags2[3] != json.Number("42") {
		t.Fatalf("attrs altered by read-modify-write: %#v", tags2)
	}
	if re.Unknown["x-note"] != "keep" {
		t.Fatalf("object unknown dropped after RMW: %#v", re.Unknown)
	}
	if re.Entries[1].Attrs == nil {
		t.Fatal("empty attrs dropped after RMW")
	}
	if !strings.Contains(string(out), hugeInt) {
		t.Fatalf("serialized bytes lost huge int digits: %s", out)
	}
	t.Logf("attrs-round-trip ok bytes=%d basis=UseNumber+verbatim-extension-preservation", len(out))
}

// TestBigIntFromAPISerializedDigitForDigit: extension values supplied by
// callers as *big.Int (or built-in ints) must be serialized as bare JSON
// numbers with every digit intact, not as strings or approximations.
func TestBigIntFromAPISerializedDigitForDigit(t *testing.T) {
	v, _ := new(big.Int).SetString(hugeInt, 10)
	m := &Manifest{
		Version: CurrentVersion,
		Name:    "api",
		Entries: []Entry{{
			ID:    "aa00000000000000000000000000000000000000000000000000000000000001",
			Size:  1,
			Attrs: map[string]any{"big": v, "i64": int64(42), "u": uint64(7)},
		}},
	}
	out, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	want := `"big":` + hugeInt
	if !strings.Contains(string(out), want) {
		t.Fatalf("*big.Int not emitted as bare number: %s", out)
	}
	if strings.Contains(string(out), `"\u`) || strings.Contains(string(out), `"`+hugeInt+`"`) {
		t.Fatalf("big int quoted as string: %s", out)
	}
	re, err := Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := new(big.Int).SetString(string(re.Entries[0].Attrs["big"].(json.Number)), 10)
	if got.Cmp(v) != 0 {
		t.Fatalf("big int mismatch: %s vs %s", got, v)
	}
	if re.Entries[0].Attrs["i64"] != json.Number("42") ||
		re.Entries[0].Attrs["u"] != json.Number("7") {
		t.Fatalf("integer attrs mangled: %#v", re.Entries[0].Attrs)
	}

	// Determinism: identical manifests -> identical bytes and content ID.
	out2, _ := Marshal(m)
	if string(out) != string(out2) {
		t.Fatal("marshal non-deterministic")
	}
	id1, err := ContentID(m)
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := ContentID(re)
	if id1 != id2 {
		t.Fatalf("content id changed across RMW: %s vs %s", id1, id2)
	}
	t.Logf("bigint=%s id=%s basis=json.Number-digit-for-digit", hugeInt, id1)
}

// TestUpgradeV1ToV2LosslessAndIdempotent: supported old-version data
// (no attrs; legacy shape) upgrades to the current version without
// dropping anything; repeating the upgrade gives the same bytes.
func TestUpgradeV1ToV2LosslessAndIdempotent(t *testing.T) {
	v1 := `{"version":1,"name":"old","entries":[` +
		`{"id":"cc00000000000000000000000000000000000000000000000000000000000003","size":7,` +
		`"x-old-attr":` + hugeInt + `}],"x-old-top":[true,null]}`
	m, err := Unmarshal([]byte(v1))
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 {
		t.Fatalf("want old version 1, got %d", m.Version)
	}
	up := Upgrade(m)
	if up.Version != CurrentVersion {
		t.Fatalf("upgrade version=%d", up.Version)
	}
	out, err := Marshal(up)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"version":2`) {
		t.Fatalf("version not bumped: %s", out)
	}
	if !strings.Contains(string(out), hugeInt) || !strings.Contains(string(out), "x-old-top") {
		t.Fatalf("upgrade dropped extension data: %s", out)
	}
	again := Upgrade(up)
	out2, err := Marshal(again)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(out2) {
		t.Fatalf("repeated upgrade changed bytes:\n%s\n%s", out, out2)
	}
	// Legacy data with no version field upgrades identically.
	legacy, err := Unmarshal([]byte(`{"entries":[],"keep":"me"}`))
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Version != 1 {
		t.Fatalf("missing version must read as 1, got %d", legacy.Version)
	}
	lo, err := Marshal(Upgrade(legacy))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(lo), `"version":2`) || !strings.Contains(string(lo), `"keep":"me"`) {
		t.Fatalf("legacy upgrade lost data: %s", lo)
	}
	t.Logf("v1->v2 ok basis=upgrade-preserves-entries+attrs+unknown")
}

// TestAttrsRejectionRules: malformed extension shapes stay classified as
// ErrInvalidManifest; future versions still fail distinctly, even with
// attrs present.
func TestAttrsRejectionRules(t *testing.T) {
	bad := []string{
		`{"version":2,"entries":[{"id":"aa00000000000000000000000000000000000000000000000000000000000001","attrs":[1]}]}`,
		`{"version":2,"entries":[{"id":"aa00000000000000000000000000000000000000000000000000000000000001","attrs":"no"}]}`,
	}
	for _, in := range bad {
		if _, err := Unmarshal([]byte(in)); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("want ErrInvalidManifest for %s, got %v", in, err)
		}
	}
	future := `{"version":3,"entries":[{"id":"aa00000000000000000000000000000000000000000000000000000000000001","attrs":{"x":1}}]}`
	m, err := Unmarshal([]byte(future))
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got m=%v err=%v", m, err)
	}
	// An unsupported type supplied through the API is rejected, not mangled.
	_, err = Marshal(&Manifest{Version: CurrentVersion, Entries: []Entry{{
		ID:    "aa00000000000000000000000000000000000000000000000000000000000001",
		Attrs: map[string]any{"bad": make(chan int)},
	}}})
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("want ErrInvalidManifest for bad attr type, got %v", err)
	}
	t.Logf("basis=attrs-type-validation+version>MaxSupportedVersion-rejected")
}

func mustMarshal(t *testing.T, m *Manifest) []byte {
	t.Helper()
	b, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
