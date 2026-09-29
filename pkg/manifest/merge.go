// Three-way merge of manifests. It powers coordinated read-modify-write
// cycles: when two writers start from the same base manifest and each
// attaches its own extensions, Merge produces a manifest containing both
// writers' contributions without either overwriting the other.
package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
)

// canonicalNumber renders integer-like extension numbers (json.Number,
// *big.Int and built-in integer kinds) as canonical decimal text so values
// like json.Number("42") and int64(42) compare equal. Non-integer or
// non-number values yield "" (they are compared structurally elsewhere).
func canonicalNumber(v any) string {
	switch x := v.(type) {
	case json.Number:
		return normalizeNumberText(string(x))
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case *big.Int:
		if x == nil {
			return ""
		}
		return x.String()
	case big.Int:
		return x.String()
	}
	return ""
}

// jsonNumber is an alias-free reference to encoding/json.Number.
func normalizeNumberText(s string) string {
	// Only integer spellings participate; floats stay non-canonical and
	// fall back to structural comparison (decoded JSON rarely has them).
	if s == "" {
		return ""
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return ""
		}
	}
	for len(s) > 1 && s[0] == '0' {
		s = s[1:]
	}
	if neg && s != "0" {
		s = "-" + s
	}
	return s
}

// MergeConflict reports that two sides of a read-modify-write cycle made
// incompatible edits to the same piece of data. Extension-only edits made
// by different writers (distinct extension keys) never conflict.
var MergeConflict = errors.New("manifest: concurrent modification conflict")

// Merge combines two derived manifests (a and b) that were both produced
// from the same base manifest. Extension fields are merged key-by-key; an
// extension key touched by only one side is taken from that side, and a
// key set to different values by both sides is a conflict. Known fields
// (name, entries) changed incompatibly are a conflict. The result is nil
// on conflict. The inputs are not mutated.
func Merge(base, a, b *Manifest) (*Manifest, error) {
	return mergeManifests(base, a, b)
}

func mergeManifests(base, a, b *Manifest) (*Manifest, error) {
	if base == nil || a == nil || b == nil {
		return nil, fmt.Errorf("%w: nil input", ErrInvalidManifest)
	}
	out := &Manifest{Version: CurrentVersion, Unknown: map[string]any{}}
	out.Version = a.Version
	if b.Version > out.Version {
		out.Version = b.Version
	}
	if out.Version > MaxSupportedVersion {
		return nil, fmt.Errorf("%w: version %d", ErrUnsupportedVersion, out.Version)
	}
	if !deepEqual(a.Name, base.Name) && !deepEqual(b.Name, base.Name) && a.Name != b.Name {
		return nil, fmt.Errorf("%w: field %q", MergeConflict, "name")
	}
	switch {
	case !deepEqual(a.Name, base.Name):
		out.Name = a.Name
	case !deepEqual(b.Name, base.Name):
		out.Name = b.Name
	default:
		out.Name = base.Name
	}

	top, err := mergeExtMaps(base.Unknown, a.Unknown, b.Unknown, "")
	if err != nil {
		return nil, err
	}
	out.Unknown = top

	ents, err := mergeEntries(base.Entries, a.Entries, b.Entries)
	if err != nil {
		return nil, err
	}
	out.Entries = ents
	return out, nil
}

func mergeEntries(base, a, b []Entry) ([]Entry, error) {
	// Order/size changes of the known schema are not concurrently editable.
	if len(a) != len(base) || len(b) != len(base) {
		return nil, fmt.Errorf("%w: entry count changed concurrently", MergeConflict)
	}
	for i := range base {
		if a[i].ID != base[i].ID || b[i].ID != base[i].ID {
			return nil, fmt.Errorf("%w: entry order/id changed concurrently", MergeConflict)
		}
	}
	out := make([]Entry, len(base))
	for i := range base {
		be, ae, be2 := base[i], a[i], b[i]
		if ae.Size != be.Size || be2.Size != be.Size {
			if ae.Size != be2.Size {
				return nil, fmt.Errorf("%w: entries[%d].size", MergeConflict, i)
			}
		}
		entry := Entry{ID: be.ID}
		switch {
		case ae.Size != be.Size:
			entry.Size = ae.Size
		case be2.Size != be.Size:
			entry.Size = be2.Size
		default:
			entry.Size = be.Size
		}
		unk, err := mergeExtMaps(be.Unknown, ae.Unknown, be2.Unknown, fmt.Sprintf("entries[%d]", i))
		if err != nil {
			return nil, err
		}
		entry.Unknown = unk
		attrs, form, err := mergeAttrs(be, ae, be2, i)
		if err != nil {
			return nil, err
		}
		entry.Attrs = attrs
		entry.attrsForm = form
		out[i] = entry
	}
	return out, nil
}

// mergeAttrs three-way merges the attrs field of one entry across
// base/a/b, preserving the absent/null/object shape distinction.
func mergeAttrs(be, ae, bb Entry, i int) (map[string]any, AttrsForm, error) {
	bf, af, cf := be.attrsForm, ae.attrsForm, bb.attrsForm
	aChanged := af != bf
	bChanged := cf != bf
	switch {
	case aChanged && bChanged && af != cf:
		return nil, AttrsOmitted, fmt.Errorf("%w: entries[%d].attrs shape", MergeConflict, i)
	}
	form := bf
	switch {
	case aChanged:
		form = af
	case bChanged:
		form = cf
	}
	if form == AttrsNull {
		return nil, AttrsNull, nil
	}
	if form == AttrsOmitted {
		// Shapes unchanged and base omitted: no attrs existed at all.
		return nil, AttrsOmitted, nil
	}
	merged, err := mergeExtMaps(attrOrNil(be, bf), attrOrNil(ae, af), attrOrNil(bb, cf),
		fmt.Sprintf("entries[%d].attrs", i))
	if err != nil {
		return nil, AttrsOmitted, err
	}
	return merged, AttrsObject, nil
}

func attrOrNil(e Entry, f AttrsForm) map[string]any {
	if f == AttrsObject {
		return e.Attrs
	}
	return nil
}

func mergeExtMaps(base, a, b map[string]any, where string) (map[string]any, error) {
	keys := map[string]struct{}{}
	for k := range base {
		keys[k] = struct{}{}
	}
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	out := map[string]any{}
	for k := range keys {
		bv, bok := base[k]
		av, aok := a[k]
		cv, cok := b[k]
		aSet := (aok != bok) || (aok && bok && !deepEqual(av, bv))
		cSet := (cok != bok) || (cok && bok && !deepEqual(cv, bv))
		switch {
		case aSet && cSet && !deepChangeEqual(aok, av, cok, cv):
			return nil, fmt.Errorf("%w: %s.%s", MergeConflict, where, k)
		case aSet:
			if aok {
				out[k] = av
			}
		case cSet:
			if cok {
				out[k] = cv
			}
		case bok:
			out[k] = bv
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// deepChangeEqual compares two possibly-absent extension edits: missing
// on one side but present on the other is unequal; present on both sides
// requires structural equality.
func deepChangeEqual(aok bool, av any, cok bool, cv any) bool {
	if aok != cok {
		return false
	}
	if !aok {
		return true // both deleted
	}
	return deepEqual(av, cv)
}

// deepEqual compares decoded extension values structurally. json.Number
// text is compared verbatim so 1, 1.0 and 1e0 are not treated as equal.
func deepEqual(x, y any) bool {
	switch xv := x.(type) {
	case nil:
		return y == nil
	case string:
		yv, ok := y.(string)
		return ok && xv == yv
	case bool:
		yv, ok := y.(bool)
		return ok && xv == yv
	case map[string]any:
		yv, ok := y.(map[string]any)
		if !ok || len(xv) != len(yv) {
			return false
		}
		for k, v := range xv {
			ov, ok := yv[k]
			if !ok || !deepEqual(v, ov) {
				return false
			}
		}
		return true
	case []any:
		yv, ok := y.([]any)
		if !ok || len(xv) != len(yv) {
			return false
		}
		for i := range xv {
			if !deepEqual(xv[i], yv[i]) {
				return false
			}
		}
		return true
	case json.Number:
		// Integer spellings compare canonically (42 == int64(42)); other
		// spellings must be byte-identical to be equal.
		if yv, ok := y.(json.Number); ok {
			if cx, cy := canonicalNumber(xv), canonicalNumber(yv); cx != "" && cy != "" {
				return cx == cy
			}
			return string(xv) == string(yv)
		}
		return numericEqual(x, y)
	default:
		return numericEqual(x, y)
	}
}

func numericEqual(x, y any) bool {
	// json.Number / *big.Int / built-in numerics compare by canonical
	// decimal text when both are integer-like.
	return canonicalNumber(x) == canonicalNumber(y) && canonicalNumber(x) != ""
}
