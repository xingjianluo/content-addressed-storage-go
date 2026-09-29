// Package manifest defines versioned content manifests. A manifest lists
// the ordered chunks of an object together with their expected IDs and
// sizes, and supports forward/backward compatible evolution.
//
// Wire format (JSON, one object):
//
//	{
//	  "version": 2,
//	  "name": "example.txt",
//	  "entries": [
//	    {
//	      "id": "<sha256-hex>",
//	      "size": 123,
//	      "attrs": { ... per-chunk custom extensions ... }
//	    }
//	  ],
//	  ... unknown fields are preserved verbatim ...
//	}
//
// Extension data ("attrs" on entries, arbitrary unknown fields on the
// manifest or on entries) may contain nested objects, arrays, booleans,
// null or arbitrarily large integers. JSON numbers are decoded with
// json.Decoder.UseNumber, so integers larger than float64 precision are
// kept digit-for-digit; *big.Int and integer values supplied by callers
// are serialized as JSON numbers without precision loss.
//
// The per-entry "attrs" field keeps its three wire forms distinct across
// read-modify-write: absent (no key), explicit null ("attrs": null) and
// object (including the empty object {}). An unmodified form round-trips
// byte-for-byte, so the serialized bytes and the content ID of a manifest
// are reproducible. See Entry.AttrsNull.
//
// Concurrent read-modify-write by multiple tools is reconciled with
// MergeExtensions: confirmed extension keys from a losing writer's
// manifest are unioned into the winner's (destination wins conflicts),
// so no confirmed extension is silently dropped.
//
// Compatibility rules:
//   - Data written by older versions (MinVersion <= Version < CurrentVersion)
//     is always readable. A missing "version" field is treated as version 1.
//   - A manifest whose Version is greater than MaxSupportedVersion is
//     rejected with ErrUnsupportedVersion; such data is never silently
//     rewritten or partially understood.
//   - Unknown fields encountered while reading (both at the top level and
//     inside entries) are retained in Unknown and re-emitted by Marshal, so
//     a read-modify-write cycle never drops information produced by a
//     newer/other writer.
//   - Malformed JSON, wrong field types or invalid entries are reported as
//     ErrInvalidManifest with a wrapped reason.
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

// Version constants.
const (
	// MinVersion is the oldest data version this build can read.
	MinVersion = 1
	// CurrentVersion is the schema version produced by this build.
	CurrentVersion = 2
	// MaxSupportedVersion is the newest data version this build understands.
	MaxSupportedVersion = 2
)

// Entry references one chunk of an object in order.
type Entry struct {
	ID   chunk.ID
	Size int
	// Attrs holds per-chunk custom extension information. Values may be
	// nested objects/arrays, strings, booleans, nil, json.Number or
	// *big.Int. It is preserved verbatim (including large-integer digits)
	// across read-modify-write cycles.
	//
	// The three wire forms are kept distinct:
	//   - Attrs == nil && AttrsNull == false: the "attrs" key is absent;
	//   - Attrs == nil && AttrsNull == true:  "attrs" is an explicit null;
	//   - Attrs != nil (possibly empty):      "attrs" is a JSON object.
	Attrs map[string]any
	// AttrsNull records that the entry carried an explicit "attrs": null.
	// It only takes effect while Attrs is nil; once Attrs is set to a
	// non-nil map the object form wins and the null marker is ignored.
	AttrsNull bool
	// Unknown preserves entry-level fields this build does not know about.
	Unknown map[string]any
}

// Manifest is the versioned content list. Unknown holds fields not part of
// the built-in schema; they are preserved across marshal/unmarshal.
type Manifest struct {
	Version int
	Name    string
	Entries []Entry
	// Unknown preserves schema fields this build does not know about.
	Unknown map[string]any
}

// Errors with stable, distinguishable failure reasons.
var (
	ErrInvalidManifest    = errors.New("manifest: invalid")
	ErrUnsupportedVersion = errors.New("manifest: unsupported version")
)

// knownFields are the top-level fields defined by the built-in schema.
var knownFields = map[string]struct{}{
	"version": {},
	"name":    {},
	"entries": {},
}

// knownEntryFields are the per-entry fields defined by the built-in schema.
var knownEntryFields = map[string]struct{}{
	"id":    {},
	"size":  {},
	"attrs": {},
}

// Validate checks structural and version rules.
func Validate(m *Manifest) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	v := m.Version
	if v == 0 {
		v = 1
	}
	if v < MinVersion {
		return fmt.Errorf("%w: version %d older than minimum %d", ErrInvalidManifest, v, MinVersion)
	}
	if v > MaxSupportedVersion {
		return fmt.Errorf("%w: version %d > max supported %d", ErrUnsupportedVersion, v, MaxSupportedVersion)
	}
	for i, e := range m.Entries {
		if len(e.ID) == 0 {
			return fmt.Errorf("%w: entries[%d]: empty id", ErrInvalidManifest, i)
		}
		if e.Size < 0 {
			return fmt.Errorf("%w: entries[%d]: negative size", ErrInvalidManifest, i)
		}
	}
	return nil
}

// Marshal serializes a manifest deterministically. Known fields and
// preserved unknown fields are emitted into the same JSON object; map
// keys are sorted by encoding/json so output is stable byte-for-byte.
// Large integers (*big.Int or json.Number in extension values) are
// emitted as JSON numbers without precision loss.
func Marshal(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if err := Validate(m); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(m.Unknown)+3)
	for k, v := range m.Unknown {
		if _, known := knownFields[k]; known {
			// A preserved key must never masquerade as a built-in field.
			return nil, fmt.Errorf("%w: unknown field collides with built-in field %q", ErrInvalidManifest, k)
		}
		nv, err := normalizeExt(v)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidManifest, k, err)
		}
		out[k] = nv
	}
	version := m.Version
	if version == 0 {
		version = CurrentVersion
	}
	out["version"] = version
	if m.Name != "" {
		out["name"] = m.Name
	}
	entries := make([]map[string]any, 0, len(m.Entries))
	for i, e := range m.Entries {
		obj := map[string]any{"id": string(e.ID), "size": e.Size}
		switch {
		case e.Attrs != nil:
			attrs := make(map[string]any, len(e.Attrs))
			for k, v := range e.Attrs {
				nv, err := normalizeExt(v)
				if err != nil {
					return nil, fmt.Errorf("%w: entries[%d].attrs.%s: %v", ErrInvalidManifest, i, k, err)
				}
				attrs[k] = nv
			}
			obj["attrs"] = attrs
		case e.AttrsNull:
			// An explicit null round-trips as an explicit null.
			obj["attrs"] = nil
		}
		for k, v := range e.Unknown {
			if _, known := knownEntryFields[k]; known {
				return nil, fmt.Errorf("%w: entries[%d]: unknown field collides with built-in field %q",
					ErrInvalidManifest, i, k)
			}
			nv, err := normalizeExt(v)
			if err != nil {
				return nil, fmt.Errorf("%w: entries[%d].%s: %v", ErrInvalidManifest, i, k, err)
			}
			obj[k] = nv
		}
		entries = append(entries, obj)
	}
	out["entries"] = entries
	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	return data, nil
}

// Unmarshal parses a manifest applying the compatibility rules.
func Unmarshal(data []byte) (*Manifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty input", ErrInvalidManifest)
	}
	raw := map[string]any{}
	dec := json.NewDecoder(bytes.NewReader(data))
	// UseNumber keeps every JSON number as its original digit text instead
	// of a float64, so large integers in extension data survive intact.
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}

	m := &Manifest{Unknown: map[string]any{}}

	if v, ok := raw["version"]; ok {
		f, err := asVersion(v)
		if err != nil {
			return nil, fmt.Errorf("%w: version: %v", ErrInvalidManifest, err)
		}
		m.Version = f
	} else {
		// Missing version: legacy data written before explicit versioning.
		m.Version = 1
	}

	if v, ok := raw["name"]; ok {
		s, err := asString(v)
		if err != nil {
			return nil, fmt.Errorf("%w: name: %v", ErrInvalidManifest, err)
		}
		m.Name = s
	}

	if v, ok := raw["entries"]; ok {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: entries: expected array", ErrInvalidManifest)
		}
		m.Entries = make([]Entry, 0, len(arr))
		for i, item := range arr {
			obj, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: entries[%d]: expected object", ErrInvalidManifest, i)
			}
			idv, present := obj["id"]
			if !present {
				return nil, fmt.Errorf("%w: entries[%d]: missing id", ErrInvalidManifest, i)
			}
			idStr, err := asString(idv)
			if err != nil {
				return nil, fmt.Errorf("%w: entries[%d].id: %v", ErrInvalidManifest, i, err)
			}
			size := 0
			if sv, present := obj["size"]; present {
				size, err = asInt(sv)
				if err != nil {
					return nil, fmt.Errorf("%w: entries[%d].size: %v", ErrInvalidManifest, i, err)
				}
			}
			e := Entry{ID: chunk.ID(idStr), Size: size}
			if av, present := obj["attrs"]; present {
				if av == nil {
					// Explicit null is a distinct, preserved form.
					e.AttrsNull = true
				} else {
					attrs, ok := av.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("%w: entries[%d].attrs: expected object", ErrInvalidManifest, i)
					}
					e.Attrs = attrs
				}
			}
			for k, v := range obj {
				if _, known := knownEntryFields[k]; known {
					continue
				}
				if e.Unknown == nil {
					e.Unknown = map[string]any{}
				}
				e.Unknown[k] = v
			}
			m.Entries = append(m.Entries, e)
		}
	}

	// Preserve every non-built-in field verbatim.
	for k, v := range raw {
		if _, known := knownFields[k]; known {
			continue
		}
		m.Unknown[k] = v
	}

	if err := Validate(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Upgrade converts an older-version manifest in place to the current
// version without dropping entries or unknown fields. It is idempotent.
func Upgrade(m *Manifest) *Manifest {
	if m == nil {
		return nil
	}
	// All supported old versions map forward; unknown fields are kept.
	m.Version = CurrentVersion
	return m
}

// MergeExtensions folds the extension information of src into dst and
// reports whether dst changed. It is the reconciliation step for
// concurrent read-modify-write: two writers that both based their edit on
// the same manifest version each produced a confirmed manifest; merging
// the loser's manifest into the winner's (and re-persisting) guarantees
// neither side's confirmed extensions are dropped.
//
// Rules (deterministic, dst wins on conflict):
//   - Object-level: keys present in src.Unknown but missing in dst.Unknown
//     are copied; keys already in dst are left untouched.
//   - Entry-level: entries are paired by index and merged only when both
//     manifests reference the same chunk ID at that index. Attrs keys are
//     unioned (dst wins); a dst entry without any attrs form adopts src's
//     form (object or explicit null). Entry.Unknown keys are unioned the
//     same way.
//
// Known fields (Version, Name, ID, Size) are never touched. Merging is
// idempotent: merging the same src twice changes nothing the second time.
func MergeExtensions(dst, src *Manifest) bool {
	if dst == nil || src == nil {
		return false
	}
	changed := false
	for k, v := range src.Unknown {
		if _, ok := dst.Unknown[k]; ok {
			continue
		}
		if dst.Unknown == nil {
			dst.Unknown = map[string]any{}
		}
		dst.Unknown[k] = v
		changed = true
	}
	n := len(dst.Entries)
	if len(src.Entries) < n {
		n = len(src.Entries)
	}
	for i := 0; i < n; i++ {
		d, s := &dst.Entries[i], &src.Entries[i]
		if d.ID != s.ID {
			continue // different content: never mix per-chunk extensions
		}
		switch {
		case d.Attrs != nil && s.Attrs != nil:
			for k, v := range s.Attrs {
				if _, ok := d.Attrs[k]; !ok {
					d.Attrs[k] = v
					changed = true
				}
			}
		case d.Attrs == nil && !d.AttrsNull:
			// dst carries no attrs form at all: adopt src's form verbatim.
			if s.Attrs != nil {
				d.Attrs = s.Attrs
				changed = true
			} else if s.AttrsNull {
				d.AttrsNull = true
				changed = true
			}
		}
		for k, v := range s.Unknown {
			if _, ok := d.Unknown[k]; ok {
				continue
			}
			if d.Unknown == nil {
				d.Unknown = map[string]any{}
			}
			d.Unknown[k] = v
			changed = true
		}
	}
	return changed
}

// ContentID returns the stable ID of a manifest's serialized form.
func ContentID(m *Manifest) (chunk.ID, error) {
	data, err := Marshal(m)
	if err != nil {
		return "", err
	}
	return chunk.ComputeID(data), nil
}

// asInt accepts a JSON number that is an integer value representable in int.
func asInt(v any) (int, error) {
	n, err := asNumber(v)
	if err != nil {
		return 0, err
	}
	i, err := n.Int64()
	if err != nil {
		return 0, errors.New("expected integer")
	}
	return int(i), nil
}

// asVersion decodes the "version" field. A non-integer value is invalid;
// an integer larger than int64 is reported as a future version (the value
// is certainly greater than MaxSupportedVersion).
func asVersion(v any) (int, error) {
	if n, ok := v.(json.Number); ok {
		if _, err := n.Int64(); err != nil {
			if _, ok := new(big.Int).SetString(string(n), 10); ok {
				return MaxSupportedVersion + 1, nil
			}
			return 0, errors.New("expected integer")
		}
		return asInt(v)
	}
	return 0, errors.New("expected integer")
}

// asNumber extracts a json.Number from a decoded value, rejecting
// booleans/strings and non-integer float spellings used for integer fields.
func asNumber(v any) (json.Number, error) {
	n, ok := v.(json.Number)
	if !ok {
		return "", errors.New("expected integer")
	}
	if strings.ContainsAny(string(n), ".eE") {
		return "", errors.New("expected integer")
	}
	return n, nil
}

// normalizeExt converts caller-supplied extension values into values
// encoding/json serializes losslessly. *big.Int (and any built-in integer
// kind) are emitted as bare JSON numbers; everything else must already be
// JSON-compatible. Containers are normalized recursively.
func normalizeExt(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, json.Number:
		return v, nil
	case *big.Int:
		if x == nil {
			return nil, nil
		}
		return json.Number(x.String()), nil
	case big.Int:
		return json.Number(x.String()), nil
	case int:
		return json.Number(strconv.Itoa(x)), nil
	case int64:
		return json.Number(strconv.FormatInt(x, 10)), nil
	case int32:
		return json.Number(strconv.FormatInt(int64(x), 10)), nil
	case uint:
		return json.Number(strconv.FormatUint(uint64(x), 10)), nil
	case uint64:
		return json.Number(strconv.FormatUint(x, 10)), nil
	case uint32:
		return json.Number(strconv.FormatUint(uint64(x), 10)), nil
	case float64:
		return x, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			ne, err := normalizeExt(e)
			if err != nil {
				return nil, err
			}
			out[i] = ne
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			ne, err := normalizeExt(e)
			if err != nil {
				return nil, err
			}
			out[k] = ne
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported extension value type %T", v)
	}
}

func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", errors.New("expected string")
	}
	return s, nil
}
