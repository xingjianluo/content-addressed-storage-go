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

// AttrsForm records the on-wire shape of an entry's "attrs" field. The
// three shapes are semantically different and must round-trip distinctly:
// an absent field is not the same as an explicit JSON null, which in turn
// is not the same as an empty object.
type AttrsForm int

const (
	// AttrsOmitted means the entry has no "attrs" key on the wire.
	AttrsOmitted AttrsForm = iota
	// AttrsNull means the entry carries an explicit "attrs": null.
	AttrsNull
	// AttrsObject means the entry carries an "attrs" object, which may be
	// empty ("attrs": {}) or hold extension keys.
	AttrsObject
)

// Entry references one chunk of an object in order.
type Entry struct {
	ID   chunk.ID
	Size int
	// Attrs holds per-chunk custom extension information. Values may be
	// nested objects/arrays, strings, booleans, nil, json.Number or
	// *big.Int. It is preserved verbatim (including large-integer digits
	// and explicit null values) across read-modify-write cycles.
	Attrs map[string]any
	// attrsForm distinguishes an absent attrs field from an explicit null
	// and from an (possibly empty) object. Use AttrsForm/SetAttrsForm.
	attrsForm AttrsForm
	// Unknown preserves entry-level fields this build does not know about.
	Unknown map[string]any
}

// AttrsForm reports how this entry's attrs field is represented on the
// wire (absent, explicit null, or object).
func (e Entry) AttrsForm() AttrsForm { return e.attrsForm }

// SetAttrsForm sets the on-wire shape of the entry's attrs field.
// Switching to AttrsObject with a nil Attrs map yields "attrs": {};
// switching to AttrsNull requires Attrs to carry no keys (Marshal rejects
// a non-empty map hidden behind an explicit null).
func (e *Entry) SetAttrsForm(f AttrsForm) { e.attrsForm = f }

// SetAttr stores one extension key on the entry and ensures the entry is
// serialized with an attrs object (an absent/null attrs shape becomes an
// object shape the moment a key is added).
func (e *Entry) SetAttr(key string, value any) {
	if e.attrsForm != AttrsObject {
		e.attrsForm = AttrsObject
		e.Attrs = map[string]any{}
	}
	if e.Attrs == nil {
		e.Attrs = map[string]any{}
	}
	e.Attrs[key] = value
}

// Manifest is the versioned content list. Unknown holds fields not part of
// the built-in schema; they are preserved across marshal/unmarshal.
type Manifest struct {
	Version int
	Name    string
	Entries []Entry
	// Unknown preserves schema fields this build does not know about.
	Unknown map[string]any
	// orderHints retains the wire key order of decoded objects so an
	// untouched read-modify-write cycle re-emits identical bytes.
	orderHints *orderHints
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
	// Normalize all extension values up front (validates caller-supplied
	// *big.Int etc.) while preserving map identity, so key-order hints stay
	// attached to the same map instances.
	topUnknown, err := normalizeExtMapInPlace(m.Unknown)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	for k := range topUnknown {
		if _, known := knownFields[k]; known {
			return nil, fmt.Errorf("%w: unknown field collides with built-in field %q", ErrInvalidManifest, k)
		}
	}
	hints := m.orderHints
	if hints == nil {
		hints = newOrderHints()
	}

	version := m.Version
	if version == 0 {
		version = CurrentVersion
	}

	var buf bytes.Buffer
	buf.WriteByte('{')
	writeJSONString(&buf, "version")
	buf.WriteByte(':')
	buf.WriteString(strconv.Itoa(version))
	if m.Name != "" {
		buf.WriteByte(',')
		writeJSONString(&buf, "name")
		buf.WriteByte(':')
		writeJSONString(&buf, m.Name)
	}

	// Entry normalization and validation first (keeps key order hints valid).
	type normEntry struct {
		attrs   map[string]any
		form    AttrsForm
		unknown map[string]any
	}
	normEntries := make([]normEntry, len(m.Entries))
	for i, e := range m.Entries {
		form := e.attrsForm
		if form == AttrsOmitted && e.Attrs != nil {
			// Manually constructed entry holding attrs: treat as object.
			form = AttrsObject
		}
		if form == AttrsNull && len(e.Attrs) > 0 {
			return nil, fmt.Errorf("%w: entries[%d].attrs: explicit null cannot carry keys",
				ErrInvalidManifest, i)
		}
		attrs, err := normalizeExtMapInPlace(e.Attrs)
		if err != nil {
			return nil, fmt.Errorf("%w: entries[%d].attrs: %v", ErrInvalidManifest, i, err)
		}
		unk, err := normalizeExtMapInPlace(e.Unknown)
		if err != nil {
			return nil, fmt.Errorf("%w: entries[%d]: %v", ErrInvalidManifest, i, err)
		}
		for k := range unk {
			if _, known := knownEntryFields[k]; known {
				return nil, fmt.Errorf("%w: entries[%d]: unknown field collides with built-in field %q",
					ErrInvalidManifest, i, k)
			}
		}
		normEntries[i] = normEntry{attrs: attrs, form: form, unknown: unk}
	}

	buf.WriteString(",")
	writeJSONString(&buf, "entries")
	buf.WriteString(":[")
	for i, e := range m.Entries {
		if i > 0 {
			buf.WriteByte(',')
		}
		ne := normEntries[i]
		buf.WriteByte('{')
		writeJSONString(&buf, "id")
		buf.WriteByte(':')
		writeJSONString(&buf, string(e.ID))
		buf.WriteByte(',')
		writeJSONString(&buf, "size")
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(e.Size))
		switch ne.form {
		case AttrsNull:
			buf.WriteString(",")
			writeJSONString(&buf, "attrs")
			buf.WriteString(":null")
		case AttrsObject:
			buf.WriteString(",")
			writeJSONString(&buf, "attrs")
			buf.WriteByte(':')
			if err := emitObject(&buf, ne.attrs, hints); err != nil {
				return nil, fmt.Errorf("%w: entries[%d].attrs: %v", ErrInvalidManifest, i, err)
			}
		}
		// Entry-level unknown fields keep their original wire order.
		for _, k := range hints.keysFor(ne.unknown) {
			buf.WriteByte(',')
			writeJSONString(&buf, k)
			buf.WriteByte(':')
			if err := emitValue(&buf, ne.unknown[k], hints); err != nil {
				return nil, fmt.Errorf("%w: entries[%d].%s: %v", ErrInvalidManifest, i, k, err)
			}
		}
		buf.WriteByte('}')
	}
	buf.WriteString("]")

	// Top-level unknown fields keep their original wire order.
	for _, k := range hints.keysFor(topUnknown) {
		buf.WriteByte(',')
		writeJSONString(&buf, k)
		buf.WriteByte(':')
		if err := emitValue(&buf, topUnknown[k], hints); err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidManifest, k, err)
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// Unmarshal parses a manifest applying the compatibility rules.
func Unmarshal(data []byte) (*Manifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty input", ErrInvalidManifest)
	}
	hints := newOrderHints()
	v, err := decodeOrdered(data, hints)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	raw, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: top-level value must be an object", ErrInvalidManifest)
	}

	m := &Manifest{Unknown: map[string]any{}, orderHints: hints}

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
					// Explicit "attrs": null is distinct from a missing key.
					e.attrsForm = AttrsNull
				} else {
					attrs, ok := av.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("%w: entries[%d].attrs: expected object or null", ErrInvalidManifest, i)
					}
					e.attrsForm = AttrsObject
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

// normalizeExtMapInPlace normalizes every value of an extension map while
// keeping the same map instance (and nested map instances) so recorded
// key-order hints continue to apply. It returns the same map (possibly
// nil).
func normalizeExtMapInPlace(m map[string]any) (map[string]any, error) {
	for k, v := range m {
		nv, err := normalizeExtInPlace(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		m[k] = nv
	}
	return m, nil
}

// normalizeExtInPlace is like normalizeExt but, for containers, mutates
// and returns the same underlying map/slice instances so object-identity
// order hints survive normalization.
func normalizeExtInPlace(v any) (any, error) {
	switch x := v.(type) {
	case []any:
		for i, e := range x {
			ne, err := normalizeExtInPlace(e)
			if err != nil {
				return nil, err
			}
			x[i] = ne
		}
		return x, nil
	case map[string]any:
		return normalizeExtMapInPlace(x)
	default:
		return normalizeExt(v)
	}
}

func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", errors.New("expected string")
	}
	return s, nil
}
