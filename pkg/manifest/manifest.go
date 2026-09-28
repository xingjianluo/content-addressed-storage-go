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
//	    {"id": "<sha256-hex>", "size": 123,
//	     ... chunk-level extension fields, preserved verbatim ...}
//	  ],
//	  ... object-level extension fields, preserved verbatim ...
//	}
//
// Compatibility rules:
//   - Data written by older versions (MinVersion <= Version < CurrentVersion)
//     is always readable. A missing "version" field is treated as version 1.
//   - A manifest whose Version is greater than MaxSupportedVersion is
//     rejected with ErrUnsupportedVersion; such data is never silently
//     rewritten or partially understood.
//   - Unknown fields encountered while reading are retained verbatim — both
//     object-level ones in Manifest.Unknown and per-chunk (per-entry) ones
//     in Entry.Unknown — and re-emitted by Marshal, so a read-modify-write
//     cycle never drops information produced by a newer/other writer.
//   - Numbers are decoded with json.Number, so arbitrarily large integers
//     keep every digit; no float64 approximation is ever introduced.
//   - Malformed JSON, wrong field types or invalid entries are reported as
//     ErrInvalidManifest with a wrapped reason.
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

// Version constants.
const (
	// MinVersion is the oldest data version this build can read.
	MinVersion = 1
	// CurrentVersion is the schema version produced by this build.
	//
	// Version history:
	//   1: {version,name,entries:[{id,size}]} plus preserved top-level
	//      unknown fields;
	//   2: like v1, but every entry may additionally carry its own bag of
	//      upper-layer-defined extension fields (chunk-level extensions),
	//      which are preserved verbatim just like top-level ones.
	CurrentVersion = 2
	// MaxSupportedVersion is the newest data version this build understands.
	MaxSupportedVersion = 2
)

// Entry references one chunk of an object in order. Besides the built-in
// content ID and length, an entry carries chunk-level extension
// information (Unknown) defined by upper-layer tools: arbitrary nested
// objects, arrays, booleans, null or (arbitrarily large) integers. It is
// preserved verbatim across unmarshal/marshal and never dropped by a
// read-modify-write cycle.
type Entry struct {
	ID   chunk.ID `json:"id"`
	Size int      `json:"size"`
	// Unknown preserves chunk-level (per-entry) schema fields this build
	// does not know about.
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

// knownEntryFields are the built-in fields of an entries[] element.
var knownEntryFields = map[string]struct{}{
	"id":   {},
	"size": {},
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
		out[k] = v
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
	for _, e := range m.Entries {
		em := map[string]any{"id": string(e.ID), "size": e.Size}
		for k, v := range e.Unknown {
			if _, known := knownEntryFields[k]; known {
				// A preserved key must never masquerade as a built-in field.
				return nil, fmt.Errorf("%w: entries: unknown field collides with built-in field %q",
					ErrInvalidManifest, k)
			}
			em[k] = v
		}
		entries = append(entries, em)
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
	raw := map[string]any{}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty input", ErrInvalidManifest)
	}
	// UseNumber keeps every integer as an exact json.Number instead of a
	// float64, so large integers survive with all digits intact.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: multiple JSON values", ErrInvalidManifest)
	}

	m := &Manifest{Unknown: map[string]any{}}

	if v, ok := raw["version"]; ok {
		f, err := asInt(v)
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
			entry := Entry{ID: chunk.ID(idStr), Size: size, Unknown: map[string]any{}}
			// Preserve every non-built-in field of this entry verbatim.
			for ek, ev := range obj {
				if _, known := knownEntryFields[ek]; known {
					continue
				}
				entry.Unknown[ek] = ev
			}
			m.Entries = append(m.Entries, entry)
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

// asInt accepts a JSON number that is an exact integer value. A
// json.Number (the type produced by UseNumber) is parsed digit by digit so
// that no precision is lost on its way to an int; a float64 is accepted
// only when it already holds an integral value.
func asInt(v any) (int, error) {
	switch n := v.(type) {
	case json.Number:
		i, err := strconv.ParseInt(n.String(), 10, 64)
		if err != nil {
			return 0, errors.New("expected integer")
		}
		return int(i), nil
	case float64:
		if n != float64(int64(n)) {
			return 0, errors.New("expected integer")
		}
		return int(n), nil
	default:
		return 0, errors.New("expected integer")
	}
}

func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", errors.New("expected string")
	}
	return s, nil
}
