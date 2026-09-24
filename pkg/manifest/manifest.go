// Package manifest defines versioned content manifests. A manifest lists
// the ordered chunks of an object together with their expected IDs and
// sizes, and supports forward/backward compatible evolution.
//
// Wire format (JSON, one object):
//
//	{
//	  "version": 1,
//	  "name": "example.txt",
//	  "entries": [ {"id": "<sha256-hex>", "size": 123} ],
//	  ... unknown fields are preserved verbatim ...
//	}
//
// Compatibility rules:
//   - Data written by older versions (MinVersion <= Version < CurrentVersion)
//     is always readable. A missing "version" field is treated as version 1.
//   - A manifest whose Version is greater than MaxSupportedVersion is
//     rejected with ErrUnsupportedVersion; such data is never silently
//     rewritten or partially understood.
//   - Unknown fields encountered while reading are retained in Unknown and
//     re-emitted by Marshal, so a read-modify-write cycle never drops
//     information produced by a newer/other writer.
//   - Malformed JSON, wrong field types or invalid entries are reported as
//     ErrInvalidManifest with a wrapped reason.
package manifest

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
)

// Version constants.
const (
	// MinVersion is the oldest data version this build can read.
	MinVersion = 1
	// CurrentVersion is the schema version produced by this build.
	CurrentVersion = 1
	// MaxSupportedVersion is the newest data version this build understands.
	MaxSupportedVersion = 1
)

// Entry references one chunk of an object in order.
type Entry struct {
	ID   chunk.ID `json:"id"`
	Size int      `json:"size"`
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
		entries = append(entries, map[string]any{"id": string(e.ID), "size": e.Size})
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
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
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
			m.Entries = append(m.Entries, Entry{ID: chunk.ID(idStr), Size: size})
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

// asInt accepts a JSON number that is an integer value.
func asInt(v any) (int, error) {
	f, ok := v.(float64)
	if !ok {
		return 0, errors.New("expected integer")
	}
	if f != float64(int64(f)) {
		return 0, errors.New("expected integer")
	}
	return int(f), nil
}

func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", errors.New("expected string")
	}
	return s, nil
}
