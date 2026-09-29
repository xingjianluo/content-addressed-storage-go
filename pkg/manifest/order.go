package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"sort"
	"strconv"
)

// orderHints remembers the on-wire key order of decoded JSON objects. The
// public representation stays map[string]any (no API changes); object
// identity is used as the hint key, so nested extension objects decoded
// from the wire re-emit in their original key order and untouched
// read-modify-write cycles are byte-for-byte stable.
type orderHints struct {
	order map[uintptr][]string
}

func newOrderHints() *orderHints {
	return &orderHints{order: map[uintptr][]string{}}
}

func mapPtr(m map[string]any) uintptr {
	return reflect.ValueOf(m).Pointer()
}

func (h *orderHints) record(m map[string]any, keys []string) {
	if h == nil || m == nil {
		return
	}
	h.order[mapPtr(m)] = append([]string(nil), keys...)
}

// keysFor returns the wire key order of m when known, followed (in sorted
// order) by any keys added after decoding. Objects without a hint are
// emitted fully sorted, which keeps API-constructed manifests
// deterministic.
func (h *orderHints) keysFor(m map[string]any) []string {
	seen := map[string]bool{}
	var keys []string
	if h != nil {
		if orig, ok := h.order[mapPtr(m)]; ok {
			for _, k := range orig {
				if _, present := m[k]; present {
					keys = append(keys, k)
					seen[k] = true
				}
			}
		}
	}
	var extra []string
	for k := range m {
		if !seen[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	return append(keys, extra...)
}

// decodeOrdered parses JSON into plain map[string]any/[]any values while
// recording object key order in hints. Numbers remain json.Number.
func decodeOrdered(data []byte, hints *orderHints) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeValue(dec, hints)
	if err != nil {
		return nil, err
	}
	// Reject trailing content after the top-level value.
	if dec.More() {
		return nil, errors.New("multiple top-level values")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder, hints *orderHints) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			m := map[string]any{}
			var keys []string
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key := kt.(string)
				val, err := decodeValue(dec, hints)
				if err != nil {
					return nil, err
				}
				if _, dup := m[key]; !dup {
					keys = append(keys, key)
				}
				m[key] = val
			}
			if _, err := dec.Token(); err != nil { // closing brace
				return nil, err
			}
			hints.record(m, keys)
			return m, nil
		case '[':
			arr := []any{}
			for dec.More() {
				val, err := decodeValue(dec, hints)
				if err != nil {
					return nil, err
				}
				arr = append(arr, val)
			}
			if _, err := dec.Token(); err != nil { // closing bracket
				return nil, err
			}
			return arr, nil
		}
	}
	return tok, nil
}

// emitValue serializes one extension value with object key order taken
// from hints. Integers (*big.Int, json.Number, built-in integer kinds)
// become bare JSON numbers; strings/keys use standard JSON escaping.
func emitValue(buf *bytes.Buffer, v any, hints *orderHints) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case string:
		writeJSONString(buf, x)
	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		buf.WriteString(string(x))
	case *big.Int:
		if x == nil {
			buf.WriteString("null")
		} else {
			buf.WriteString(x.String())
		}
	case big.Int:
		buf.WriteString(x.String())
	case int:
		buf.WriteString(strconv.Itoa(x))
	case int64:
		buf.WriteString(strconv.FormatInt(x, 10))
	case int32:
		buf.WriteString(strconv.FormatInt(int64(x), 10))
	case uint:
		buf.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(x, 10))
	case uint32:
		buf.WriteString(strconv.FormatUint(uint64(x), 10))
	case float64:
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		buf.Write(b)
	case []any:
		buf.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := emitValue(buf, e, hints); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		return emitObject(buf, x, hints)
	default:
		// Fall back to encoding/json so custom JSON-marshalable values still
		// work; unsupported types (channel etc.) error here.
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return nil
}

func emitObject(buf *bytes.Buffer, m map[string]any, hints *orderHints) error {
	buf.WriteByte('{')
	keys := hints.keysFor(m)
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		writeJSONString(buf, k)
		buf.WriteByte(':')
		if err := emitValue(buf, m[k], hints); err != nil {
			return err
		}
	}
	buf.WriteByte('}')
	return nil
}

// writeJSONString writes a JSON-quoted string using the standard encoder
// so escaping matches encoding/json exactly (HTML-safe by design, which
// is what encoding/json produced before).
func writeJSONString(buf *bytes.Buffer, s string) {
	b, _ := json.Marshal(s)
	buf.Write(b)
}
