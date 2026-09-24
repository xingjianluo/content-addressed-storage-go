// Package chunk defines deterministic content splitting and stable
// content identifiers for the content-addressed storage.
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// ID is the stable content identifier. It is the lowercase hex encoding
// of SHA-256 over the chunk payload. Identical payloads always produce
// identical IDs, regardless of when or where they are written.
type ID string

// AlgorithmSHA256 is the identifier of the hash algorithm in use.
const AlgorithmSHA256 = "sha256"

// DefaultSplitSize is the fixed chunk size used unless a caller overrides it.
const DefaultSplitSize = 64 * 1024

// Chunk is one deterministic piece of content.
type Chunk struct {
	ID   ID
	Data []byte
}

// Errors used for distinguishable failure reasons.
var (
	// ErrIDMismatch means the recorded ID does not match the payload hash.
	ErrIDMismatch = errors.New("chunk: content id mismatch")
	// ErrCorrupt means a payload failed its integrity check at rest.
	ErrCorrupt = errors.New("chunk: content corrupt")
)

// ComputeID returns the stable ID for a payload: hex(sha256(data)).
func ComputeID(data []byte) ID {
	sum := sha256.Sum256(data)
	return ID(hex.EncodeToString(sum[:]))
}

// Split slices data into deterministic fixed-size chunks. Splitting is
// purely positional: the same input and size always yield the same
// boundaries, the same number of chunks and the same IDs. A zero-length
// input yields no chunks; the final chunk may be shorter than size.
func Split(data []byte, size int) []Chunk {
	if size <= 0 {
		size = DefaultSplitSize
	}
	if len(data) == 0 {
		return []Chunk{}
	}
	n := (len(data) + size - 1) / size
	chunks := make([]Chunk, 0, n)
	for start := 0; start < len(data); start += size {
		end := start + size
		if end > len(data) {
			end = len(data)
		}
		payload := data[start:end]
		chunks = append(chunks, Chunk{ID: ComputeID(payload), Data: payload})
	}
	return chunks
}

// Join concatenates chunks back into the original payload.
func Join(chunks []Chunk) []byte {
	total := 0
	for _, c := range chunks {
		total += len(c.Data)
	}
	out := make([]byte, 0, total)
	for _, c := range chunks {
		out = append(out, c.Data...)
	}
	return out
}

// Verify checks chunk integrity: the recorded ID must equal the hash of
// the payload. A mismatch is reported as ErrIDMismatch so callers can
// distinguish a wrong identifier from other I/O failures.
func Verify(c Chunk) error {
	want := ComputeID(c.Data)
	if c.ID != want {
		return fmt.Errorf("%w: claimed=%s actual=%s", ErrIDMismatch, c.ID, want)
	}
	return nil
}
