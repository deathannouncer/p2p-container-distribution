// Package chunk handles splitting container image layers (or any blob) into
// content-addressed chunks for distribution across the cluster.
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// DefaultSize is the default chunk size (4 MiB), chosen to balance
// replication granularity against per-chunk metadata/network overhead.
const DefaultSize = 4 * 1024 * 1024

// Chunk represents a single content-addressed piece of a larger blob.
type Chunk struct {
	ID    string // hex-encoded sha256 of Data, used as the ring key
	Index int    // position within the parent blob
	Data  []byte
}

// Manifest describes how a blob was split, so it can be reassembled.
type Manifest struct {
	BlobID    string   `json:"blob_id"`
	Size      int64    `json:"size"`
	ChunkIDs  []string `json:"chunk_ids"`
	ChunkSize int      `json:"chunk_size"`
}

// Split reads r and splits it into fixed-size, content-addressed chunks.
// Returns the chunks and a manifest describing how to reassemble them.
func Split(blobID string, r io.Reader, chunkSize int) ([]Chunk, *Manifest, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultSize
	}
	var chunks []Chunk
	var totalSize int64
	buf := make([]byte, chunkSize)
	idx := 0
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			id := ID(data)
			chunks = append(chunks, Chunk{ID: id, Index: idx, Data: data})
			totalSize += int64(n)
			idx++
		}
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("chunk: reading blob %s: %w", blobID, err)
		}
	}

	chunkIDs := make([]string, len(chunks))
	for i, c := range chunks {
		chunkIDs[i] = c.ID
	}

	m := &Manifest{
		BlobID:    blobID,
		Size:      totalSize,
		ChunkIDs:  chunkIDs,
		ChunkSize: chunkSize,
	}
	return chunks, m, nil
}

// ID computes the content address (sha256 hex digest) for a chunk's bytes.
// Identical content always produces the same ID, which is what allows
// dedup and verification on read.
func ID(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Verify checks that data hashes to the expected chunk ID, used after
// fetching a chunk from a peer to detect corruption or bit-rot.
func Verify(id string, data []byte) bool {
	return ID(data) == id
}

// Reassemble concatenates chunk data in manifest order into w.
func Reassemble(w io.Writer, m *Manifest, get func(chunkID string) ([]byte, error)) error {
	for _, id := range m.ChunkIDs {
		data, err := get(id)
		if err != nil {
			return fmt.Errorf("chunk: fetching %s: %w", id, err)
		}
		if !Verify(id, data) {
			return fmt.Errorf("chunk: integrity check failed for %s", id)
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("chunk: writing %s: %w", id, err)
		}
	}
	return nil
}
