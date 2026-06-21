package chunk

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestSplitAndReassemble(t *testing.T) {
	original := make([]byte, 10*1024*1024+777) // not a clean multiple of chunk size
	if _, err := rand.Read(original); err != nil {
		t.Fatal(err)
	}

	chunks, manifest, err := Split("blob-1", bytes.NewReader(original), 1024*1024)
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}
	if manifest.Size != int64(len(original)) {
		t.Fatalf("manifest size mismatch: got %d want %d", manifest.Size, len(original))
	}

	store := make(map[string][]byte, len(chunks))
	for _, c := range chunks {
		if !Verify(c.ID, c.Data) {
			t.Fatalf("chunk %d failed self-verification", c.Index)
		}
		store[c.ID] = c.Data
	}

	var out bytes.Buffer
	err = Reassemble(&out, manifest, func(id string) ([]byte, error) {
		return store[id], nil
	})
	if err != nil {
		t.Fatalf("reassemble failed: %v", err)
	}

	if !bytes.Equal(out.Bytes(), original) {
		t.Fatalf("reassembled data does not match original")
	}
}

func TestReassembleDetectsCorruption(t *testing.T) {
	original := []byte("hello distributed world, this is a test blob")
	chunks, manifest, err := Split("blob-2", bytes.NewReader(original), 8)
	if err != nil {
		t.Fatal(err)
	}
	store := make(map[string][]byte)
	for _, c := range chunks {
		store[c.ID] = c.Data
	}
	// Corrupt one chunk's bytes in storage.
	for id := range store {
		store[id] = []byte("corrupted")
		break
	}

	var out bytes.Buffer
	err = Reassemble(&out, manifest, func(id string) ([]byte, error) {
		return store[id], nil
	})
	if err == nil {
		t.Fatalf("expected integrity error on corrupted chunk, got nil")
	}
}

func TestDedupSameContent(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 1024)
	id1 := ID(data)
	id2 := ID(data)
	if id1 != id2 {
		t.Fatalf("identical content produced different IDs")
	}
}
