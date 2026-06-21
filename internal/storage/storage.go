// Package storage defines the chunk storage backend interface and provides
// a local-filesystem implementation. See s3.go for the S3-backed
// implementation used for cold/overflow storage.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrNotFound is returned when a chunk does not exist in the backend.
var ErrNotFound = errors.New("storage: chunk not found")

// Backend is the interface every storage implementation (local disk, S3,
// in-memory for tests) must satisfy. Nodes use this for both serving reads
// and accepting replicated writes from peers.
type Backend interface {
	Put(chunkID string, data []byte) error
	Get(chunkID string) ([]byte, error)
	Has(chunkID string) bool
	Delete(chunkID string) error
	List() ([]string, error)
}

// LocalFS stores chunks as individual files under a root directory, sharded
// into subdirectories by the first 2 hex characters of the chunk ID to
// avoid huge flat directories.
type LocalFS struct {
	root string
}

// NewLocalFS creates (if needed) and returns a filesystem-backed store
// rooted at dir.
func NewLocalFS(dir string) (*LocalFS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: creating root %s: %w", dir, err)
	}
	return &LocalFS{root: dir}, nil
}

func (l *LocalFS) path(chunkID string) string {
	if len(chunkID) < 2 {
		return filepath.Join(l.root, "_short", chunkID)
	}
	return filepath.Join(l.root, chunkID[:2], chunkID)
}

func (l *LocalFS) Put(chunkID string, data []byte) error {
	p := l.path(chunkID)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (l *LocalFS) Get(chunkID string) ([]byte, error) {
	data, err := os.ReadFile(l.path(chunkID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}

func (l *LocalFS) Has(chunkID string) bool {
	_, err := os.Stat(l.path(chunkID))
	return err == nil
}

func (l *LocalFS) Delete(chunkID string) error {
	err := os.Remove(l.path(chunkID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (l *LocalFS) List() ([]string, error) {
	var ids []string
	err := filepath.WalkDir(l.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".tmp" {
			return nil
		}
		ids = append(ids, filepath.Base(path))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// Memory is a simple in-process backend, primarily useful for tests.
type Memory struct {
	data map[string][]byte
}

func NewMemory() *Memory {
	return &Memory{data: make(map[string][]byte)}
}

func (m *Memory) Put(chunkID string, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[chunkID] = cp
	return nil
}

func (m *Memory) Get(chunkID string) ([]byte, error) {
	d, ok := m.data[chunkID]
	if !ok {
		return nil, ErrNotFound
	}
	return d, nil
}

func (m *Memory) Has(chunkID string) bool {
	_, ok := m.data[chunkID]
	return ok
}

func (m *Memory) Delete(chunkID string) error {
	delete(m.data, chunkID)
	return nil
}

func (m *Memory) List() ([]string, error) {
	ids := make([]string, 0, len(m.data))
	for id := range m.data {
		ids = append(ids, id)
	}
	return ids, nil
}

// Copy streams all chunks from src into dst. Used by recovery to rebuild a
// replacement node's local store from a healthy peer.
func Copy(dst, src Backend) error {
	ids, err := src.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		data, err := src.Get(id)
		if err != nil {
			return err
		}
		if err := dst.Put(id, data); err != nil {
			return err
		}
	}
	return nil
}

var _ Backend = (*LocalFS)(nil)
var _ Backend = (*Memory)(nil)
var _ io.Closer = (*noopCloser)(nil)

type noopCloser struct{}

func (noopCloser) Close() error { return nil }
