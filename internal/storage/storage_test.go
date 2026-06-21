package storage

import (
	"path/filepath"
	"testing"
)

func TestLocalFSPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewLocalFS(dir)
	if err != nil {
		t.Fatal(err)
	}

	id := "abcd1234"
	data := []byte("hello chunk")

	if fs.Has(id) {
		t.Fatalf("expected chunk to be absent before Put")
	}
	if err := fs.Put(id, data); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if !fs.Has(id) {
		t.Fatalf("expected chunk to be present after Put")
	}

	got, err := fs.Get(id)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q want %q", got, data)
	}

	if err := fs.Delete(id); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if fs.Has(id) {
		t.Fatalf("expected chunk to be absent after Delete")
	}

	if _, err := fs.Get(id); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLocalFSSharding(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewLocalFS(dir)
	id := "ff112233"
	fs.Put(id, []byte("x"))
	expected := filepath.Join(dir, "ff", id)
	if _, err := fs.Get(id); err != nil {
		t.Fatal(err)
	}
	if fs.path(id) != expected {
		t.Fatalf("path mismatch: got %s want %s", fs.path(id), expected)
	}
}

func TestLocalFSList(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewLocalFS(dir)
	ids := []string{"aa01", "bb02", "cc03"}
	for _, id := range ids {
		fs.Put(id, []byte(id))
	}
	listed, err := fs.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != len(ids) {
		t.Fatalf("expected %d entries, got %d", len(ids), len(listed))
	}
}

func TestMemoryBackend(t *testing.T) {
	m := NewMemory()
	if err := m.Put("id1", []byte("data1")); err != nil {
		t.Fatal(err)
	}
	got, err := m.Get("id1")
	if err != nil || string(got) != "data1" {
		t.Fatalf("unexpected get result: %v %v", got, err)
	}
}

func TestCopy(t *testing.T) {
	src := NewMemory()
	dst := NewMemory()
	src.Put("a", []byte("1"))
	src.Put("b", []byte("2"))

	if err := Copy(dst, src); err != nil {
		t.Fatal(err)
	}
	if !dst.Has("a") || !dst.Has("b") {
		t.Fatalf("expected dst to have copied chunks")
	}
}
