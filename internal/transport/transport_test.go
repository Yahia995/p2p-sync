package transport

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/store"
	"p2psync/internal/vv"
)

func serve(t *testing.T) (*store.Store, string, string) {
	t.Helper()
	root := t.TempDir()
	st, err := store.Open(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	server := httptest.NewServer(NewHandler(st, root))
	t.Cleanup(server.Close)
	return st, root, strings.TrimPrefix(server.URL, "http://")
}

func TestFileAndBlock(t *testing.T) {
	st, root, addr := serve(t)
	path := filepath.Join(root, "a.txt")
	os.WriteFile(path, []byte("hello"), 0o644)
	blocks, hash, _ := chunk.HashFile(path)
	st.Put(store.Record{Path: "a.txt", Version: vv.Vector{"n1": 1}, Hash: hash, Size: 5, MTime: 123}, blocks)
	client := NewClient(time.Second)

	records, err := client.Index(addr)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Path != "a.txt" || records[0].Version["n1"] != 1 {
		t.Fatalf("Index = %+v", records)
	}
	if records[0].MTime != 0 {
		t.Error("mtime is local and must not be sent")
	}

	info, err := client.File(addr, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Hash != hash || len(info.Blocks) != 1 || info.Blocks[0] != blocks[0] {
		t.Fatalf("File = %+v", info)
	}

	data, err := client.Block(addr, blocks[0].Hash)
	if err != nil || string(data) != "hello" {
		t.Fatalf("Block = %q, %v", data, err)
	}
}

func TestErrors(t *testing.T) {
	st, root, addr := serve(t)
	client := NewClient(time.Second)

	if _, err := client.File(addr, "missing"); err == nil {
		t.Error("expected error for unknown file")
	}
	if _, err := client.Block(addr, "unknown"); err == nil {
		t.Error("expected error for unknown block")
	}

	path := filepath.Join(root, "a.txt")
	os.WriteFile(path, []byte("hello"), 0o644)
	blocks, hash, _ := chunk.HashFile(path)
	st.Put(store.Record{Path: "a.txt", Version: vv.Vector{"n1": 1}, Hash: hash, Size: 5}, blocks)
	os.WriteFile(path, []byte("HELLO"), 0o644)
	if _, err := client.Block(addr, blocks[0].Hash); err == nil {
		t.Error("expected error when the file changed under the stored block")
	}

	if _, err := client.Index("127.0.0.1:1"); err == nil {
		t.Error("expected error for unreachable peer")
	}
}
