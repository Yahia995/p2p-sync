package syncer

import (
	"bytes"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/scan"
	"p2psync/internal/store"
	"p2psync/internal/transport"
	"p2psync/internal/vv"
)

type testNode struct {
	root        string
	store       *store.Store
	scanner     *scan.Scanner
	syncer      *Syncer
	addr        string
	blockServed atomic.Int64
	corrupt     atomic.Bool
}

func startNode(t *testing.T) *testNode {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, scan.MetaDir), 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(root, scan.MetaDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	self, err := st.NodeID()
	if err != nil {
		t.Fatal(err)
	}
	lock := &sync.Mutex{}
	n := &testNode{root: root, store: st}
	n.scanner = &scan.Scanner{Root: root, Store: st, Self: self, Lock: lock}
	n.syncer = &Syncer{Self: self, Root: root, Store: st, Lock: lock, Client: transport.NewClient("test", 5*time.Second, transport.NewFaults())}
	if err := n.syncer.ResetTmp(); err != nil {
		t.Fatal(err)
	}

	handler := transport.NewHandler(st, root)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/block" {
			n.blockServed.Add(1)
			if n.corrupt.Load() {
				w.Write([]byte("garbage"))
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	n.addr = strings.TrimPrefix(server.URL, "http://")
	return n
}

func (n *testNode) write(t *testing.T, path string, data []byte) {
	t.Helper()
	full := filepath.Join(n.root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func (n *testNode) read(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(n.root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (n *testNode) exists(path string) bool {
	_, err := os.Stat(filepath.Join(n.root, filepath.FromSlash(path)))
	return err == nil
}

func (n *testNode) rescan(t *testing.T) []string {
	t.Helper()
	changed, err := n.scanner.Rescan()
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func (n *testNode) pull(t *testing.T, from *testNode) {
	t.Helper()
	if err := n.syncer.SyncWith(from.addr); err != nil {
		t.Fatal(err)
	}
}

func (n *testNode) record(t *testing.T, path string) store.Record {
	t.Helper()
	rec, found, err := n.store.Get(path)
	if err != nil || !found {
		t.Fatalf("record %s: found=%v err=%v", path, found, err)
	}
	return rec
}

func randomBytes(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed))
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rng.IntN(256))
	}
	return data
}

func sameRecord(t *testing.T, a, b *testNode, path string) {
	t.Helper()
	ra, rb := a.record(t, path), b.record(t, path)
	if vv.Compare(ra.Version, rb.Version) != vv.Equal || ra.Hash != rb.Hash || ra.Deleted != rb.Deleted {
		t.Errorf("%s differs: %+v vs %+v", path, ra, rb)
	}
}

func TestCreatePropagates(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "docs/hello.txt", []byte("hello"))
	a.rescan(t)

	b.pull(t, a)

	if got := b.read(t, "docs/hello.txt"); string(got) != "hello" {
		t.Fatalf("content = %q", got)
	}
	sameRecord(t, a, b, "docs/hello.txt")
	if changed := b.rescan(t); len(changed) != 0 {
		t.Errorf("received file was re-indexed as a local change: %v", changed)
	}
}

func TestEmptyFilePropagates(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "empty", nil)
	a.rescan(t)
	b.pull(t, a)
	if got := b.read(t, "empty"); len(got) != 0 {
		t.Fatalf("content = %q", got)
	}
	sameRecord(t, a, b, "empty")
}

func TestModifyAndDeletePropagate(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "f.txt", []byte("v1"))
	a.rescan(t)
	b.pull(t, a)

	a.write(t, "f.txt", []byte("version 2"))
	a.rescan(t)
	b.pull(t, a)
	if got := b.read(t, "f.txt"); string(got) != "version 2" {
		t.Fatalf("content = %q", got)
	}
	sameRecord(t, a, b, "f.txt")

	os.Remove(filepath.Join(a.root, "f.txt"))
	a.rescan(t)
	b.pull(t, a)
	if b.exists("f.txt") {
		t.Fatal("file not deleted on b")
	}
	if !b.record(t, "f.txt").Deleted {
		t.Error("b has no tombstone")
	}
	sameRecord(t, a, b, "f.txt")
}

func TestBothDirections(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "from-a", []byte("a"))
	b.write(t, "from-b", []byte("b"))
	a.rescan(t)
	b.rescan(t)

	a.pull(t, b)
	b.pull(t, a)

	for _, n := range []*testNode{a, b} {
		if !n.exists("from-a") || !n.exists("from-b") {
			t.Fatalf("node %s missing files", n.root)
		}
	}
	sameRecord(t, a, b, "from-a")
	sameRecord(t, a, b, "from-b")
}

func TestOlderVersionIgnored(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "f.txt", []byte("v1"))
	a.rescan(t)
	b.pull(t, a)
	b.write(t, "f.txt", []byte("v2 on b"))
	b.rescan(t)

	a.blockServed.Store(0)
	b.pull(t, a)
	if got := b.read(t, "f.txt"); string(got) != "v2 on b" {
		t.Fatalf("newer local version overwritten: %q", got)
	}
	if a.blockServed.Load() != 0 {
		t.Error("blocks were requested for an older version")
	}
}

func TestOnlyChangedChunkTransferred(t *testing.T) {
	a, b := startNode(t), startNode(t)
	data := randomBytes(1, 5*chunk.Size+100)
	a.write(t, "big.bin", data)
	a.rescan(t)
	b.pull(t, a)
	if a.blockServed.Load() != 6 {
		t.Fatalf("first transfer requested %d blocks, want 6", a.blockServed.Load())
	}
	if !bytes.Equal(b.read(t, "big.bin"), data) {
		t.Fatal("big file differs")
	}

	data[3*chunk.Size+7] ^= 0xff
	a.write(t, "big.bin", data)
	a.rescan(t)
	a.blockServed.Store(0)
	b.pull(t, a)
	if a.blockServed.Load() != 1 {
		t.Errorf("update requested %d blocks, want 1", a.blockServed.Load())
	}
	if !bytes.Equal(b.read(t, "big.bin"), data) {
		t.Fatal("updated big file differs")
	}
	sameRecord(t, a, b, "big.bin")
}

func TestCorruptBlockRejected(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "f.bin", randomBytes(2, 1000))
	a.rescan(t)
	a.corrupt.Store(true)

	if err := b.syncer.SyncWith(a.addr); err == nil {
		t.Fatal("expected error for corrupt block")
	}
	if b.exists("f.bin") {
		t.Error("corrupt file was installed")
	}
	if _, found, _ := b.store.Get("f.bin"); found {
		t.Error("record stored for corrupt file")
	}
	leftovers, _ := os.ReadDir(b.syncer.tmpDir())
	if len(leftovers) != 0 {
		t.Errorf("temp files left: %v", leftovers)
	}

	a.corrupt.Store(false)
	b.pull(t, a)
	sameRecord(t, a, b, "f.bin")
}

func TestUnscannedLocalEditNotOverwritten(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "f.txt", []byte("v1"))
	a.rescan(t)
	b.pull(t, a)

	a.write(t, "f.txt", []byte("v2 from a"))
	a.rescan(t)
	time.Sleep(10 * time.Millisecond)
	b.write(t, "f.txt", []byte("edit on b not scanned yet"))

	b.pull(t, a)
	if got := b.read(t, "f.txt"); string(got) != "edit on b not scanned yet" {
		t.Fatalf("local edit overwritten: %q", got)
	}
}

func TestUnscannedLocalFileNotOverwritten(t *testing.T) {
	a, b := startNode(t), startNode(t)
	a.write(t, "f.txt", []byte("from a"))
	a.rescan(t)
	b.write(t, "f.txt", []byte("new on b"))

	b.pull(t, a)
	if got := b.read(t, "f.txt"); string(got) != "new on b" {
		t.Fatalf("local file overwritten: %q", got)
	}
}

func TestResetTmp(t *testing.T) {
	n := startNode(t)
	leftover := filepath.Join(n.syncer.tmpDir(), "download-123")
	if err := os.WriteFile(leftover, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := n.syncer.ResetTmp(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(n.syncer.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("tmp not empty: %v", entries)
	}
}

func TestValidPath(t *testing.T) {
	valid := []string{"a.txt", "dir/a.txt", ".hidden", "a/.p2psync"}
	invalid := []string{"", "../escape", "/etc/passwd", "a/../../b", ".p2psync", ".p2psync/index.db"}
	for _, p := range valid {
		if !validPath(p) {
			t.Errorf("%q should be valid", p)
		}
	}
	for _, p := range invalid {
		if validPath(p) {
			t.Errorf("%q should be invalid", p)
		}
	}
}
