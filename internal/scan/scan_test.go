package scan

import (
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"p2psync/internal/store"
	"p2psync/internal/vv"
)

func newScanner(t *testing.T) *Scanner {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, MetaDir), 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(root, MetaDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &Scanner{Root: root, Store: st, Self: "self", Lock: &sync.Mutex{}}
}

func writeFile(t *testing.T, s *Scanner, path, content string) {
	t.Helper()
	full := filepath.Join(s.Root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func rescan(t *testing.T, s *Scanner) []string {
	t.Helper()
	changed, err := s.Rescan()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(changed)
	return changed
}

func record(t *testing.T, s *Scanner, path string) store.Record {
	t.Helper()
	rec, found, err := s.Store.Get(path)
	if err != nil || !found {
		t.Fatalf("record %s: found=%v err=%v", path, found, err)
	}
	return rec
}

func wantVersion(t *testing.T, rec store.Record, want vv.Vector) {
	t.Helper()
	if vv.Compare(rec.Version, want) != vv.Equal {
		t.Errorf("%s version = %v, want %v", rec.Path, rec.Version, want)
	}
}

func TestNewFile(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "hello")
	if got := rescan(t, s); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("changed = %v", got)
	}
	rec := record(t, s, "a.txt")
	wantVersion(t, rec, vv.Vector{"self": 1})
	if rec.Size != 5 || rec.Deleted || rec.Hash == "" {
		t.Errorf("unexpected record %+v", rec)
	}
}

func TestUnchangedFileNotBumped(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "hello")
	rescan(t, s)
	if got := rescan(t, s); len(got) != 0 {
		t.Fatalf("second rescan changed %v", got)
	}
	wantVersion(t, record(t, s, "a.txt"), vv.Vector{"self": 1})
}

func TestSameContentRewriteNotBumped(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "hello")
	rescan(t, s)
	time.Sleep(10 * time.Millisecond)
	writeFile(t, s, "a.txt", "hello")
	if got := rescan(t, s); len(got) != 0 {
		t.Fatalf("rewrite with same content changed %v", got)
	}
	wantVersion(t, record(t, s, "a.txt"), vv.Vector{"self": 1})
}

func TestModifyBumps(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "hello")
	rescan(t, s)
	first := record(t, s, "a.txt")
	writeFile(t, s, "a.txt", "hello world")
	if got := rescan(t, s); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("changed = %v", got)
	}
	rec := record(t, s, "a.txt")
	wantVersion(t, rec, vv.Vector{"self": 2})
	if rec.Hash == first.Hash {
		t.Error("hash did not change")
	}
}

func TestDeleteCreatesTombstone(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "hello")
	rescan(t, s)
	os.Remove(filepath.Join(s.Root, "a.txt"))
	if got := rescan(t, s); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("changed = %v", got)
	}
	rec := record(t, s, "a.txt")
	wantVersion(t, rec, vv.Vector{"self": 2})
	if !rec.Deleted || rec.Hash != "" || rec.Size != 0 {
		t.Errorf("not a tombstone: %+v", rec)
	}
	if got := rescan(t, s); len(got) != 0 {
		t.Fatalf("tombstone bumped again: %v", got)
	}
}

func TestRecreateAfterDelete(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "hello")
	rescan(t, s)
	os.Remove(filepath.Join(s.Root, "a.txt"))
	rescan(t, s)
	writeFile(t, s, "a.txt", "hello")
	if got := rescan(t, s); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("changed = %v", got)
	}
	rec := record(t, s, "a.txt")
	wantVersion(t, rec, vv.Vector{"self": 3})
	if rec.Deleted {
		t.Error("record still deleted")
	}
}

func TestVersionKeepsOtherNodes(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a.txt", "from peer")
	rescan(t, s)
	rec := record(t, s, "a.txt")
	rec.Version = vv.Vector{"peer": 4}
	s.Store.Put(rec, nil)
	writeFile(t, s, "a.txt", "local edit")
	rescan(t, s)
	wantVersion(t, record(t, s, "a.txt"), vv.Vector{"peer": 4, "self": 1})
}

func TestNestedDirsAndMetaDirIgnored(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "a/b/c.txt", "deep")
	writeFile(t, s, "a/d.txt", "less deep")
	writeFile(t, s, MetaDir+"/tmp/partial", "ignored")
	got := rescan(t, s)
	if !slices.Equal(got, []string{"a/b/c.txt", "a/d.txt"}) {
		t.Fatalf("changed = %v", got)
	}
}

func TestUnstableFileSkipped(t *testing.T) {
	s := newScanner(t)
	s.StableDelay = 300 * time.Millisecond
	writeFile(t, s, "growing.txt", "part 1")

	done := make(chan []string)
	go func() {
		changed, _ := s.Rescan()
		done <- changed
	}()
	time.Sleep(100 * time.Millisecond)
	writeFile(t, s, "growing.txt", "part 1 and part 2")

	if got := <-done; len(got) != 0 {
		t.Fatalf("file indexed while still being written: %v", got)
	}
	if got := rescan(t, s); !slices.Equal(got, []string{"growing.txt"}) {
		t.Fatalf("changed = %v", got)
	}
	if rec := record(t, s, "growing.txt"); rec.Size != int64(len("part 1 and part 2")) {
		t.Errorf("indexed wrong size %d", rec.Size)
	}
}
