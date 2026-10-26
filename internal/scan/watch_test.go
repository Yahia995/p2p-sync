package scan

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2psync/internal/vv"
)

func startWatching(t *testing.T, s *Scanner) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Watch(ctx, 50*time.Millisecond, time.Hour) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func indexed(s *Scanner, path string, deleted bool) func() bool {
	return func() bool {
		rec, found, err := s.Store.Get(path)
		return err == nil && found && rec.Deleted == deleted
	}
}

func TestWatchIndexesExistingFilesAtStart(t *testing.T) {
	s := newScanner(t)
	writeFile(t, s, "before.txt", "already here")
	startWatching(t, s)
	eventually(t, "before.txt indexed", indexed(s, "before.txt", false))
}

func TestWatchNewFileAndDelete(t *testing.T) {
	s := newScanner(t)
	startWatching(t, s)

	writeFile(t, s, "a.txt", "hello")
	eventually(t, "a.txt indexed", indexed(s, "a.txt", false))

	os.Remove(filepath.Join(s.Root, "a.txt"))
	eventually(t, "a.txt tombstone", indexed(s, "a.txt", true))
}

func TestWatchNewNestedDirectory(t *testing.T) {
	s := newScanner(t)
	startWatching(t, s)

	writeFile(t, s, "x/y/z/deep.txt", "deep")
	eventually(t, "deep.txt indexed", indexed(s, "x/y/z/deep.txt", false))

	time.Sleep(100 * time.Millisecond)
	writeFile(t, s, "x/y/z/later.txt", "added after the watch was registered")
	eventually(t, "later.txt indexed", indexed(s, "x/y/z/later.txt", false))
}

func TestWatchIgnoresMetaDir(t *testing.T) {
	s := newScanner(t)
	startWatching(t, s)
	writeFile(t, s, MetaDir+"/tmp/download-1", "partial")
	writeFile(t, s, "real.txt", "real")
	eventually(t, "real.txt indexed", indexed(s, "real.txt", false))
	if _, found, _ := s.Store.Get(MetaDir + "/tmp/download-1"); found {
		t.Error("file inside .p2psync was indexed")
	}
}

func TestWatchFileWrittenInPiecesIndexedOnce(t *testing.T) {
	s := newScanner(t)
	s.StableDelay = 100 * time.Millisecond
	startWatching(t, s)

	full := filepath.Join(s.Root, "slow.log")
	f, err := os.Create(full)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		f.WriteString("line of a slowly written file\n")
		time.Sleep(10 * time.Millisecond)
	}
	f.Close()

	wantSize := int64(10 * len("line of a slowly written file\n"))
	eventually(t, "slow.log fully indexed", func() bool {
		rec, found, _ := s.Store.Get("slow.log")
		return found && rec.Size == wantSize
	})
	rec, _, _ := s.Store.Get("slow.log")
	if vv.Compare(rec.Version, vv.Vector{"self": 1}) != vv.Equal {
		t.Errorf("version = %v, want one local version", rec.Version)
	}
}
