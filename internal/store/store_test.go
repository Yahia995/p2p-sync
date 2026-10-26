package store

import (
	"path/filepath"
	"slices"
	"testing"

	"p2psync/internal/chunk"
	"p2psync/internal/vv"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestNodeIDPersists(t *testing.T) {
	s, path := openTemp(t)
	first, err := s.NodeID()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 {
		t.Fatalf("node id %q should be 32 hex chars", first)
	}
	s.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second, err := reopened.NodeID()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("node id changed after reopen: %s -> %s", first, second)
	}
}

func TestPutGet(t *testing.T) {
	s, _ := openTemp(t)
	rec := Record{Path: "dir/a.txt", Version: vv.Vector{"n1": 2, "n2": 1}, Hash: "abc", Size: 10, MTime: 99}
	if err := s.Put(rec, nil); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Get("dir/a.txt")
	if err != nil || !found {
		t.Fatalf("Get: found=%v err=%v", found, err)
	}
	if got.Hash != "abc" || got.Size != 10 || got.MTime != 99 || got.Deleted {
		t.Errorf("unexpected record %+v", got)
	}
	if vv.Compare(got.Version, rec.Version) != vv.Equal {
		t.Errorf("version = %v, want %v", got.Version, rec.Version)
	}

	if _, found, _ := s.Get("missing"); found {
		t.Error("missing path should not be found")
	}
}

func TestPutReplacesRecordAndBlocks(t *testing.T) {
	s, _ := openTemp(t)
	old := []chunk.Block{{Hash: "h1", Size: chunk.Size}, {Hash: "h2", Size: 5}}
	if err := s.Put(Record{Path: "a", Version: vv.Vector{"n1": 1}, Hash: "x"}, old); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(Record{Path: "a", Version: vv.Vector{"n1": 2}, Deleted: true}, nil); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.Get("a")
	if !got.Deleted || got.Version["n1"] != 2 {
		t.Errorf("record not replaced: %+v", got)
	}
	blocks, err := s.Blocks("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 0 {
		t.Errorf("old blocks still present: %v", blocks)
	}
}

func TestBlocksAndFindBlock(t *testing.T) {
	s, _ := openTemp(t)
	blocks := []chunk.Block{{Hash: "h1", Size: chunk.Size}, {Hash: "h2", Size: chunk.Size}, {Hash: "h3", Size: 7}}
	if err := s.Put(Record{Path: "big", Version: vv.Vector{"n1": 1}, Hash: "x"}, blocks); err != nil {
		t.Fatal(err)
	}
	got, err := s.Blocks("big")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != blocks[0] || got[2] != blocks[2] {
		t.Errorf("Blocks = %v", got)
	}

	path, offset, size, found, err := s.FindBlock("h3")
	if err != nil || !found {
		t.Fatalf("FindBlock: found=%v err=%v", found, err)
	}
	if path != "big" || offset != 2*chunk.Size || size != 7 {
		t.Errorf("FindBlock = %s %d %d", path, offset, size)
	}
	if _, _, _, found, _ := s.FindBlock("nope"); found {
		t.Error("unknown hash should not be found")
	}
}

func TestAllSortedByPath(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"b", "a/z", "a"} {
		if err := s.Put(Record{Path: p, Version: vv.Vector{"n1": 1}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	records, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || records[0].Path != "a" || records[1].Path != "a/z" || records[2].Path != "b" {
		t.Errorf("All = %v", records)
	}
}

func TestSetMTime(t *testing.T) {
	s, _ := openTemp(t)
	s.Put(Record{Path: "a", Version: vv.Vector{"n1": 1}, MTime: 1}, nil)
	if err := s.SetMTime("a", 42); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.Get("a")
	if got.MTime != 42 || got.Version["n1"] != 1 {
		t.Errorf("SetMTime changed more than mtime: %+v", got)
	}
}

func TestSetVersion(t *testing.T) {
	s, _ := openTemp(t)
	s.Put(Record{Path: "a", Version: vv.Vector{"n1": 1}, Hash: "x", MTime: 5}, []chunk.Block{{Hash: "h", Size: 1}})
	if err := s.SetVersion("a", vv.Vector{"n1": 1, "n2": 3}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.Get("a")
	if got.Version["n2"] != 3 || got.Hash != "x" || got.MTime != 5 {
		t.Errorf("SetVersion changed more than version: %+v", got)
	}
	if blocks, _ := s.Blocks("a"); len(blocks) != 1 {
		t.Errorf("SetVersion dropped blocks: %v", blocks)
	}
}

func TestPeersPersist(t *testing.T) {
	s, path := openTemp(t)
	for _, p := range []Peer{{Addr: "b:1"}, {Addr: "a:1", ID: "id-a"}, {Addr: "b:1", ID: "id-b"}} {
		if err := s.SavePeer(p); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	peers, err := reopened.Peers()
	if err != nil {
		t.Fatal(err)
	}
	want := []Peer{{Addr: "a:1", ID: "id-a"}, {Addr: "b:1", ID: "id-b"}}
	if !slices.Equal(peers, want) {
		t.Errorf("Peers = %v, want %v", peers, want)
	}
}
