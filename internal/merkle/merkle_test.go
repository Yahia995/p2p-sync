package merkle

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"p2psync/internal/store"
	"p2psync/internal/vv"
)

func sampleRecords(n int) []store.Record {
	var records []store.Record
	for i := 0; i < n; i++ {
		records = append(records, store.Record{
			Path:    fmt.Sprintf("dir%d/file%d.txt", i%7, i),
			Version: vv.Vector{"n1": uint64(i + 1), "n2": 3},
			Hash:    fmt.Sprintf("%064x", i),
			Size:    int64(i),
		})
	}
	return records
}

func allIDs() []string {
	ids := []string{""}
	for _, inner := range Children("") {
		ids = append(ids, inner)
		ids = append(ids, Children(inner)...)
	}
	return ids
}

func differingIDs(a, b *Tree) []string {
	var diff []string
	for _, id := range allIDs() {
		if a.Hash(id) != b.Hash(id) {
			diff = append(diff, id)
		}
	}
	return diff
}

func TestBucket(t *testing.T) {
	b := Bucket("docs/a.txt")
	if len(b) != 2 || !ValidID(b) || !IsLeaf(b) {
		t.Fatalf("Bucket = %q", b)
	}
	if Bucket("docs/a.txt") != b {
		t.Error("Bucket is not deterministic")
	}
}

func TestValidID(t *testing.T) {
	for _, id := range []string{"", "a", "0f", "ff"} {
		if !ValidID(id) {
			t.Errorf("%q should be valid", id)
		}
	}
	for _, id := range []string{"abc", "g", "A", "0G", "../"} {
		if ValidID(id) {
			t.Errorf("%q should be invalid", id)
		}
	}
}

func TestRootIndependentOfOrder(t *testing.T) {
	records := sampleRecords(200)
	shuffled := append([]store.Record(nil), records...)
	rng := rand.New(rand.NewPCG(1, 1))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	a, b := Build(records), Build(shuffled)
	if a.Hash("") != b.Hash("") {
		t.Fatal("root depends on record order")
	}
	if diff := differingIDs(a, b); len(diff) != 0 {
		t.Errorf("nodes differ: %v", diff)
	}
}

func TestEmptyTreesMatch(t *testing.T) {
	if Build(nil).Hash("") != Build([]store.Record{}).Hash("") {
		t.Error("empty trees differ")
	}
	if Build(nil).Hash("") == Build(sampleRecords(1)).Hash("") {
		t.Error("empty and non-empty trees match")
	}
}

func TestOneChangeTouchesOnePath(t *testing.T) {
	changes := map[string]func(*store.Record){
		"version": func(r *store.Record) { r.Version = vv.Bump(r.Version, "n3") },
		"hash":    func(r *store.Record) { r.Hash = fmt.Sprintf("%064x", 999999) },
		"deleted": func(r *store.Record) { r.Deleted, r.Hash = true, "" },
	}
	for name, change := range changes {
		records := sampleRecords(200)
		before := Build(records)
		changed := append([]store.Record(nil), records...)
		change(&changed[42])
		after := Build(changed)

		leaf := Bucket(changed[42].Path)
		want := []string{"", leaf[:1], leaf}
		if diff := differingIDs(before, after); fmt.Sprint(diff) != fmt.Sprint(want) {
			t.Errorf("%s change: differing nodes = %v, want %v", name, diff, want)
		}
	}
}

func TestLocalFieldsIgnored(t *testing.T) {
	records := sampleRecords(10)
	changed := append([]store.Record(nil), records...)
	changed[3].MTime = 123456
	if Build(records).Hash("") != Build(changed).Hash("") {
		t.Error("mtime is local and must not affect the tree")
	}
}

func TestNode(t *testing.T) {
	records := sampleRecords(50)
	tree := Build(records)

	root := tree.Node("")
	if root.Hash != tree.Hash("") || len(root.Children) != 16 || root.Records != nil {
		t.Errorf("root node = %+v", root)
	}
	if root.Children[3] != tree.Hash("3") {
		t.Error("root child hash mismatch")
	}

	leafID := Bucket(records[0].Path)
	leaf := tree.Node(leafID)
	if leaf.Children != nil || len(leaf.Records) == 0 {
		t.Fatalf("leaf node = %+v", leaf)
	}
	for _, r := range leaf.Records {
		if Bucket(r.Path) != leafID {
			t.Errorf("record %s in wrong bucket %s", r.Path, leafID)
		}
	}
}
