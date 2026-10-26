package syncer

import (
	"fmt"
	"maps"
	"testing"
)

func (n *testNode) pullMerkle(t *testing.T, from *testNode) {
	t.Helper()
	if err := n.syncer.SyncMerkle(from.addr); err != nil {
		t.Fatal(err)
	}
}

func TestMerkleFullSync(t *testing.T) {
	a, b := startNode(t), startNode(t)
	for i := 0; i < 40; i++ {
		a.write(t, fmt.Sprintf("dir%d/file%d.txt", i%5, i), []byte(fmt.Sprintf("content %d", i)))
	}
	a.rescan(t)

	b.pullMerkle(t, a)
	if !maps.Equal(index(t, a), index(t, b)) || !maps.Equal(folder(t, a), folder(t, b)) {
		t.Fatal("nodes differ after a Merkle sync")
	}
}

func TestMerkleInSyncCostsOneRequest(t *testing.T) {
	a, b := startNode(t), startNode(t)
	for i := 0; i < 40; i++ {
		a.write(t, fmt.Sprintf("file%d.txt", i), []byte(fmt.Sprintf("content %d", i)))
	}
	a.rescan(t)
	b.pullMerkle(t, a)

	a.resetCounts()
	b.pullMerkle(t, a)
	if got := a.served("/merkle"); got != 1 {
		t.Errorf("in-sync round used %d /merkle requests, want 1", got)
	}
	if got := a.served("/index") + a.served("/file") + a.served("/block"); got != 0 {
		t.Errorf("in-sync round made %d other requests", got)
	}
}

func TestMerkleOneChangeVisitsOnePath(t *testing.T) {
	a, b := startNode(t), startNode(t)
	for i := 0; i < 40; i++ {
		a.write(t, fmt.Sprintf("file%d.txt", i), []byte(fmt.Sprintf("content %d", i)))
	}
	a.rescan(t)
	b.pullMerkle(t, a)

	editAndScan(t, a, "file7.txt", "changed")
	a.resetCounts()
	b.pullMerkle(t, a)
	if got := a.served("/merkle"); got != 3 {
		t.Errorf("one changed file used %d /merkle requests, want 3 (root, inner, leaf)", got)
	}
	if got := a.served("/file"); got != 1 {
		t.Errorf("one changed file caused %d /file requests, want 1", got)
	}
	if string(b.read(t, "file7.txt")) != "changed" {
		t.Error("change not applied")
	}
	sameRecord(t, a, b, "file7.txt")
}

func TestMerkleHandlesConflicts(t *testing.T) {
	a, b := startNode(t), startNode(t)
	editAndScan(t, a, "f.txt", "base")
	b.pullMerkle(t, a)
	editAndScan(t, a, "f.txt", "edit on a")
	editAndScan(t, b, "f.txt", "edit on b")

	for round := 0; round < 3; round++ {
		a.pullMerkle(t, b)
		b.pullMerkle(t, a)
	}
	files := assertConverged(t, a, b)
	if len(conflictFiles(files)) != 1 {
		t.Errorf("conflict copies = %v", conflictFiles(files))
	}
}

func TestMerkleRejectsBadNodeID(t *testing.T) {
	a := startNode(t)
	if _, err := a.syncer.Client.Merkle(a.addr, "xyz"); err == nil {
		t.Error("invalid node id accepted")
	}
}
