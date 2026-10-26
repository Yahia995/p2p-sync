package syncer

import (
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/scan"
	"p2psync/internal/store"
	"p2psync/internal/vv"
)

func TestBeatsIsOrderIndependent(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	hashes := []string{chunk.Hash([]byte("x")), chunk.Hash([]byte("y")), chunk.Hash([]byte("z"))}
	randomRecord := func() store.Record {
		if rng.IntN(3) == 0 {
			return store.Record{Deleted: true}
		}
		return store.Record{Hash: hashes[rng.IntN(len(hashes))]}
	}
	pick := func(a, b store.Record) store.Record {
		if beats(b, a) {
			return b
		}
		return a
	}
	orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for i := 0; i < 500; i++ {
		records := []store.Record{randomRecord(), randomRecord(), randomRecord()}
		var winners []store.Record
		for _, o := range orders {
			winners = append(winners, pick(pick(records[o[0]], records[o[1]]), records[o[2]]))
		}
		for _, w := range winners[1:] {
			if w.Deleted != winners[0].Deleted || w.Hash != winners[0].Hash {
				t.Fatalf("winner depends on order for %+v", records)
			}
		}
	}
}

func TestEditBeatsDelete(t *testing.T) {
	edit := store.Record{Hash: "0000"}
	tombstone := store.Record{Deleted: true}
	if !beats(edit, tombstone) || beats(tombstone, edit) {
		t.Error("an edit must beat a delete")
	}
}

func TestConflictName(t *testing.T) {
	hash := "abcdef0123456789"
	tests := map[string]string{
		"a.txt":         "a.conflict-abcdef01.txt",
		"dir/sub/a.txt": "dir/sub/a.conflict-abcdef01.txt",
		"dir/a.tar.gz":  "dir/a.tar.conflict-abcdef01.gz",
		"noext":         "noext.conflict-abcdef01",
		".hidden":       ".hidden.conflict-abcdef01",
		"dir/.hidden":   "dir/.hidden.conflict-abcdef01",
	}
	for in, want := range tests {
		if got := conflictName(in, hash); got != want {
			t.Errorf("conflictName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidRecord(t *testing.T) {
	good := chunk.Hash([]byte("x"))
	if !validRecord(store.Record{Path: "a", Hash: good}) {
		t.Error("valid file rejected")
	}
	if !validRecord(store.Record{Path: "a", Deleted: true}) {
		t.Error("valid tombstone rejected")
	}
	if validRecord(store.Record{Path: "a", Hash: "short"}) {
		t.Error("short hash accepted")
	}
	if validRecord(store.Record{Path: "a", Deleted: true, Hash: good}) {
		t.Error("tombstone with hash accepted")
	}
	if validRecord(store.Record{Path: "../a", Hash: good}) {
		t.Error("escaping path accepted")
	}
}

func folder(t *testing.T, n *testNode) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(n.root, func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(n.root, full)
		if d.IsDir() && rel == scan.MetaDir {
			return filepath.SkipDir
		}
		if d.Type().IsRegular() {
			data, err := os.ReadFile(full)
			if err != nil {
				return err
			}
			files[filepath.ToSlash(rel)] = string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func index(t *testing.T, n *testNode) map[string]string {
	t.Helper()
	records, err := n.store.All()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range records {
		state := "file " + r.Hash
		if r.Deleted {
			state = "deleted"
		}
		out[r.Path] = vv.Encode(r.Version) + " " + state
	}
	return out
}

func syncAll(t *testing.T, nodes ...*testNode) {
	t.Helper()
	for round := 0; round < 3; round++ {
		for _, a := range nodes {
			for _, b := range nodes {
				if a != b {
					a.pull(t, b)
				}
			}
		}
	}
}

func assertConverged(t *testing.T, nodes ...*testNode) map[string]string {
	t.Helper()
	first := folder(t, nodes[0])
	firstIndex := index(t, nodes[0])
	for _, n := range nodes[1:] {
		if got := folder(t, n); !maps.Equal(got, first) {
			t.Fatalf("folders differ:\n%v\n%v", first, got)
		}
		if got := index(t, n); !maps.Equal(got, firstIndex) {
			t.Fatalf("indexes differ:\n%v\n%v", firstIndex, got)
		}
	}
	return first
}

func conflictFiles(files map[string]string) []string {
	var names []string
	for p := range files {
		if strings.Contains(p, ".conflict-") {
			names = append(names, p)
		}
	}
	slices.Sort(names)
	return names
}

func editAndScan(t *testing.T, n *testNode, path, content string) {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	n.write(t, path, []byte(content))
	n.rescan(t)
}

func higherHash(a, b string) string {
	if chunk.Hash([]byte(a)) > chunk.Hash([]byte(b)) {
		return a
	}
	return b
}

func TestConcurrentEditsKeepBoth(t *testing.T) {
	for _, order := range []string{"a pulls first", "b pulls first", "both resolve"} {
		t.Run(order, func(t *testing.T) {
			a, b := startNode(t), startNode(t)
			editAndScan(t, a, "report.txt", "original")
			b.pull(t, a)

			editAndScan(t, a, "report.txt", "edit from a")
			editAndScan(t, b, "report.txt", "edit from b, longer")

			switch order {
			case "a pulls first":
				a.pull(t, b)
				b.pull(t, a)
			case "b pulls first":
				b.pull(t, a)
				a.pull(t, b)
			case "both resolve":
				relay := startNode(t)
				relay.pull(t, b)
				b.pull(t, a)
				a.pull(t, relay)
				if !maps.Equal(index(t, a), index(t, b)) || !maps.Equal(folder(t, a), folder(t, b)) {
					t.Fatalf("independent resolutions differ:\n%v\n%v", index(t, a), index(t, b))
				}
			}
			syncAll(t, a, b)

			files := assertConverged(t, a, b)
			winner := higherHash("edit from a", "edit from b, longer")
			loser := "edit from a"
			if winner == loser {
				loser = "edit from b, longer"
			}
			if files["report.txt"] != winner {
				t.Errorf("report.txt = %q, want winner %q", files["report.txt"], winner)
			}
			copies := conflictFiles(files)
			want := conflictName("report.txt", chunk.Hash([]byte(loser)))
			if !slices.Equal(copies, []string{want}) {
				t.Fatalf("conflict copies = %v, want [%s]", copies, want)
			}
			if files[want] != loser {
				t.Errorf("conflict copy = %q, want %q", files[want], loser)
			}
			for _, n := range []*testNode{a, b} {
				if changed := n.rescan(t); len(changed) != 0 {
					t.Errorf("rescan after resolution created versions: %v", changed)
				}
			}
		})
	}
}

func TestThreeWayConflictConverges(t *testing.T) {
	contents := []string{"version one", "version two", "version three"}
	for _, order := range [][3]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}} {
		nodes := []*testNode{startNode(t), startNode(t), startNode(t)}
		editAndScan(t, nodes[0], "f.txt", "base")
		syncAll(t, nodes...)
		for i, n := range nodes {
			editAndScan(t, n, "f.txt", contents[i])
		}

		x, y, z := nodes[order[0]], nodes[order[1]], nodes[order[2]]
		x.pull(t, y)
		z.pull(t, x)
		y.pull(t, z)
		syncAll(t, nodes...)

		files := assertConverged(t, nodes...)
		best := higherHash(higherHash(contents[0], contents[1]), contents[2])
		if files["f.txt"] != best {
			t.Errorf("order %v: f.txt = %q, want %q", order, files["f.txt"], best)
		}
		var wantCopies []string
		for _, c := range contents {
			if c != best {
				wantCopies = append(wantCopies, conflictName("f.txt", chunk.Hash([]byte(c))))
			}
		}
		slices.Sort(wantCopies)
		if got := conflictFiles(files); !slices.Equal(got, wantCopies) {
			t.Errorf("order %v: conflict copies = %v, want %v", order, got, wantCopies)
		}
	}
}

func TestDeleteVersusEdit(t *testing.T) {
	for _, deleterPullsFirst := range []bool{true, false} {
		a, b := startNode(t), startNode(t)
		editAndScan(t, a, "old.txt", "keep me")
		b.pull(t, a)

		os.Remove(filepath.Join(a.root, "old.txt"))
		a.rescan(t)
		editAndScan(t, b, "old.txt", "edited while deleted elsewhere")

		if deleterPullsFirst {
			a.pull(t, b)
		} else {
			b.pull(t, a)
		}
		syncAll(t, a, b)

		files := assertConverged(t, a, b)
		if files["old.txt"] != "edited while deleted elsewhere" {
			t.Errorf("old.txt = %q, edit should win", files["old.txt"])
		}
		if copies := conflictFiles(files); len(copies) != 0 {
			t.Errorf("unexpected conflict copies %v", copies)
		}
	}
}

func TestBothDelete(t *testing.T) {
	a, b := startNode(t), startNode(t)
	editAndScan(t, a, "gone.txt", "bye")
	b.pull(t, a)
	os.Remove(filepath.Join(a.root, "gone.txt"))
	os.Remove(filepath.Join(b.root, "gone.txt"))
	a.rescan(t)
	b.rescan(t)

	syncAll(t, a, b)
	assertConverged(t, a, b)
	rec := a.record(t, "gone.txt")
	if !rec.Deleted || len(rec.Version) != 2 {
		t.Errorf("tombstones not merged: %+v", rec)
	}
}

func TestSameContentIsNotAConflict(t *testing.T) {
	a, b := startNode(t), startNode(t)
	editAndScan(t, a, "same.txt", "identical")
	editAndScan(t, b, "same.txt", "identical")

	syncAll(t, a, b)
	files := assertConverged(t, a, b)
	if copies := conflictFiles(files); len(copies) != 0 {
		t.Errorf("false conflict produced copies %v", copies)
	}
	if rec := a.record(t, "same.txt"); len(rec.Version) != 2 {
		t.Errorf("versions not merged: %v", rec.Version)
	}
}

func TestIndependentCreatesConflict(t *testing.T) {
	a, b := startNode(t), startNode(t)
	editAndScan(t, a, "new.txt", "created on a")
	editAndScan(t, b, "new.txt", "created on b")

	syncAll(t, a, b)
	files := assertConverged(t, a, b)
	if len(conflictFiles(files)) != 1 {
		t.Errorf("want exactly one conflict copy, got %v", conflictFiles(files))
	}
}

func TestDeletedConflictCopyStaysDeleted(t *testing.T) {
	a, b, c := startNode(t), startNode(t), startNode(t)
	editAndScan(t, a, "f.txt", "base")
	syncAll(t, a, b, c)
	editAndScan(t, a, "f.txt", "edit a")
	editAndScan(t, b, "f.txt", "edit b")
	c.pull(t, a)

	a.pull(t, b)
	copies := conflictFiles(folder(t, a))
	if len(copies) != 1 {
		t.Fatalf("copies on a = %v", copies)
	}
	os.Remove(filepath.Join(a.root, filepath.FromSlash(copies[0])))
	a.rescan(t)

	b.pull(t, c)
	if got := conflictFiles(folder(t, b)); !slices.Equal(got, copies) {
		t.Fatalf("b should resolve the same conflict on its own, copies = %v", got)
	}
	syncAll(t, a, b, c)
	files := assertConverged(t, a, b, c)
	if got := conflictFiles(files); len(got) != 0 {
		t.Errorf("deleted conflict copy came back: %v", got)
	}
}

func TestUnscannedEditBlocksConflictResolution(t *testing.T) {
	a, b := startNode(t), startNode(t)
	editAndScan(t, a, "f.txt", "base")
	b.pull(t, a)
	editAndScan(t, a, "f.txt", "edit on a")
	editAndScan(t, b, "f.txt", "edit on b")
	time.Sleep(5 * time.Millisecond)
	b.write(t, "f.txt", []byte("newer edit on b, not scanned"))

	b.pull(t, a)
	if got := string(b.read(t, "f.txt")); got != "newer edit on b, not scanned" {
		t.Fatalf("unscanned edit overwritten: %q", got)
	}
	b.rescan(t)
	syncAll(t, a, b)
	files := assertConverged(t, a, b)
	found := false
	for _, content := range files {
		if content == "newer edit on b, not scanned" {
			found = true
		}
	}
	if !found {
		t.Errorf("unscanned edit lost: %v", files)
	}
}
