package node

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/membership"
	"p2psync/internal/scan"
	"p2psync/internal/transport"
)

func randomBytes(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed))
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rng.IntN(256))
	}
	return data
}

func TestStatusEndpoint(t *testing.T) {
	c := newCluster(t, 2)
	c.waitAllAlive()
	resp, err := http.Get("http://" + c.slots[0].addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got status
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ID != c.slots[0].node.ID || len(got.Members) != 1 || got.Members[0].State != membership.Alive {
		t.Errorf("status = %+v", got)
	}
}

func TestCrashDetectedAndRecovered(t *testing.T) {
	c := newCluster(t, 3)
	c.waitAllAlive()

	c.stop(2)
	waitFor(t, 2*time.Second, "node 2 suspected", func() bool {
		return c.stateOf(0, 2) != membership.Alive && c.stateOf(1, 2) != membership.Alive
	})
	waitFor(t, 2*time.Second, "node 2 dead", func() bool {
		return c.stateOf(0, 2) == membership.Dead && c.stateOf(1, 2) == membership.Dead
	})
	if alive := c.slots[0].node.Members.Alive(); len(alive) != 1 || alive[0] != c.slots[1].addr {
		t.Errorf("node 0 still syncs with a dead node: alive = %v", alive)
	}

	c.write(0, "while-down.txt", []byte("written while node 2 was down"))
	c.write(2, "offline-edit.txt", []byte("written on node 2 while it was stopped"))
	c.start(2)
	c.waitAllAlive()
	if err := c.waitConverged(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if c.read(1, "offline-edit.txt") != "written on node 2 while it was stopped" {
		t.Error("offline edit did not propagate")
	}
}

func TestPartitionThenHeal(t *testing.T) {
	c := newCluster(t, 4)
	c.waitAllAlive()
	c.write(0, "report.txt", []byte("base"))
	if err := c.waitConverged(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	c.partition([]int{0, 1})
	waitFor(t, 2*time.Second, "sides see each other dead", func() bool {
		return c.stateOf(0, 2) == membership.Dead && c.stateOf(3, 1) == membership.Dead && c.stateOf(0, 1) == membership.Alive
	})
	c.write(0, "report.txt", []byte("edit on side A"))
	c.write(3, "report.txt", []byte("edit on side B, longer"))
	waitFor(t, 5*time.Second, "edits spread inside each side", func() bool {
		return c.read(1, "report.txt") == "edit on side A" && c.read(2, "report.txt") == "edit on side B, longer"
	})

	c.heal()
	if err := c.waitConverged(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	files, _ := folderHashes(c.slots[0].dir)
	if copies := conflictCopies(files); len(copies) != 1 {
		t.Errorf("want one conflict copy after healing, got %v", copies)
	}
}

func TestConvergesWithDelay(t *testing.T) {
	c := newCluster(t, 3)
	for i := range c.slots {
		for j := range c.slots {
			if i < j {
				c.setRule(i, j, transport.Rule{DelayMs: 100})
			}
		}
	}
	c.waitAllAlive()
	c.write(0, "big.bin", randomBytes(1, 3*chunk.Size))
	c.write(1, "small.txt", []byte("small"))
	c.write(2, "docs/other.txt", []byte("other"))
	if err := c.waitConverged(20 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestSourceCrashDuringDownload(t *testing.T) {
	c := newCluster(t, 2)
	c.waitAllAlive()
	c.setRule(0, 1, transport.Rule{DelayMs: 20})
	data := randomBytes(2, 40*chunk.Size)
	c.write(0, "big.bin", data)

	tmp := filepath.Join(c.slots[1].dir, scan.MetaDir, "tmp")
	waitFor(t, 10*time.Second, "download in progress on node 1", func() bool {
		entries, _ := os.ReadDir(tmp)
		return len(entries) > 0
	})
	c.stop(0)

	waitFor(t, 5*time.Second, "temp file removed after failed download", func() bool {
		entries, _ := os.ReadDir(tmp)
		return len(entries) == 0
	})
	if _, err := os.Stat(filepath.Join(c.slots[1].dir, "big.bin")); err == nil {
		t.Fatal("partial file installed")
	}

	c.heal()
	c.start(0)
	if err := c.waitConverged(20 * time.Second); err != nil {
		t.Fatal(err)
	}
}
