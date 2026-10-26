package node

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/scan"
	"p2psync/internal/transport"
)

const (
	clusterSize = 5
	steps       = 60
)

var paths = []string{
	"a.txt", "b.txt", "c.txt", "notes.md", "big.bin",
	"docs/d.txt", "docs/e.txt", "docs/deep/f.txt", "photos/g.jpg", "photos/h.jpg",
}

func seedCount(t *testing.T) int {
	value := os.Getenv("CONVERGENCE_SEEDS")
	if value == "" {
		return 10
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("CONVERGENCE_SEEDS=%q is not a number", value)
	}
	return n
}

func TestConvergence(t *testing.T) {
	first := uint64(1)
	if value := os.Getenv("CONVERGENCE_FIRST_SEED"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		first = parsed
	}
	for seed := first; seed < first+uint64(seedCount(t)); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runConvergence(t, seed)
		})
	}
}

func randomContent(rng *rand.Rand, seed uint64, step int) []byte {
	if rng.IntN(8) == 0 {
		data := make([]byte, chunk.Size+rng.IntN(2*chunk.Size))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		return data
	}
	return []byte(fmt.Sprintf("seed %d step %d value %d\n", seed, step, rng.Int()))
}

func randomSide(rng *rand.Rand) []int {
	var side []int
	for len(side) == 0 || len(side) == clusterSize {
		side = side[:0]
		for i := 0; i < clusterSize; i++ {
			if rng.IntN(2) == 0 {
				side = append(side, i)
			}
		}
	}
	return side
}

func runConvergence(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed))
	c := newCluster(t, clusterSize)
	var log []string
	record := func(format string, args ...any) {
		log = append(log, fmt.Sprintf(format, args...))
	}

	for step := 0; step < steps; step++ {
		i := rng.IntN(clusterSize)
		p := paths[rng.IntN(len(paths))]
		switch action := rng.IntN(20); {
		case action < 9:
			c.write(i, p, randomContent(rng, seed, step))
			record("%d: node %d writes %s", step, i, p)
		case action < 13:
			c.remove(i, p)
			record("%d: node %d deletes %s", step, i, p)
		case action < 15:
			side := randomSide(rng)
			c.partition(side)
			record("%d: partition %v from the rest", step, side)
		case action < 17:
			c.heal()
			record("%d: heal", step)
		case action < 18:
			j := (i + 1 + rng.IntN(clusterSize-1)) % clusterSize
			delay := rng.IntN(100)
			c.setRule(i, j, transport.Rule{DelayMs: delay})
			record("%d: delay %dms between %d and %d", step, delay, i, j)
		default:
			if c.running(i) {
				c.stop(i)
				record("%d: node %d crashes", step, i)
			} else {
				c.start(i)
				record("%d: node %d restarts", step, i)
			}
		}
		time.Sleep(time.Duration(rng.IntN(30)) * time.Millisecond)
	}

	c.heal()
	for i := range c.slots {
		c.start(i)
	}
	record("end: heal, all nodes running")

	if err := c.waitConverged(30 * time.Second); err != nil {
		t.Fatalf("seed %d did not converge: %v\nactions:\n%s", seed, err, strings.Join(log, "\n"))
	}
	checkConflictNames(t, c)
	checkNoTempFiles(t, c)
}

func checkConflictNames(t *testing.T, c *cluster) {
	t.Helper()
	files, err := folderHashes(c.slots[0].dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range conflictCopies(files) {
		marker := strings.LastIndex(name, ".conflict-")
		prefix := name[marker+len(".conflict-"):]
		prefix = strings.TrimSuffix(prefix, path.Ext(prefix))
		if !strings.HasPrefix(files[name], prefix) {
			t.Errorf("conflict copy %s does not match its content hash %s", name, files[name])
		}
	}
}

func checkNoTempFiles(t *testing.T, c *cluster) {
	t.Helper()
	for i, s := range c.slots {
		entries, err := os.ReadDir(filepath.Join(s.dir, scan.MetaDir, "tmp"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("node %d has leftover temp files: %v", i, entries)
		}
	}
}
