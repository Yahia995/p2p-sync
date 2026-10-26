package node

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/membership"
	"p2psync/internal/scan"
	"p2psync/internal/transport"
	"p2psync/internal/vv"
)

type slot struct {
	dir    string
	addr   string
	faults *transport.Faults
	node   *Node
}

type cluster struct {
	t         *testing.T
	slots     []*slot
	configure func(*Config)
}

func fastConfig(s *slot, listen string, peers []string) Config {
	cfg := Defaults()
	cfg.Dir = s.dir
	cfg.Listen = listen
	cfg.Peers = peers
	cfg.Faults = s.faults
	cfg.Logger = log.New(io.Discard, "", 0)
	cfg.StableDelay = 20 * time.Millisecond
	cfg.Debounce = 20 * time.Millisecond
	cfg.RescanEvery = 500 * time.Millisecond
	cfg.AntiEntropyEvery = 50 * time.Millisecond
	cfg.HeartbeatEvery = 50 * time.Millisecond
	cfg.SuspectAfter = 200 * time.Millisecond
	cfg.DeadAfter = 500 * time.Millisecond
	cfg.RequestTimeout = 2 * time.Second
	return cfg
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	return newClusterWith(t, size, func(*Config) {})
}

func newClusterWith(t *testing.T, size int, configure func(*Config)) *cluster {
	t.Helper()
	c := &cluster{t: t, configure: configure}
	for i := 0; i < size; i++ {
		c.slots = append(c.slots, &slot{dir: t.TempDir(), faults: transport.NewFaults()})
	}
	t.Cleanup(func() {
		for i := range c.slots {
			c.stop(i)
		}
	})
	for _, s := range c.slots {
		cfg := fastConfig(s, "127.0.0.1:0", nil)
		configure(&cfg)
		n, err := Start(cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.node = n
		s.addr = n.Addr
	}
	for _, s := range c.slots {
		for _, other := range c.slots {
			s.node.Members.AddPeer(other.addr)
		}
	}
	return c
}

func (c *cluster) peersOf(i int) []string {
	var peers []string
	for j, s := range c.slots {
		if j != i {
			peers = append(peers, s.addr)
		}
	}
	return peers
}

func (c *cluster) running(i int) bool {
	return c.slots[i].node != nil
}

func (c *cluster) stop(i int) {
	if s := c.slots[i]; s.node != nil {
		s.node.Stop()
		s.node = nil
	}
}

func (c *cluster) start(i int) {
	c.t.Helper()
	s := c.slots[i]
	if s.node != nil {
		return
	}
	cfg := fastConfig(s, s.addr, c.peersOf(i))
	c.configure(&cfg)
	n, err := Start(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	s.node = n
}

func (c *cluster) setRule(i, j int, rule transport.Rule) {
	c.slots[i].faults.Set(c.slots[j].addr, rule)
	c.slots[j].faults.Set(c.slots[i].addr, rule)
}

func (c *cluster) partition(side []int) {
	inSide := map[int]bool{}
	for _, i := range side {
		inSide[i] = true
	}
	for i := range c.slots {
		for j := range c.slots {
			if inSide[i] && !inSide[j] {
				c.setRule(i, j, transport.Rule{Drop: true})
			}
		}
	}
}

func (c *cluster) heal() {
	for _, s := range c.slots {
		s.faults.Clear()
	}
}

func (c *cluster) write(i int, path string, data []byte) {
	c.t.Helper()
	full := filepath.Join(c.slots[i].dir, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		c.t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		c.t.Fatal(err)
	}
}

func (c *cluster) remove(i int, path string) {
	os.Remove(filepath.Join(c.slots[i].dir, filepath.FromSlash(path)))
}

func (c *cluster) read(i int, path string) string {
	data, _ := os.ReadFile(filepath.Join(c.slots[i].dir, filepath.FromSlash(path)))
	return string(data)
}

func folderHashes(dir string) (map[string]string, error) {
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, full)
		if d.IsDir() && rel == scan.MetaDir {
			return filepath.SkipDir
		}
		if !d.Type().IsRegular() {
			return nil
		}
		_, hash, err := chunk.HashFile(full)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = hash
		return nil
	})
	return files, err
}

func (c *cluster) snapshot(i int) (folder, live, index map[string]string, err error) {
	folder, err = folderHashes(c.slots[i].dir)
	if err != nil {
		return nil, nil, nil, err
	}
	records, err := c.slots[i].node.Store.All()
	if err != nil {
		return nil, nil, nil, err
	}
	live, index = map[string]string{}, map[string]string{}
	for _, r := range records {
		index[r.Path] = fmt.Sprintf("%s deleted=%v %s", vv.Encode(r.Version), r.Deleted, r.Hash)
		if !r.Deleted {
			live[r.Path] = r.Hash
		}
	}
	return folder, live, index, nil
}

func (c *cluster) converged() error {
	var firstFolder, firstIndex map[string]string
	for i := range c.slots {
		folder, live, index, err := c.snapshot(i)
		if err != nil {
			return err
		}
		if !maps.Equal(folder, live) {
			return fmt.Errorf("node %d: disk and index disagree\ndisk:  %v\nindex: %v", i, folder, live)
		}
		if i == 0 {
			firstFolder, firstIndex = folder, index
			continue
		}
		if !maps.Equal(folder, firstFolder) || !maps.Equal(index, firstIndex) {
			return fmt.Errorf("node %d differs from node 0:%s", i, describeDiff(firstIndex, index))
		}
	}
	return nil
}

func describeDiff(a, b map[string]string) string {
	var out strings.Builder
	keys := slices.Sorted(maps.Keys(a))
	for p := range b {
		if _, ok := a[p]; !ok {
			keys = append(keys, p)
		}
	}
	for _, p := range keys {
		if a[p] != b[p] {
			fmt.Fprintf(&out, "\n  %s\n    node 0: %s\n    other:  %s", p, a[p], b[p])
		}
	}
	return out.String()
}

func (c *cluster) waitConverged(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := c.converged()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *cluster) stateOf(i, j int) string {
	for _, m := range c.slots[i].node.Members.Members() {
		if m.Addr == c.slots[j].addr {
			return m.State
		}
	}
	return "unknown"
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *cluster) waitAllAlive() {
	c.t.Helper()
	waitFor(c.t, 5*time.Second, "all members alive", func() bool {
		for i := range c.slots {
			for j := range c.slots {
				if i != j && c.running(i) && c.running(j) && c.stateOf(i, j) != membership.Alive {
					return false
				}
			}
		}
		return true
	})
}

func conflictCopies(files map[string]string) []string {
	var names []string
	for p := range files {
		if strings.Contains(p, ".conflict-") {
			names = append(names, p)
		}
	}
	slices.Sort(names)
	return names
}
