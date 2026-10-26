package node

import (
	"testing"
	"time"

	"p2psync/internal/membership"
)

func newSeededCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := emptyCluster(t, size, func(*Config) {})
	for i, s := range c.slots {
		cfg := fastConfig(s, "127.0.0.1:0", nil)
		cfg.Seed = c.seed
		n, err := Start(cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.node = n
		s.addr = n.Addr
		if i == 0 {
			c.seed = n.Addr
		}
	}
	return c
}

func (c *cluster) idOf(i, j int) string {
	for _, m := range c.slots[i].node.Members.Members() {
		if m.Addr == c.slots[j].addr {
			return m.ID
		}
	}
	return ""
}

func TestDiscoveryFromSeed(t *testing.T) {
	c := newSeededCluster(t, 5)
	c.waitAllAlive()
	for i := range c.slots {
		for j := range c.slots {
			if i != j && c.idOf(i, j) != c.slots[j].node.ID {
				t.Errorf("node %d has id %q for node %d, want %q", i, c.idOf(i, j), j, c.slots[j].node.ID)
			}
		}
	}

	c.write(4, "from-last.txt", []byte("hello"))
	if err := c.waitConverged(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := c.read(1, "from-last.txt"); got != "hello" {
		t.Errorf("node 1 has %q", got)
	}
}

func TestRejoinWithoutSeed(t *testing.T) {
	c := newSeededCluster(t, 3)
	c.waitAllAlive()

	c.stop(0)
	c.stop(2)
	c.start(2)
	waitFor(t, 5*time.Second, "node 2 and node 1 alive to each other", func() bool {
		return c.stateOf(2, 1) == membership.Alive && c.stateOf(1, 2) == membership.Alive
	})

	c.write(2, "after-restart.txt", []byte("still syncing"))
	waitFor(t, 5*time.Second, "file on node 1", func() bool {
		return c.read(1, "after-restart.txt") == "still syncing"
	})
}
