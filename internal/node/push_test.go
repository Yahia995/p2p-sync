package node

import (
	"testing"
	"time"

	"p2psync/internal/transport"
)

func withoutAntiEntropy(cfg *Config) {
	cfg.AntiEntropyEvery = time.Hour
}

func TestPushDeliversWithoutAntiEntropy(t *testing.T) {
	c := newClusterWith(t, 3, withoutAntiEntropy)
	c.waitAllAlive()

	c.write(0, "pushed.txt", []byte("arrives by push only"))
	waitFor(t, 3*time.Second, "push to reach nodes 1 and 2", func() bool {
		return c.read(1, "pushed.txt") == "arrives by push only" && c.read(2, "pushed.txt") == "arrives by push only"
	})

	c.remove(1, "pushed.txt")
	waitFor(t, 3*time.Second, "pushed delete to reach nodes 0 and 2", func() bool {
		return c.read(0, "pushed.txt") == "" && c.read(2, "pushed.txt") == ""
	})
	if err := c.waitConverged(3 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestReceivedChangesAreNotPushedAgain(t *testing.T) {
	c := newClusterWith(t, 3, withoutAntiEntropy)
	c.waitAllAlive()

	c.write(0, "once.txt", []byte("pushed once"))
	waitFor(t, 3*time.Second, "push to reach nodes 1 and 2", func() bool {
		return c.read(1, "once.txt") != "" && c.read(2, "once.txt") != ""
	})
	time.Sleep(300 * time.Millisecond)

	pushes := []int64{
		c.slots[0].node.pushesReceived.Load(),
		c.slots[1].node.pushesReceived.Load(),
		c.slots[2].node.pushesReceived.Load(),
	}
	if pushes[0] != 0 || pushes[1] != 1 || pushes[2] != 1 {
		t.Errorf("pushes received = %v, want [0 1 1]: receivers must not forward", pushes)
	}
}

func TestLostPushRepairedByAntiEntropy(t *testing.T) {
	c := newCluster(t, 2)
	c.waitAllAlive()
	c.slots[1].faults.Set(c.slots[0].addr, transport.Rule{Drop: true})
	c.write(0, "lost.txt", []byte("the push for this file is dropped"))
	time.Sleep(300 * time.Millisecond)
	if c.read(1, "lost.txt") != "" {
		t.Fatal("file crossed a dropped link")
	}

	c.heal()
	if err := c.waitConverged(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}
