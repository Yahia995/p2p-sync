package node

import (
	"testing"
	"time"

	"p2psync/internal/membership"
	"p2psync/internal/transport"
)

func (c *cluster) memberOf(i, j int) membership.Member {
	for _, m := range c.slots[i].node.Members.Members() {
		if m.Addr == c.slots[j].addr {
			return m
		}
	}
	return membership.Member{}
}

func TestIndirectPingKeepsCutPeerAlive(t *testing.T) {
	c := newCluster(t, 3)
	c.waitAllAlive()
	c.setRule(0, 2, transport.Rule{Drop: true})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.stateOf(0, 2) == membership.Dead || c.stateOf(2, 0) == membership.Dead {
			t.Fatal("a peer reachable through node 1 was declared dead")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if inc := c.slots[2].node.Members.Incarnation(); inc != 0 {
		t.Errorf("node 2 had to refute %d suspicions; ping-req through node 1 should prevent them", inc)
	}
	if inc := c.slots[0].node.Members.Incarnation(); inc != 0 {
		t.Errorf("node 0 had to refute %d suspicions", inc)
	}
}

func TestDeadDetectedByAll(t *testing.T) {
	c := newCluster(t, 5)
	c.waitAllAlive()
	stopped := time.Now()
	c.stop(4)
	waitFor(t, 2*time.Second, "every node to mark node 4 dead", func() bool {
		for i := 0; i < 4; i++ {
			if c.stateOf(i, 4) != membership.Dead {
				return false
			}
		}
		return true
	})
	t.Logf("all 4 nodes marked node 4 dead after %v", time.Since(stopped).Round(10*time.Millisecond))
	for i := 0; i < 4; i++ {
		if alive := c.slots[i].node.Members.Alive(); len(alive) != 3 {
			t.Errorf("node %d alive members = %v", i, alive)
		}
	}
}

func TestFalseSuspicionRefuted(t *testing.T) {
	c := newCluster(t, 3)
	c.waitAllAlive()
	rumor := c.memberOf(0, 1)
	rumor.State = membership.Suspect

	client := transport.NewClient("test", time.Second, transport.NewFaults())
	if err := client.PostJSON(c.slots[0].addr, "/gossip", []membership.Member{rumor}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, "node 1 to refute", func() bool {
		return c.slots[1].node.Members.Incarnation() > rumor.Incarnation &&
			c.memberOf(0, 1).Incarnation > rumor.Incarnation && c.stateOf(0, 1) == membership.Alive &&
			c.memberOf(2, 1).Incarnation > rumor.Incarnation && c.stateOf(2, 1) == membership.Alive
	})
}

func membershipRate(t *testing.T, size int) float64 {
	c := newCluster(t, size)
	c.waitAllAlive()
	window := time.Second
	before := make([]int64, size)
	for i, s := range c.slots {
		before[i] = s.node.Members.MessagesSent()
	}
	time.Sleep(window)
	var total int64
	for i, s := range c.slots {
		total += s.node.Members.MessagesSent() - before[i]
	}
	periods := float64(window) / float64(fastConfig(c.slots[0], "", nil).ProbeEvery)
	return float64(total) / float64(size) / periods
}

func TestMembershipCostConstantPerNode(t *testing.T) {
	small := membershipRate(t, 3)
	large := membershipRate(t, 8)
	t.Logf("membership messages per node per period: 3 nodes %.2f, 8 nodes %.2f", small, large)
	if large > small*1.5 {
		t.Errorf("per-node cost grew from %.2f to %.2f; all-to-all would be 2 -> 7", small, large)
	}
}
