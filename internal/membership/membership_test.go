package membership

import (
	"slices"
	"testing"
	"time"
)

func newList(peers ...string) *List {
	return New("me", "me:1", peers, nil, 3*time.Second, 10*time.Second)
}

func state(l *List, addr string) string {
	for _, m := range l.Members() {
		if m.Addr == addr {
			return m.State
		}
	}
	return "missing"
}

func TestPeersStartDead(t *testing.T) {
	l := newList("a:1", "b:1")
	if state(l, "a:1") != Dead || state(l, "b:1") != Dead {
		t.Errorf("unheard peers should be dead: %v", l.Members())
	}
	if len(l.Alive()) != 0 {
		t.Errorf("Alive = %v", l.Alive())
	}
}

func TestSelfIsNotAMember(t *testing.T) {
	l := newList("me:1", "a:1")
	if len(l.Members()) != 1 {
		t.Errorf("members = %v", l.Members())
	}
}

func TestStateTransitions(t *testing.T) {
	l := newList("a:1")
	start := time.Now()
	l.heard("id-a", "a:1", start)

	steps := []struct {
		after time.Duration
		want  string
	}{
		{0, Alive},
		{2 * time.Second, Alive},
		{3 * time.Second, Suspect},
		{9 * time.Second, Suspect},
		{10 * time.Second, Dead},
		{time.Hour, Dead},
	}
	for _, s := range steps {
		l.updateStates(start.Add(s.after))
		if got := state(l, "a:1"); got != s.want {
			t.Errorf("after %v: state = %s, want %s", s.after, got, s.want)
		}
	}

	l.heard("id-a", "a:1", start.Add(time.Hour))
	l.updateStates(start.Add(time.Hour))
	if got := state(l, "a:1"); got != Alive {
		t.Errorf("after a new heartbeat: state = %s, want alive", got)
	}
}

func TestHeartbeatFromUnknownPeerAddsIt(t *testing.T) {
	l := newList()
	l.heard("id-x", "x:1", time.Now())
	members := l.Members()
	if len(members) != 1 || members[0].ID != "id-x" || members[0].State != Alive {
		t.Errorf("members = %v", members)
	}
}

func TestAliveOnlyListsAlive(t *testing.T) {
	l := newList("a:1", "b:1", "c:1")
	now := time.Now()
	l.heard("a", "a:1", now)
	l.heard("b", "b:1", now.Add(-5*time.Second))
	l.updateStates(now)
	if got := l.Alive(); !slices.Equal(got, []string{"a:1"}) {
		t.Errorf("Alive = %v", got)
	}
}
