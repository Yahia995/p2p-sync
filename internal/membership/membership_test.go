package membership

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"p2psync/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func newListOn(t *testing.T, st *store.Store, peers ...string) *List {
	t.Helper()
	l, err := New("me", "me:1", "", st, nil, 3*time.Second, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range peers {
		l.AddPeer(addr)
	}
	return l
}

func newList(t *testing.T, peers ...string) *List {
	return newListOn(t, openStore(t), peers...)
}

func member(l *List, addr string) Member {
	for _, m := range l.Members() {
		if m.Addr == addr {
			return m
		}
	}
	return Member{State: "missing"}
}

func state(l *List, addr string) string {
	return member(l, addr).State
}

func TestPeersStartDead(t *testing.T) {
	l := newList(t, "a:1", "b:1")
	if state(l, "a:1") != Dead || state(l, "b:1") != Dead {
		t.Errorf("unheard peers should be dead: %v", l.Members())
	}
	if len(l.Alive()) != 0 {
		t.Errorf("Alive = %v", l.Alive())
	}
}

func TestSelfIsNotAMember(t *testing.T) {
	l := newList(t, "me:1", "a:1")
	if len(l.Members()) != 1 {
		t.Errorf("members = %v", l.Members())
	}
}

func TestStateTransitions(t *testing.T) {
	l := newList(t, "a:1")
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

func TestHeartbeatFromUnknownPeerIgnored(t *testing.T) {
	l := newList(t)
	l.heard("id-x", "x:1", time.Now())
	if members := l.Members(); len(members) != 0 {
		t.Errorf("members = %v", members)
	}
}

func TestLearnAddsUnknownAsDead(t *testing.T) {
	l := newList(t)
	l.learn([]Member{{Addr: "x:1", ID: "id-x", State: Alive}})
	if m := member(l, "x:1"); m.ID != "id-x" || m.State != Dead {
		t.Errorf("learned member = %+v, want id-x and dead", m)
	}
}

func TestLearnFillsMissingIDOnly(t *testing.T) {
	l := newList(t, "a:1")
	l.learn([]Member{{Addr: "a:1", ID: "id-a"}})
	if m := member(l, "a:1"); m.ID != "id-a" {
		t.Errorf("id = %q, want id-a", m.ID)
	}
	l.learn([]Member{{Addr: "a:1", ID: "rumor"}})
	if m := member(l, "a:1"); m.ID != "id-a" {
		t.Errorf("gossip replaced a known id: %q", m.ID)
	}
}

func TestLearnKeepsLocalState(t *testing.T) {
	l := newList(t, "a:1")
	l.heard("id-a", "a:1", time.Now())
	l.learn([]Member{{Addr: "a:1", ID: "id-a", State: Dead}})
	if got := state(l, "a:1"); got != Alive {
		t.Errorf("gossip changed local state to %s", got)
	}
}

func TestLearnSkipsSelf(t *testing.T) {
	l := newList(t)
	l.learn([]Member{{Addr: "me:1", ID: "me"}, {Addr: ""}})
	if members := l.Members(); len(members) != 0 {
		t.Errorf("members = %v", members)
	}
}

func TestViewStartsWithSelf(t *testing.T) {
	l := newList(t, "a:1")
	view := l.view()
	if len(view) != 2 || view[0].Addr != "me:1" || view[0].ID != "me" || view[0].State != Alive {
		t.Errorf("view = %v", view)
	}
}

func TestMembersReloadedAsDead(t *testing.T) {
	st := openStore(t)
	l := newListOn(t, st, "a:1")
	l.learn([]Member{{Addr: "b:1", ID: "id-b"}})
	l.heard("id-a", "a:1", time.Now())

	reloaded := newListOn(t, st)
	want := []Member{{Addr: "a:1", ID: "id-a", State: Dead}, {Addr: "b:1", ID: "id-b", State: Dead}}
	if got := reloaded.Members(); !slices.Equal(got, want) {
		t.Errorf("reloaded members = %v, want %v", got, want)
	}
}

func TestAliveOnlyListsAlive(t *testing.T) {
	l := newList(t, "a:1", "b:1", "c:1")
	now := time.Now()
	l.heard("a", "a:1", now)
	l.heard("b", "b:1", now.Add(-5*time.Second))
	l.updateStates(now)
	if got := l.Alive(); !slices.Equal(got, []string{"a:1"}) {
		t.Errorf("Alive = %v", got)
	}
}
