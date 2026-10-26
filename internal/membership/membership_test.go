package membership

import (
	"math/bits"
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
	l, err := New("me", "me:1", "", st, nil, nil, 5*time.Second)
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

func stateAt(l *List, addr string) (string, uint64) {
	m := member(l, addr)
	return m.State, m.Incarnation
}

func TestPeersStartAlive(t *testing.T) {
	l := newList(t, "a:1", "b:1")
	if got := l.Alive(); !slices.Equal(got, []string{"a:1", "b:1"}) {
		t.Errorf("Alive = %v", got)
	}
}

func TestSelfIsNotAMember(t *testing.T) {
	l := newList(t, "me:1", "a:1")
	if len(l.Members()) != 1 {
		t.Errorf("members = %v", l.Members())
	}
}

func TestMergeRules(t *testing.T) {
	cases := []struct {
		name      string
		update    Member
		wantState string
		wantInc   uint64
	}{
		{"higher incarnation wins even if better", Member{Incarnation: 3, State: Alive}, Alive, 3},
		{"higher incarnation wins", Member{Incarnation: 3, State: Dead}, Dead, 3},
		{"same incarnation worse state wins", Member{Incarnation: 2, State: Dead}, Dead, 2},
		{"same incarnation better state ignored", Member{Incarnation: 2, State: Alive}, Suspect, 2},
		{"same incarnation same state ignored", Member{Incarnation: 2, State: Suspect}, Suspect, 2},
		{"lower incarnation ignored", Member{Incarnation: 1, State: Dead}, Suspect, 2},
	}
	for _, c := range cases {
		l := newList(t)
		l.learn([]Member{{Addr: "a:1", ID: "id-a", Incarnation: 2, State: Suspect}})
		c.update.Addr = "a:1"
		l.learn([]Member{c.update})
		if state, inc := stateAt(l, "a:1"); state != c.wantState || inc != c.wantInc {
			t.Errorf("%s: got %s@%d, want %s@%d", c.name, state, inc, c.wantState, c.wantInc)
		}
	}
}

func TestUnknownMemberTakenAsReported(t *testing.T) {
	l := newList(t)
	l.learn([]Member{{Addr: "x:1", ID: "id-x", Incarnation: 4, State: Suspect}})
	if m := member(l, "x:1"); m.ID != "id-x" || m.State != Suspect || m.Incarnation != 4 {
		t.Errorf("learned member = %+v", m)
	}
}

func TestLearnFillsMissingIDOnly(t *testing.T) {
	l := newList(t, "a:1")
	l.learn([]Member{{Addr: "a:1", ID: "id-a", State: Alive}})
	if m := member(l, "a:1"); m.ID != "id-a" {
		t.Errorf("id = %q, want id-a", m.ID)
	}
	l.learn([]Member{{Addr: "a:1", ID: "rumor", Incarnation: 9, State: Alive}})
	if m := member(l, "a:1"); m.ID != "id-a" {
		t.Errorf("a rumor replaced a known id: %q", m.ID)
	}
}

func TestLearnIgnoresInvalid(t *testing.T) {
	l := newList(t)
	l.learn([]Member{{Addr: "", State: Alive}, {Addr: "x:1", State: "sleeping"}})
	if members := l.Members(); len(members) != 0 {
		t.Errorf("members = %v", members)
	}
}

func TestRefutation(t *testing.T) {
	l := newList(t)
	l.learn([]Member{{Addr: "me:1", Incarnation: 0, State: Suspect}})
	if l.Incarnation() != 1 {
		t.Fatalf("incarnation = %d after a suspicion, want 1", l.Incarnation())
	}
	updates := l.piggyback()
	if len(updates) != 1 || updates[0].Addr != "me:1" || updates[0].State != Alive || updates[0].Incarnation != 1 {
		t.Errorf("refutation not disseminated: %v", updates)
	}

	l.learn([]Member{{Addr: "me:1", Incarnation: 0, State: Dead}})
	l.learn([]Member{{Addr: "me:1", Incarnation: 7, State: Alive}})
	if l.Incarnation() != 1 {
		t.Errorf("old rumor or alive rumor changed incarnation to %d", l.Incarnation())
	}

	l.learn([]Member{{Addr: "me:1", Incarnation: 5, State: Dead}})
	if l.Incarnation() != 6 {
		t.Errorf("incarnation = %d after dead@5, want 6", l.Incarnation())
	}
}

func TestSuspicionExpires(t *testing.T) {
	l := newList(t, "a:1", "b:1")
	l.learn([]Member{{Addr: "b:1", Incarnation: 0, State: Dead}})
	l.suspect("a:1")
	l.suspect("b:1")
	start := member(l, "a:1").suspectSince
	if state, _ := stateAt(l, "a:1"); state != Suspect {
		t.Fatalf("a:1 = %s, want suspect", state)
	}
	if state, _ := stateAt(l, "b:1"); state != Dead {
		t.Errorf("a dead member became %s", state)
	}

	l.expireSuspects(start.Add(4 * time.Second))
	if state, _ := stateAt(l, "a:1"); state != Suspect {
		t.Errorf("after 4 s: %s, want suspect", state)
	}
	l.expireSuspects(start.Add(5 * time.Second))
	if state, inc := stateAt(l, "a:1"); state != Dead || inc != 0 {
		t.Errorf("after 5 s: %s@%d, want dead@0", state, inc)
	}
}

func TestPiggybackLimits(t *testing.T) {
	l := newList(t)
	for _, addr := range []string{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1", "g:1"} {
		l.learn([]Member{{Addr: addr, State: Alive}})
	}
	sent := map[string]int{}
	first := l.piggyback()
	if len(first) != maxPiggyback {
		t.Fatalf("piggyback carried %d updates, want %d", len(first), maxPiggyback)
	}
	second := l.piggyback()
	if second[0].Addr != "g:1" {
		t.Errorf("least-sent update not first: %v", second)
	}
	for updates := slices.Concat(first, second); len(updates) > 0; updates = l.piggyback() {
		for _, u := range updates {
			sent[u.Addr]++
		}
	}

	limit := retransmitMult * bits.Len(8)
	for addr, n := range sent {
		if n != limit {
			t.Errorf("%s sent %d times, want %d", addr, n, limit)
		}
	}
}

func TestRoundRobinSkipsDead(t *testing.T) {
	l := newList(t, "a:1", "b:1", "c:1")
	l.learn([]Member{{Addr: "c:1", State: Dead}})
	for round := 0; round < 3; round++ {
		var seen []string
		for i := 0; i < 2; i++ {
			seen = append(seen, l.nextTarget())
		}
		slices.Sort(seen)
		if !slices.Equal(seen, []string{"a:1", "b:1"}) {
			t.Errorf("round %d probed %v", round, seen)
		}
	}
}

func TestViewStartsWithSelf(t *testing.T) {
	l := newList(t, "a:1")
	l.learn([]Member{{Addr: "me:1", State: Suspect}})
	view := l.view()
	if len(view) != 2 || view[0].Addr != "me:1" || view[0].ID != "me" || view[0].State != Alive || view[0].Incarnation != 1 {
		t.Errorf("view = %v", view)
	}
}

func TestMembersReloadedAsAlive(t *testing.T) {
	st := openStore(t)
	l := newListOn(t, st, "a:1")
	l.learn([]Member{{Addr: "b:1", ID: "id-b", Incarnation: 3, State: Dead}})
	l.learn([]Member{{Addr: "a:1", ID: "id-a", State: Alive}})

	reloaded := newListOn(t, st)
	want := []Member{{Addr: "a:1", ID: "id-a", State: Alive}, {Addr: "b:1", ID: "id-b", State: Alive}}
	if got := reloaded.Members(); !slices.Equal(got, want) {
		t.Errorf("reloaded members = %v, want %v", got, want)
	}
}
