package membership

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	"p2psync/internal/store"
	"p2psync/internal/transport"
)

const (
	Alive   = "alive"
	Suspect = "suspect"
	Dead    = "dead"
)

const gossipFanout = 2

type Member struct {
	Addr      string    `json:"addr"`
	ID        string    `json:"id"`
	State     string    `json:"state"`
	LastHeard time.Time `json:"last_heard,omitzero"`
}

type heartbeat struct {
	From string `json:"from"`
	Addr string `json:"addr"`
}

type List struct {
	self         string
	selfAddr     string
	seed         string
	store        *store.Store
	client       *transport.Client
	suspectAfter time.Duration
	deadAfter    time.Duration

	mu      sync.Mutex
	members map[string]*Member
}

func New(self, selfAddr, seed string, st *store.Store, client *transport.Client, suspectAfter, deadAfter time.Duration) (*List, error) {
	l := &List{
		self:         self,
		selfAddr:     selfAddr,
		seed:         seed,
		store:        st,
		client:       client,
		suspectAfter: suspectAfter,
		deadAfter:    deadAfter,
		members:      map[string]*Member{},
	}
	peers, err := st.Peers()
	if err != nil {
		return nil, err
	}
	for _, p := range peers {
		if p.Addr != selfAddr {
			l.members[p.Addr] = &Member{Addr: p.Addr, ID: p.ID, State: Dead}
		}
	}
	return l, nil
}

func (l *List) AddPeer(addr string) {
	l.learn([]Member{{Addr: addr}})
}

func (l *List) learn(members []Member) {
	var changed []store.Peer
	l.mu.Lock()
	for _, m := range members {
		if m.Addr == "" || m.Addr == l.selfAddr {
			continue
		}
		known := l.members[m.Addr]
		switch {
		case known == nil:
			l.members[m.Addr] = &Member{Addr: m.Addr, ID: m.ID, State: Dead}
		case known.ID == "" && m.ID != "":
			known.ID = m.ID
		default:
			continue
		}
		changed = append(changed, store.Peer{Addr: m.Addr, ID: m.ID})
	}
	l.mu.Unlock()
	l.save(changed)
}

func (l *List) save(peers []store.Peer) {
	for _, p := range peers {
		if err := l.store.SavePeer(p); err != nil {
			log.Println("members:", err)
		}
	}
}

func (l *List) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb heartbeat
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil || hb.From == "" || hb.Addr == "" {
		http.Error(w, "bad heartbeat", http.StatusBadRequest)
		return
	}
	l.heard(hb.From, hb.Addr, time.Now())
	w.WriteHeader(http.StatusNoContent)
}

func (l *List) heard(id, addr string, now time.Time) {
	l.mu.Lock()
	m := l.members[addr]
	if m == nil {
		l.mu.Unlock()
		return
	}
	idChanged := m.ID != id
	m.ID = id
	m.LastHeard = now
	m.State = Alive
	l.mu.Unlock()
	if idChanged {
		l.save([]store.Peer{{Addr: addr, ID: id}})
	}
}

func (l *List) HandleJoin(w http.ResponseWriter, r *http.Request) {
	var m Member
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil || m.ID == "" || m.Addr == "" {
		http.Error(w, "bad join", http.StatusBadRequest)
		return
	}
	l.learn([]Member{m})
	transport.WriteJSON(w, l.view())
}

func (l *List) HandleGossip(w http.ResponseWriter, r *http.Request) {
	var members []Member
	if err := json.NewDecoder(r.Body).Decode(&members); err != nil {
		http.Error(w, "bad gossip", http.StatusBadRequest)
		return
	}
	l.learn(members)
	transport.WriteJSON(w, l.view())
}

func (l *List) view() []Member {
	view := []Member{l.selfMember()}
	for _, m := range l.Members() {
		view = append(view, Member{Addr: m.Addr, ID: m.ID, State: m.State})
	}
	return view
}

func (l *List) selfMember() Member {
	return Member{Addr: l.selfAddr, ID: l.self, State: Alive}
}

func (l *List) exchange(addr, path string, body any) {
	var answer []Member
	if err := l.client.PostJSON(addr, path, body, &answer); err == nil {
		l.learn(answer)
	}
}

func (l *List) joinIfAlone() {
	if l.seed == "" || len(l.Members()) > 0 {
		return
	}
	l.exchange(l.seed, "/join", l.selfMember())
}

func (l *List) gossip() {
	alive := l.Alive()
	rand.Shuffle(len(alive), func(i, j int) { alive[i], alive[j] = alive[j], alive[i] })
	view := l.view()
	for _, addr := range alive[:min(gossipFanout, len(alive))] {
		go l.exchange(addr, "/gossip", view)
	}
}

func (l *List) updateStates(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.members {
		silence := now.Sub(m.LastHeard)
		switch {
		case silence < l.suspectAfter:
			m.State = Alive
		case silence < l.deadAfter:
			m.State = Suspect
		default:
			m.State = Dead
		}
	}
}

func (l *List) sendHeartbeats() {
	hb := heartbeat{From: l.self, Addr: l.selfAddr}
	for _, m := range l.Members() {
		go l.client.PostJSON(m.Addr, "/heartbeat", hb, nil)
	}
}

func (l *List) Run(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		l.joinIfAlone()
		l.sendHeartbeats()
		l.gossip()
		l.updateStates(time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *List) Members() []Member {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Member
	for _, m := range l.members {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

func (l *List) Alive() []string {
	var addrs []string
	for _, m := range l.Members() {
		if m.State == Alive {
			addrs = append(addrs, m.Addr)
		}
	}
	return addrs
}
