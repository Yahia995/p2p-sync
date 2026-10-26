package membership

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"p2psync/internal/transport"
)

const (
	Alive   = "alive"
	Suspect = "suspect"
	Dead    = "dead"
)

type Member struct {
	Addr      string    `json:"addr"`
	ID        string    `json:"id"`
	State     string    `json:"state"`
	LastHeard time.Time `json:"last_heard"`
}

type heartbeat struct {
	From string `json:"from"`
	Addr string `json:"addr"`
}

type List struct {
	self         string
	selfAddr     string
	client       *transport.Client
	suspectAfter time.Duration
	deadAfter    time.Duration

	mu      sync.Mutex
	members map[string]*Member
}

func New(self, selfAddr string, peers []string, client *transport.Client, suspectAfter, deadAfter time.Duration) *List {
	l := &List{
		self:         self,
		selfAddr:     selfAddr,
		client:       client,
		suspectAfter: suspectAfter,
		deadAfter:    deadAfter,
		members:      map[string]*Member{},
	}
	for _, addr := range peers {
		l.AddPeer(addr)
	}
	return l
}

func (l *List) AddPeer(addr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if addr == l.selfAddr || l.members[addr] != nil {
		return
	}
	l.members[addr] = &Member{Addr: addr, State: Dead}
}

func (l *List) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb heartbeat
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil || hb.Addr == "" {
		http.Error(w, "bad heartbeat", http.StatusBadRequest)
		return
	}
	l.heard(hb.From, hb.Addr, time.Now())
	w.WriteHeader(http.StatusNoContent)
}

func (l *List) heard(id, addr string, now time.Time) {
	l.AddPeer(addr)
	l.mu.Lock()
	defer l.mu.Unlock()
	if m := l.members[addr]; m != nil {
		m.ID = id
		m.LastHeard = now
		m.State = Alive
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
		go l.client.PostJSON(m.Addr, "/heartbeat", hb)
	}
}

func (l *List) Run(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		l.sendHeartbeats()
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
