package membership

import (
	"context"
	"encoding/json"
	"log"
	"math/bits"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2psync/internal/store"
	"p2psync/internal/transport"
)

const (
	Alive   = "alive"
	Suspect = "suspect"
	Dead    = "dead"
)

const (
	gossipFanout   = 2
	indirectProbes = 3
	maxPiggyback   = 6
	retransmitMult = 3
)

var stateRank = map[string]int{Alive: 0, Suspect: 1, Dead: 2}

type Member struct {
	Addr        string `json:"addr"`
	ID          string `json:"id"`
	Incarnation uint64 `json:"incarnation"`
	State       string `json:"state"`

	suspectSince time.Time
}

type swimMessage struct {
	Updates []Member `json:"updates"`
}

type pingReq struct {
	Target  string   `json:"target"`
	Updates []Member `json:"updates"`
}

type pingReqAck struct {
	OK      bool     `json:"ok"`
	Updates []Member `json:"updates"`
}

type List struct {
	self           string
	selfAddr       string
	seed           string
	store          *store.Store
	pingClient     *transport.Client
	client         *transport.Client
	suspectTimeout time.Duration
	messagesSent   atomic.Int64

	mu          sync.Mutex
	incarnation uint64
	members     map[string]*Member
	pending     map[string]int
	probeOrder  []string
}

func New(self, selfAddr, seed string, st *store.Store, pingClient, client *transport.Client, suspectTimeout time.Duration) (*List, error) {
	l := &List{
		self:           self,
		selfAddr:       selfAddr,
		seed:           seed,
		store:          st,
		pingClient:     pingClient,
		client:         client,
		suspectTimeout: suspectTimeout,
		members:        map[string]*Member{},
		pending:        map[string]int{},
	}
	peers, err := st.Peers()
	if err != nil {
		return nil, err
	}
	for _, p := range peers {
		if p.Addr != selfAddr {
			l.members[p.Addr] = &Member{Addr: p.Addr, ID: p.ID, State: Alive}
		}
	}
	return l, nil
}

func (l *List) AddPeer(addr string) {
	l.mu.Lock()
	if addr == l.selfAddr || l.members[addr] != nil {
		l.mu.Unlock()
		return
	}
	l.members[addr] = &Member{Addr: addr, State: Alive}
	l.mu.Unlock()
	l.save([]store.Peer{{Addr: addr}})
}

func overrides(update, known Member) bool {
	if update.Incarnation != known.Incarnation {
		return update.Incarnation > known.Incarnation
	}
	return stateRank[update.State] > stateRank[known.State]
}

func (l *List) learn(updates []Member) {
	var changed []store.Peer
	l.mu.Lock()
	for _, u := range updates {
		if _, valid := stateRank[u.State]; !valid || u.Addr == "" {
			continue
		}
		if u.Addr == l.selfAddr {
			l.refute(u)
			continue
		}
		known := l.members[u.Addr]
		if known == nil {
			known = &Member{Addr: u.Addr, ID: u.ID}
			l.members[u.Addr] = known
			changed = append(changed, store.Peer{Addr: u.Addr, ID: u.ID})
		} else if known.ID == "" && u.ID != "" {
			known.ID = u.ID
			changed = append(changed, store.Peer{Addr: u.Addr, ID: u.ID})
		}
		if known.State == "" || overrides(u, *known) {
			l.set(known, u.Incarnation, u.State)
		}
	}
	l.mu.Unlock()
	l.save(changed)
}

func (l *List) refute(rumor Member) {
	if rumor.State != Alive && rumor.Incarnation >= l.incarnation {
		l.incarnation = rumor.Incarnation + 1
		l.pending[l.selfAddr] = 0
	}
}

func (l *List) set(m *Member, incarnation uint64, state string) {
	if state == Suspect && m.State != Suspect {
		m.suspectSince = time.Now()
	}
	m.Incarnation = incarnation
	m.State = state
	l.pending[m.Addr] = 0
}

func (l *List) suspect(addr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m := l.members[addr]; m != nil && m.State == Alive {
		l.set(m, m.Incarnation, Suspect)
	}
}

func (l *List) expireSuspects(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.members {
		if m.State == Suspect && now.Sub(m.suspectSince) >= l.suspectTimeout {
			l.set(m, m.Incarnation, Dead)
		}
	}
}

func (l *List) save(peers []store.Peer) {
	for _, p := range peers {
		if err := l.store.SavePeer(p); err != nil {
			log.Println("members:", err)
		}
	}
}

func (l *List) piggyback() []Member {
	l.mu.Lock()
	defer l.mu.Unlock()
	limit := retransmitMult * bits.Len(uint(len(l.members)+1))
	var addrs []string
	for addr := range l.pending {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool {
		if l.pending[addrs[i]] != l.pending[addrs[j]] {
			return l.pending[addrs[i]] < l.pending[addrs[j]]
		}
		return addrs[i] < addrs[j]
	})
	var updates []Member
	for _, addr := range addrs[:min(maxPiggyback, len(addrs))] {
		updates = append(updates, l.entry(addr))
		l.pending[addr]++
		if l.pending[addr] >= limit {
			delete(l.pending, addr)
		}
	}
	return updates
}

func (l *List) entry(addr string) Member {
	if addr == l.selfAddr {
		return l.selfEntry()
	}
	return *l.members[addr]
}

func (l *List) selfEntry() Member {
	return Member{Addr: l.selfAddr, ID: l.self, Incarnation: l.incarnation, State: Alive}
}

func (l *List) HandlePing(w http.ResponseWriter, r *http.Request) {
	var msg swimMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad ping", http.StatusBadRequest)
		return
	}
	l.learn(msg.Updates)
	transport.WriteJSON(w, swimMessage{Updates: l.piggyback()})
}

func (l *List) HandlePingReq(w http.ResponseWriter, r *http.Request) {
	var req pingReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" {
		http.Error(w, "bad ping-req", http.StatusBadRequest)
		return
	}
	l.learn(req.Updates)
	ok := l.ping(req.Target)
	transport.WriteJSON(w, pingReqAck{OK: ok, Updates: l.piggyback()})
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
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Member{l.selfEntry()}, l.sorted()...)
}

func (l *List) ping(addr string) bool {
	l.messagesSent.Add(1)
	var reply swimMessage
	if err := l.pingClient.PostJSON(addr, "/ping", swimMessage{Updates: l.piggyback()}, &reply); err != nil {
		return false
	}
	l.learn(reply.Updates)
	return true
}

func (l *List) indirectPing(target string) bool {
	var helpers []string
	for _, addr := range shuffled(l.withState(Alive)) {
		if addr != target && len(helpers) < indirectProbes {
			helpers = append(helpers, addr)
		}
	}
	results := make(chan bool, len(helpers))
	for _, helper := range helpers {
		go func() {
			l.messagesSent.Add(1)
			var reply pingReqAck
			err := l.client.PostJSON(helper, "/ping-req", pingReq{Target: target, Updates: l.piggyback()}, &reply)
			if err == nil {
				l.learn(reply.Updates)
			}
			results <- err == nil && reply.OK
		}()
	}
	for range helpers {
		if <-results {
			return true
		}
	}
	return false
}

func (l *List) nextTarget() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.probeOrder) == 0 {
		for _, m := range l.members {
			if m.State != Dead {
				l.probeOrder = append(l.probeOrder, m.Addr)
			}
		}
		l.probeOrder = shuffled(l.probeOrder)
	}
	for len(l.probeOrder) > 0 {
		addr := l.probeOrder[0]
		l.probeOrder = l.probeOrder[1:]
		if m := l.members[addr]; m != nil && m.State != Dead {
			return addr
		}
	}
	return ""
}

func (l *List) probe() {
	target := l.nextTarget()
	if target == "" || l.ping(target) || l.indirectPing(target) {
		return
	}
	l.suspect(target)
}

func (l *List) exchange(addr, path string, body any) {
	l.messagesSent.Add(1)
	var answer []Member
	if err := l.client.PostJSON(addr, path, body, &answer); err == nil {
		l.learn(answer)
	}
}

func (l *List) joinIfAlone() {
	if l.seed == "" || len(l.Members()) > 0 {
		return
	}
	l.mu.Lock()
	self := l.selfEntry()
	l.mu.Unlock()
	l.exchange(l.seed, "/join", self)
}

func (l *List) gossip() {
	alive := shuffled(l.withState(Alive))
	targets := alive[:min(gossipFanout, len(alive))]
	if dead := shuffled(l.withState(Dead)); len(dead) > 0 {
		targets = append(targets, dead[0])
	}
	view := l.view()
	for _, addr := range targets {
		go l.exchange(addr, "/gossip", view)
	}
}

func (l *List) Run(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		l.joinIfAlone()
		l.probe()
		l.gossip()
		l.expireSuspects(time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func shuffled(addrs []string) []string {
	rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })
	return addrs
}

func (l *List) sorted() []Member {
	var out []Member
	for _, m := range l.members {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

func (l *List) Members() []Member {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sorted()
}

func (l *List) withState(state string) []string {
	var addrs []string
	for _, m := range l.Members() {
		if m.State == state {
			addrs = append(addrs, m.Addr)
		}
	}
	return addrs
}

func (l *List) Alive() []string {
	return l.withState(Alive)
}

func (l *List) Incarnation() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.incarnation
}

func (l *List) MessagesSent() int64 {
	return l.messagesSent.Load()
}
