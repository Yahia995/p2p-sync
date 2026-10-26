package transport

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

const peerHeader = "X-Node-Addr"

type Rule struct {
	Drop    bool `json:"drop"`
	DelayMs int  `json:"delay_ms"`
}

type Faults struct {
	mu    sync.Mutex
	rules map[string]Rule
}

func NewFaults() *Faults {
	return &Faults{rules: map[string]Rule{}}
}

func (f *Faults) Set(peer string, rule Rule) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rule == (Rule{}) {
		delete(f.rules, peer)
		return
	}
	f.rules[peer] = rule
}

func (f *Faults) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = map[string]Rule{}
}

func (f *Faults) Rules() map[string]Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := map[string]Rule{}
	for peer, rule := range f.rules {
		copied[peer] = rule
	}
	return copied
}

func (f *Faults) rule(peer string) Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rules[peer]
}

func (f *Faults) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rule := f.rule(r.Header.Get(peerHeader))
		if rule.Drop {
			http.Error(w, "dropped by fault filter", http.StatusServiceUnavailable)
			return
		}
		time.Sleep(time.Duration(rule.DelayMs) * time.Millisecond)
		next.ServeHTTP(w, r)
	})
}

func (f *Faults) HandleAdmin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Peer string `json:"peer"`
		Rule
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Peer == "" {
		http.Error(w, "expected {\"peer\": addr, \"drop\": bool, \"delay_ms\": int}", http.StatusBadRequest)
		return
	}
	f.Set(req.Peer, req.Rule)
	w.WriteHeader(http.StatusNoContent)
}
