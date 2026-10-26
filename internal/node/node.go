package node

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"p2psync/internal/membership"
	"p2psync/internal/scan"
	"p2psync/internal/store"
	"p2psync/internal/syncer"
	"p2psync/internal/transport"
)

type Config struct {
	Dir       string
	Listen    string
	Advertise string
	Peers     []string
	Seed      string
	Faults    *transport.Faults
	Logger    *log.Logger

	StableDelay      time.Duration
	Debounce         time.Duration
	RescanEvery      time.Duration
	AntiEntropyEvery time.Duration
	ProbeEvery       time.Duration
	PingTimeout      time.Duration
	SuspectTimeout   time.Duration
	RequestTimeout   time.Duration
}

func Defaults() Config {
	return Config{
		StableDelay:      500 * time.Millisecond,
		Debounce:         200 * time.Millisecond,
		RescanEvery:      60 * time.Second,
		AntiEntropyEvery: 5 * time.Second,
		ProbeEvery:       time.Second,
		PingTimeout:      300 * time.Millisecond,
		SuspectTimeout:   5 * time.Second,
		RequestTimeout:   10 * time.Second,
	}
}

type Node struct {
	ID      string
	Addr    string
	Store   *store.Store
	Scanner *scan.Scanner
	Syncer  *syncer.Syncer
	Members *membership.List
	Faults  *transport.Faults

	log            *log.Logger
	server         *http.Server
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	pushesReceived atomic.Int64
}

type pushMessage struct {
	From    string         `json:"from"`
	Records []store.Record `json:"records"`
}

type status struct {
	ID                 string                    `json:"id"`
	Addr               string                    `json:"addr"`
	Incarnation        uint64                    `json:"incarnation"`
	Files              int                       `json:"files"`
	Members            []membership.Member       `json:"members"`
	Faults             map[string]transport.Rule `json:"faults"`
	PushesReceived     int64                     `json:"pushes_received"`
	MembershipMessages int64                     `json:"membership_messages"`
}

func Start(cfg Config) (*Node, error) {
	if err := os.MkdirAll(filepath.Join(cfg.Dir, scan.MetaDir), 0o755); err != nil {
		return nil, err
	}
	st, err := store.Open(filepath.Join(cfg.Dir, scan.MetaDir, "index.db"))
	if err != nil {
		return nil, err
	}
	id, err := st.NodeID()
	if err != nil {
		st.Close()
		return nil, err
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		st.Close()
		return nil, err
	}

	n := &Node{ID: id, Addr: cfg.Advertise, Store: st, Faults: cfg.Faults, log: cfg.Logger}
	if n.Addr == "" {
		n.Addr = listener.Addr().String()
	}
	if n.Faults == nil {
		n.Faults = transport.NewFaults()
	}
	if n.log == nil {
		n.log = log.Default()
	}

	lock := &sync.Mutex{}
	n.Scanner = &scan.Scanner{Root: cfg.Dir, Store: st, Self: id, Lock: lock, StableDelay: cfg.StableDelay, OnChange: n.push}
	n.Syncer = &syncer.Syncer{Self: id, Root: cfg.Dir, Store: st, Lock: lock, Client: transport.NewClient(n.Addr, cfg.RequestTimeout, n.Faults)}
	if err := n.Syncer.ResetTmp(); err != nil {
		listener.Close()
		st.Close()
		return nil, err
	}
	pingClient := transport.NewClient(n.Addr, cfg.PingTimeout, n.Faults)
	membershipClient := transport.NewClient(n.Addr, cfg.ProbeEvery, n.Faults)
	n.Members, err = membership.New(id, n.Addr, cfg.Seed, st, pingClient, membershipClient, cfg.SuspectTimeout)
	if err != nil {
		listener.Close()
		st.Close()
		return nil, err
	}
	for _, addr := range cfg.Peers {
		n.Members.AddPeer(addr)
	}

	mux := transport.NewHandler(st, cfg.Dir)
	mux.HandleFunc("POST /ping", n.Members.HandlePing)
	mux.HandleFunc("POST /ping-req", n.Members.HandlePingReq)
	mux.HandleFunc("POST /join", n.Members.HandleJoin)
	mux.HandleFunc("POST /gossip", n.Members.HandleGossip)
	mux.HandleFunc("POST /push", n.handlePush)
	mux.HandleFunc("POST /admin/faults", n.Faults.HandleAdmin)
	mux.HandleFunc("GET /status", n.handleStatus)
	n.server = &http.Server{Handler: n.Faults.Wrap(mux)}

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.background(func() { n.server.Serve(listener) })
	n.background(func() {
		if err := n.Scanner.Watch(ctx, cfg.Debounce, cfg.RescanEvery); err != nil {
			n.log.Println("watcher:", err)
		}
	})
	n.background(func() { n.Members.Run(ctx, cfg.ProbeEvery) })
	n.background(func() { n.antiEntropy(ctx, cfg.AntiEntropyEvery) })
	return n, nil
}

func (n *Node) background(task func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		task()
	}()
}

func (n *Node) antiEntropy(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		alive := n.Members.Alive()
		if len(alive) == 0 {
			continue
		}
		if err := n.Syncer.SyncMerkle(alive[rand.IntN(len(alive))]); err != nil {
			n.log.Println("sync:", err)
		}
	}
}

func (n *Node) push(paths []string) {
	msg := pushMessage{From: n.Addr}
	for _, p := range paths {
		rec, found, err := n.Store.Get(p)
		if err != nil {
			n.log.Println("push:", err)
			return
		}
		if found {
			msg.Records = append(msg.Records, rec)
		}
	}
	for _, addr := range n.Members.Alive() {
		go func() {
			if err := n.Syncer.Client.PostJSON(addr, "/push", msg, nil); err != nil {
				n.log.Println("push:", err)
			}
		}()
	}
}

func (n *Node) handlePush(w http.ResponseWriter, r *http.Request) {
	var msg pushMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil || msg.From == "" {
		http.Error(w, "bad push", http.StatusBadRequest)
		return
	}
	n.pushesReceived.Add(1)
	if err := n.Syncer.ApplyRecords(msg.From, msg.Records); err != nil {
		n.log.Println("push from", msg.From+":", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	records, err := n.Store.All()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	transport.WriteJSON(w, status{
		ID:                 n.ID,
		Addr:               n.Addr,
		Incarnation:        n.Members.Incarnation(),
		Files:              len(records),
		Members:            n.Members.Members(),
		Faults:             n.Faults.Rules(),
		PushesReceived:     n.pushesReceived.Load(),
		MembershipMessages: n.Members.MessagesSent(),
	})
}

func (n *Node) Stop() {
	n.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.server.Shutdown(ctx); err != nil {
		n.server.Close()
	}
	n.wg.Wait()
	n.Store.Close()
}
