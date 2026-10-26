package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"p2psync/internal/scan"
	"p2psync/internal/store"
	"p2psync/internal/syncer"
	"p2psync/internal/transport"
)

const (
	watchDebounce     = 200 * time.Millisecond
	rescanPeriod      = 60 * time.Second
	antiEntropyPeriod = 5 * time.Second
	requestTimeout    = 10 * time.Second
)

type config struct {
	dir       string
	listen    string
	advertise string
	peers     []string
	seed      string
}

func parseFlags(args []string) (config, error) {
	var cfg config
	var peers string
	fs := flag.NewFlagSet("p2psync", flag.ContinueOnError)
	fs.StringVar(&cfg.dir, "dir", "", "shared folder")
	fs.StringVar(&cfg.listen, "listen", "127.0.0.1:7001", "listen address")
	fs.StringVar(&cfg.advertise, "advertise", "", "address announced to peers (default: listen)")
	fs.StringVar(&peers, "peers", "", "comma-separated static peer addresses")
	fs.StringVar(&cfg.seed, "seed", "", "seed node address")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.dir == "" {
		return config{}, errors.New("-dir is required")
	}
	if cfg.advertise == "" {
		cfg.advertise = cfg.listen
	}
	if peers != "" {
		cfg.peers = strings.Split(peers, ",")
	}
	return cfg, nil
}

func every(period time.Duration, task func() error) {
	for {
		if err := task(); err != nil {
			log.Println(err)
		}
		time.Sleep(period)
	}
}

func run(cfg config) error {
	if err := os.MkdirAll(filepath.Join(cfg.dir, scan.MetaDir), 0o755); err != nil {
		return err
	}
	st, err := store.Open(filepath.Join(cfg.dir, scan.MetaDir, "index.db"))
	if err != nil {
		return err
	}
	defer st.Close()
	self, err := st.NodeID()
	if err != nil {
		return err
	}

	lock := &sync.Mutex{}
	scanner := &scan.Scanner{Root: cfg.dir, Store: st, Self: self, Lock: lock, StableDelay: 500 * time.Millisecond}
	engine := &syncer.Syncer{Root: cfg.dir, Store: st, Lock: lock, Client: transport.NewClient(requestTimeout)}
	if err := engine.ResetTmp(); err != nil {
		return err
	}

	go func() {
		if err := scanner.Watch(context.Background(), watchDebounce, rescanPeriod); err != nil {
			log.Fatal(err)
		}
	}()
	go every(antiEntropyPeriod, func() error {
		if len(cfg.peers) == 0 {
			return nil
		}
		return engine.SyncWith(cfg.peers[rand.IntN(len(cfg.peers))])
	})

	log.Printf("node %s listening on %s, peers %v", self, cfg.listen, cfg.peers)
	return http.ListenAndServe(cfg.listen, transport.NewHandler(st, cfg.dir))
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
