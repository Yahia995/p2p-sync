package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"p2psync/internal/scan"
	"p2psync/internal/store"
	"p2psync/internal/vv"
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
	scanner := &scan.Scanner{Root: cfg.dir, Store: st, Self: self, StableDelay: 500 * time.Millisecond}
	if _, err := scanner.Rescan(); err != nil {
		return err
	}
	records, err := st.All()
	if err != nil {
		return err
	}
	fmt.Println("node", self)
	for _, rec := range records {
		fmt.Printf("%s %s deleted=%v size=%d hash=%.8s\n", rec.Path, vv.Encode(rec.Version), rec.Deleted, rec.Size, rec.Hash)
	}
	return nil
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
