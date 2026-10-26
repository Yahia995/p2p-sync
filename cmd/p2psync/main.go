package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"p2psync/internal/node"
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
	nodeCfg := node.Defaults()
	nodeCfg.Dir = cfg.dir
	nodeCfg.Listen = cfg.listen
	nodeCfg.Advertise = cfg.advertise
	nodeCfg.Peers = cfg.peers
	nodeCfg.Seed = cfg.seed

	n, err := node.Start(nodeCfg)
	if err != nil {
		return err
	}
	log.Printf("node %s listening on %s, advertised as %s, peers %v, seed %q", n.ID, cfg.listen, n.Addr, cfg.peers, cfg.seed)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	n.Stop()
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
