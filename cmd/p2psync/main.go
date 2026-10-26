package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
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

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Printf("dir=%s listen=%s advertise=%s peers=%v seed=%s\n", cfg.dir, cfg.listen, cfg.advertise, cfg.peers, cfg.seed)
}
