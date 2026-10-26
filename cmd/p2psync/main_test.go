package main

import (
	"slices"
	"testing"
)

func TestParseFlagsDefaults(t *testing.T) {
	cfg, err := parseFlags([]string{"-dir", "/tmp/a"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:7001" || cfg.advertise != "127.0.0.1:7001" {
		t.Fatalf("unexpected addresses: %+v", cfg)
	}
	if cfg.peers != nil {
		t.Fatalf("expected no peers, got %v", cfg.peers)
	}
}

func TestParseFlagsPeers(t *testing.T) {
	cfg, err := parseFlags([]string{"-dir", "/tmp/a", "-peers", "10.0.0.1:7001,10.0.0.2:7002"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.peers, []string{"10.0.0.1:7001", "10.0.0.2:7002"}) {
		t.Fatalf("unexpected peers: %v", cfg.peers)
	}
}

func TestParseFlagsRequiresDir(t *testing.T) {
	if _, err := parseFlags(nil); err == nil {
		t.Fatal("expected error without -dir")
	}
}
