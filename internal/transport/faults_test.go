package transport

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func echoServer(t *testing.T, faults *Faults) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /index", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, []string{})
	})
	mux.HandleFunc("POST /admin/faults", faults.HandleAdmin)
	server := httptest.NewServer(faults.Wrap(mux))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func TestDropOutgoing(t *testing.T) {
	addr := echoServer(t, NewFaults())
	faults := NewFaults()
	client := NewClient("me:1", time.Second, faults)

	faults.Set(addr, Rule{Drop: true})
	if _, err := client.Index(addr); err == nil {
		t.Fatal("request to a dropped peer should fail")
	}
	faults.Set(addr, Rule{})
	if _, err := client.Index(addr); err != nil {
		t.Fatalf("request after removing the rule failed: %v", err)
	}
}

func TestDropIncoming(t *testing.T) {
	serverFaults := NewFaults()
	addr := echoServer(t, serverFaults)
	serverFaults.Set("blocked:1", Rule{Drop: true})

	if _, err := NewClient("blocked:1", time.Second, NewFaults()).Index(addr); err == nil {
		t.Error("request from a dropped peer should be rejected")
	}
	if _, err := NewClient("other:1", time.Second, NewFaults()).Index(addr); err != nil {
		t.Errorf("request from another peer failed: %v", err)
	}
}

func TestDelay(t *testing.T) {
	addr := echoServer(t, NewFaults())
	faults := NewFaults()
	faults.Set(addr, Rule{DelayMs: 100})
	start := time.Now()
	if _, err := NewClient("me:1", time.Second, faults).Index(addr); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("request took %v, want at least 100ms", elapsed)
	}
}

func TestAdminEndpoint(t *testing.T) {
	faults := NewFaults()
	addr := echoServer(t, faults)
	admin := NewClient("", time.Second, NewFaults())

	if err := admin.PostJSON(addr, "/admin/faults", map[string]any{"peer": "x:1", "drop": true}); err != nil {
		t.Fatal(err)
	}
	if !faults.Rules()["x:1"].Drop {
		t.Fatalf("rule not set: %v", faults.Rules())
	}
	if err := admin.PostJSON(addr, "/admin/faults", map[string]any{"peer": "x:1", "drop": false}); err != nil {
		t.Fatal(err)
	}
	if len(faults.Rules()) != 0 {
		t.Errorf("rule not removed: %v", faults.Rules())
	}
	if err := admin.PostJSON(addr, "/admin/faults", map[string]any{"drop": true}); err == nil {
		t.Error("missing peer should be rejected")
	}
}
