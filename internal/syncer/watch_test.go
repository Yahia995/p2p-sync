package syncer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2psync/internal/vv"
)

func TestReceivedFileIsNotReannounced(t *testing.T) {
	a, b := startNode(t), startNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- b.scanner.Watch(ctx, 20*time.Millisecond, time.Hour) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	a.write(t, "docs/report.txt", randomBytes(3, 300000))
	a.rescan(t)
	b.pull(t, a)
	time.Sleep(300 * time.Millisecond)

	got := b.record(t, "docs/report.txt")
	want := a.record(t, "docs/report.txt")
	if vv.Compare(got.Version, want.Version) != vv.Equal {
		t.Fatalf("receiver bumped the version: got %v, want %v", got.Version, want.Version)
	}
	if _, ok := got.Version[b.scanner.Self]; ok {
		t.Errorf("receiver %s appears in the version vector %v", b.scanner.Self, got.Version)
	}

	os.Remove(filepath.Join(a.root, "docs/report.txt"))
	a.rescan(t)
	b.pull(t, a)
	time.Sleep(300 * time.Millisecond)
	if rec := b.record(t, "docs/report.txt"); !rec.Deleted || vv.Compare(rec.Version, a.record(t, "docs/report.txt").Version) != vv.Equal {
		t.Errorf("received delete was re-announced: %+v", rec)
	}
}
