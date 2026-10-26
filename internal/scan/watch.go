package scan

import (
	"context"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

func (s *Scanner) Watch(ctx context.Context, debounce, fullEvery time.Duration) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()
	if err := s.watchTree(w, s.Root); err != nil {
		return err
	}

	s.rescanAndLog()
	pending := time.NewTimer(debounce)
	pending.Stop()
	full := time.NewTicker(fullEvery)
	defer full.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-w.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Create) {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					s.watchTree(w, event.Name)
				}
			}
			pending.Reset(debounce)
		case err, ok := <-w.Errors:
			if !ok {
				return nil
			}
			log.Println("watcher:", err)
			pending.Reset(debounce)
		case <-pending.C:
			s.rescanAndLog()
		case <-full.C:
			s.rescanAndLog()
		}
	}
}

func (s *Scanner) watchTree(w *fsnotify.Watcher, dir string) error {
	return filepath.WalkDir(dir, func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if full == filepath.Join(s.Root, MetaDir) {
			return filepath.SkipDir
		}
		return w.Add(full)
	})
}

func (s *Scanner) rescanAndLog() {
	if _, err := s.Rescan(); err != nil {
		log.Println("rescan:", err)
	}
}
