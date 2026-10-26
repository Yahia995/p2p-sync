package scan

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/store"
	"p2psync/internal/vv"
)

const MetaDir = ".p2psync"

type Scanner struct {
	Root        string
	Store       *store.Store
	Self        string
	StableDelay time.Duration
}

type fileState struct {
	size  int64
	mtime int64
}

func (s *Scanner) Rescan() ([]string, error) {
	onDisk, err := s.walk()
	if err != nil {
		return nil, err
	}
	candidates, err := s.changedFiles(onDisk)
	if err != nil {
		return nil, err
	}
	time.Sleep(s.StableDelay)

	var changed []string
	for path, before := range candidates {
		created, err := s.indexFile(path, before)
		if err != nil {
			return nil, err
		}
		if created {
			changed = append(changed, path)
		}
	}
	deleted, err := s.markDeleted(onDisk)
	if err != nil {
		return nil, err
	}
	return append(changed, deleted...), nil
}

func (s *Scanner) walk() (map[string]fileState, error) {
	files := map[string]fileState{}
	err := filepath.WalkDir(s.Root, func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(s.Root, full)
		if err != nil {
			return err
		}
		if d.IsDir() && rel == MetaDir {
			return filepath.SkipDir
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = fileState{size: info.Size(), mtime: info.ModTime().UnixNano()}
		return nil
	})
	return files, err
}

func (s *Scanner) changedFiles(onDisk map[string]fileState) (map[string]fileState, error) {
	candidates := map[string]fileState{}
	for path, state := range onDisk {
		rec, found, err := s.Store.Get(path)
		if err != nil {
			return nil, err
		}
		if found && !rec.Deleted && rec.Size == state.size && rec.MTime == state.mtime {
			continue
		}
		candidates[path] = state
	}
	return candidates, nil
}

func (s *Scanner) indexFile(path string, before fileState) (bool, error) {
	full := filepath.Join(s.Root, filepath.FromSlash(path))
	info, err := os.Stat(full)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Size() != before.size || info.ModTime().UnixNano() != before.mtime {
		return false, nil
	}
	blocks, hash, err := chunk.HashFile(full)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	rec, found, err := s.Store.Get(path)
	if err != nil {
		return false, err
	}
	if found && !rec.Deleted && rec.Hash == hash {
		return false, s.Store.SetMTime(path, before.mtime)
	}
	next := store.Record{
		Path:    path,
		Version: vv.Bump(rec.Version, s.Self),
		Hash:    hash,
		Size:    before.size,
		MTime:   before.mtime,
	}
	return true, s.Store.Put(next, blocks)
}

func (s *Scanner) markDeleted(onDisk map[string]fileState) ([]string, error) {
	records, err := s.Store.All()
	if err != nil {
		return nil, err
	}
	var deleted []string
	for _, rec := range records {
		if rec.Deleted {
			continue
		}
		if _, exists := onDisk[rec.Path]; exists {
			continue
		}
		tombstone := store.Record{Path: rec.Path, Version: vv.Bump(rec.Version, s.Self), Deleted: true}
		if err := s.Store.Put(tombstone, nil); err != nil {
			return nil, err
		}
		deleted = append(deleted, rec.Path)
	}
	return deleted, nil
}
