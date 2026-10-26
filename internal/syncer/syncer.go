package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"p2psync/internal/chunk"
	"p2psync/internal/scan"
	"p2psync/internal/store"
	"p2psync/internal/transport"
	"p2psync/internal/vv"
)

type Syncer struct {
	Root   string
	Store  *store.Store
	Lock   *sync.Mutex
	Client *transport.Client
}

func (s *Syncer) tmpDir() string {
	return filepath.Join(s.Root, scan.MetaDir, "tmp")
}

func (s *Syncer) fullPath(path string) string {
	return filepath.Join(s.Root, filepath.FromSlash(path))
}

func (s *Syncer) ResetTmp() error {
	if err := os.RemoveAll(s.tmpDir()); err != nil {
		return err
	}
	return os.MkdirAll(s.tmpDir(), 0o755)
}

func (s *Syncer) SyncWith(addr string) error {
	records, err := s.Client.Index(addr)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range records {
		if err := s.handleRemote(addr, r); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Path, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) handleRemote(addr string, r store.Record) error {
	if !validPath(r.Path) {
		return errors.New("invalid path")
	}
	newer, err := s.isNewer(r)
	if err != nil || !newer {
		return err
	}
	if r.Deleted {
		return s.applyDelete(r)
	}
	return s.applyFile(addr, r)
}

func validPath(path string) bool {
	return filepath.IsLocal(filepath.FromSlash(path)) && !strings.HasPrefix(path, scan.MetaDir+"/") && path != scan.MetaDir
}

func (s *Syncer) isNewer(r store.Record) (bool, error) {
	local, found, err := s.Store.Get(r.Path)
	if err != nil {
		return false, err
	}
	return !found || vv.Compare(r.Version, local.Version) == vv.After, nil
}

func (s *Syncer) applyDelete(r store.Record) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	local, found, err := s.Store.Get(r.Path)
	if err != nil {
		return err
	}
	if found && vv.Compare(r.Version, local.Version) != vv.After {
		return nil
	}
	if !s.diskMatches(r.Path, local, found) {
		return nil
	}
	if err := os.Remove(s.fullPath(r.Path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return s.Store.Put(store.Record{Path: r.Path, Version: r.Version, Deleted: true}, nil)
}

func (s *Syncer) applyFile(addr string, r store.Record) error {
	info, err := s.Client.File(addr, r.Path)
	if err != nil {
		return err
	}
	if info.Deleted || vv.Compare(info.Version, r.Version) != vv.Equal {
		return nil
	}
	tmp, err := s.download(addr, info)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	return s.install(info, tmp)
}

func (s *Syncer) download(addr string, info transport.FileInfo) (string, error) {
	f, err := os.CreateTemp(s.tmpDir(), "download-")
	if err != nil {
		return "", err
	}
	defer f.Close()
	fileHash := sha256.New()
	for _, b := range info.Blocks {
		data, err := s.fetchBlock(addr, b)
		if err == nil {
			_, err = f.Write(data)
		}
		if err != nil {
			os.Remove(f.Name())
			return "", err
		}
		fileHash.Write(data)
	}
	if hex.EncodeToString(fileHash.Sum(nil)) != info.Hash {
		os.Remove(f.Name())
		return "", errors.New("file hash mismatch")
	}
	return f.Name(), nil
}

func (s *Syncer) fetchBlock(addr string, b chunk.Block) ([]byte, error) {
	path, offset, size, found, err := s.Store.FindBlock(b.Hash)
	if err == nil && found {
		data, err := chunk.ReadBlock(s.fullPath(path), offset, size, b.Hash)
		if err == nil {
			return data, nil
		}
	}
	data, err := s.Client.Block(addr, b.Hash)
	if err != nil {
		return nil, err
	}
	if chunk.Hash(data) != b.Hash {
		return nil, chunk.ErrHashMismatch
	}
	return data, nil
}

func (s *Syncer) install(info transport.FileInfo, tmp string) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	local, found, err := s.Store.Get(info.Path)
	if err != nil {
		return err
	}
	if found && vv.Compare(info.Version, local.Version) != vv.After {
		return nil
	}
	if !s.diskMatches(info.Path, local, found) {
		return nil
	}
	target := s.fullPath(info.Path)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	stat, err := os.Stat(target)
	if err != nil {
		return err
	}
	info.MTime = stat.ModTime().UnixNano()
	return s.Store.Put(info.Record, info.Blocks)
}

func (s *Syncer) diskMatches(path string, local store.Record, found bool) bool {
	stat, err := os.Stat(s.fullPath(path))
	if errors.Is(err, fs.ErrNotExist) {
		return !found || local.Deleted
	}
	if err != nil {
		return false
	}
	return found && !local.Deleted && stat.Size() == local.Size && stat.ModTime().UnixNano() == local.MTime
}
