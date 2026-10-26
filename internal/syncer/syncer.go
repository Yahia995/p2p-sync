package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

var errLocalChanged = errors.New("local file changed since the decision, retry later")

type Syncer struct {
	Self   string
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
		if err := s.handleRemote(addr, r); err != nil && !errors.Is(err, errLocalChanged) {
			errs = append(errs, fmt.Errorf("%s: %w", r.Path, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) handleRemote(addr string, r store.Record) error {
	if !validRecord(r) {
		return errors.New("invalid record")
	}
	local, found, err := s.Store.Get(r.Path)
	if err != nil {
		return err
	}
	if !found {
		return s.apply(addr, r, r.Version, local, false)
	}
	switch vv.Compare(r.Version, local.Version) {
	case vv.After:
		return s.apply(addr, r, r.Version, local, true)
	case vv.Concurrent:
		if r.Deleted == local.Deleted && r.Hash == local.Hash {
			return s.setVersion(local, vv.Merge(local.Version, r.Version))
		}
		return s.resolveConflict(addr, local, r)
	}
	return nil
}

func validRecord(r store.Record) bool {
	if !validPath(r.Path) {
		return false
	}
	if r.Deleted {
		return r.Hash == ""
	}
	_, err := hex.DecodeString(r.Hash)
	return err == nil && len(r.Hash) == 2*sha256.Size
}

func validPath(path string) bool {
	return filepath.IsLocal(filepath.FromSlash(path)) && !strings.HasPrefix(path, scan.MetaDir+"/") && path != scan.MetaDir
}

func (s *Syncer) apply(addr string, r store.Record, version vv.Vector, local store.Record, found bool) error {
	if r.Deleted {
		return s.remove(r.Path, version, local, found)
	}
	info, tmp, err := s.fetch(addr, r)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	info.Version = version
	return s.install(info.Record, info.Blocks, tmp, local, found)
}

func (s *Syncer) fetch(addr string, r store.Record) (transport.FileInfo, string, error) {
	info, err := s.Client.File(addr, r.Path)
	if err != nil {
		return info, "", err
	}
	if info.Deleted || vv.Compare(info.Version, r.Version) != vv.Equal {
		return info, "", errors.New("peer version changed")
	}
	tmp, err := s.download(addr, info)
	return info, tmp, err
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

func (s *Syncer) copyFromDisk(local store.Record) (string, error) {
	src, err := os.Open(s.fullPath(local.Path))
	if err != nil {
		return "", err
	}
	defer src.Close()
	dst, err := os.CreateTemp(s.tmpDir(), "copy-")
	if err != nil {
		return "", err
	}
	defer dst.Close()
	fileHash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(dst, fileHash), src); err != nil {
		os.Remove(dst.Name())
		return "", err
	}
	if hex.EncodeToString(fileHash.Sum(nil)) != local.Hash {
		os.Remove(dst.Name())
		return "", errLocalChanged
	}
	return dst.Name(), nil
}

func (s *Syncer) install(rec store.Record, blocks []chunk.Block, tmp string, before store.Record, beforeFound bool) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if err := s.checkUnchanged(rec.Path, before, beforeFound); err != nil {
		return err
	}
	target := s.fullPath(rec.Path)
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
	rec.MTime = stat.ModTime().UnixNano()
	return s.Store.Put(rec, blocks)
}

func (s *Syncer) remove(path string, version vv.Vector, before store.Record, beforeFound bool) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if err := s.checkUnchanged(path, before, beforeFound); err != nil {
		return err
	}
	if err := os.Remove(s.fullPath(path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return s.Store.Put(store.Record{Path: path, Version: version, Deleted: true}, nil)
}

func (s *Syncer) setVersion(before store.Record, version vv.Vector) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if err := s.checkUnchanged(before.Path, before, true); err != nil {
		return err
	}
	return s.Store.SetVersion(before.Path, version)
}

func (s *Syncer) checkUnchanged(path string, before store.Record, beforeFound bool) error {
	now, found, err := s.Store.Get(path)
	if err != nil {
		return err
	}
	if found != beforeFound {
		return errLocalChanged
	}
	if found && (vv.Compare(now.Version, before.Version) != vv.Equal || now.Deleted != before.Deleted || now.Hash != before.Hash) {
		return errLocalChanged
	}
	if !s.diskMatches(path, now, found) {
		return errLocalChanged
	}
	return nil
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
