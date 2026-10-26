package syncer

import (
	"os"
	"path"
	"strings"

	"p2psync/internal/store"
	"p2psync/internal/vv"
)

func beats(a, b store.Record) bool {
	if a.Deleted != b.Deleted {
		return !a.Deleted
	}
	return a.Hash > b.Hash
}

func conflictName(p, hash string) string {
	dir, file := path.Split(p)
	ext := path.Ext(file)
	stem := strings.TrimSuffix(file, ext)
	if stem == "" {
		stem, ext = file, ""
	}
	return dir + stem + ".conflict-" + hash[:8] + ext
}

func conflictCopy(loser store.Record) store.Record {
	return store.Record{
		Path:    conflictName(loser.Path, loser.Hash),
		Version: loser.Version,
		Hash:    loser.Hash,
		Size:    loser.Size,
	}
}

func (s *Syncer) resolveConflict(addr string, local, remote store.Record) error {
	merged := vv.Bump(vv.Merge(local.Version, remote.Version), s.Self)
	if beats(remote, local) {
		if !local.Deleted {
			if err := s.keepLocalCopy(local); err != nil {
				return err
			}
		}
		return s.apply(addr, remote, merged, local, true)
	}
	if !remote.Deleted {
		if err := s.keepRemoteCopy(addr, remote); err != nil {
			return err
		}
	}
	return s.setVersion(local, merged)
}

func (s *Syncer) keepLocalCopy(local store.Record) error {
	copyRec := conflictCopy(local)
	existing, found, needed, err := s.copyNeeded(copyRec)
	if err != nil || !needed {
		return err
	}
	blocks, err := s.Store.Blocks(local.Path)
	if err != nil {
		return err
	}
	tmp, err := s.copyFromDisk(local)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	return s.install(copyRec, blocks, tmp, existing, found)
}

func (s *Syncer) keepRemoteCopy(addr string, remote store.Record) error {
	copyRec := conflictCopy(remote)
	existing, found, needed, err := s.copyNeeded(copyRec)
	if err != nil || !needed {
		return err
	}
	info, tmp, err := s.fetch(addr, remote)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	return s.install(copyRec, info.Blocks, tmp, existing, found)
}

func (s *Syncer) copyNeeded(copyRec store.Record) (store.Record, bool, bool, error) {
	existing, found, err := s.Store.Get(copyRec.Path)
	if err != nil {
		return existing, found, false, err
	}
	needed := !found || vv.Compare(copyRec.Version, existing.Version) == vv.After
	return existing, found, needed, nil
}
