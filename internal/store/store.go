package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"

	"p2psync/internal/chunk"
	"p2psync/internal/vv"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
	path    TEXT PRIMARY KEY,
	version TEXT NOT NULL,
	deleted INTEGER NOT NULL,
	hash    TEXT NOT NULL,
	size    INTEGER NOT NULL,
	mtime   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS blocks (
	path TEXT NOT NULL,
	idx  INTEGER NOT NULL,
	hash TEXT NOT NULL,
	size INTEGER NOT NULL,
	PRIMARY KEY (path, idx)
);
CREATE INDEX IF NOT EXISTS blocks_by_hash ON blocks(hash);
`

type Record struct {
	Path    string
	Version vv.Vector
	Deleted bool
	Hash    string
	Size    int64
	MTime   int64
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, stmt := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000", schema} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) NodeID() (string, error) {
	var id string
	err := s.db.QueryRow("SELECT value FROM meta WHERE key = 'node_id'").Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	random := make([]byte, 16)
	rand.Read(random)
	id = hex.EncodeToString(random)
	_, err = s.db.Exec("INSERT INTO meta (key, value) VALUES ('node_id', ?)", id)
	return id, err
}

func (s *Store) Get(path string) (Record, bool, error) {
	row := s.db.QueryRow("SELECT path, version, deleted, hash, size, mtime FROM files WHERE path = ?", path)
	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return rec, true, nil
}

func (s *Store) All() ([]Record, error) {
	rows, err := s.db.Query("SELECT path, version, deleted, hash, size, mtime FROM files ORDER BY path")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

func (s *Store) Put(rec Record, blocks []chunk.Block) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO files (path, version, deleted, hash, size, mtime) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (path) DO UPDATE SET version = excluded.version, deleted = excluded.deleted,
		hash = excluded.hash, size = excluded.size, mtime = excluded.mtime`,
		rec.Path, vv.Encode(rec.Version), rec.Deleted, rec.Hash, rec.Size, rec.MTime)
	if err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM blocks WHERE path = ?", rec.Path); err != nil {
		return err
	}
	for i, b := range blocks {
		if _, err := tx.Exec("INSERT INTO blocks (path, idx, hash, size) VALUES (?, ?, ?, ?)", rec.Path, i, b.Hash, b.Size); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) SetMTime(path string, mtime int64) error {
	_, err := s.db.Exec("UPDATE files SET mtime = ? WHERE path = ?", mtime, path)
	return err
}

func (s *Store) Blocks(path string) ([]chunk.Block, error) {
	rows, err := s.db.Query("SELECT hash, size FROM blocks WHERE path = ? ORDER BY idx", path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []chunk.Block
	for rows.Next() {
		var b chunk.Block
		if err := rows.Scan(&b.Hash, &b.Size); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

func (s *Store) FindBlock(hash string) (path string, offset int64, size int64, found bool, err error) {
	var idx int64
	err = s.db.QueryRow("SELECT path, idx, size FROM blocks WHERE hash = ? LIMIT 1", hash).Scan(&path, &idx, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, 0, false, nil
	}
	if err != nil {
		return "", 0, 0, false, err
	}
	return path, idx * chunk.Size, size, true, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRecord(row rowScanner) (Record, error) {
	var rec Record
	var version string
	if err := row.Scan(&rec.Path, &version, &rec.Deleted, &rec.Hash, &rec.Size, &rec.MTime); err != nil {
		return Record{}, err
	}
	v, err := vv.Decode(version)
	if err != nil {
		return Record{}, err
	}
	rec.Version = v
	return rec, nil
}
