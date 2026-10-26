package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"p2psync/internal/store"
	"p2psync/internal/vv"
)

const hexDigits = "0123456789abcdef"

type Node struct {
	ID       string         `json:"node"`
	Hash     string         `json:"hash"`
	Children []string       `json:"children,omitempty"`
	Records  []store.Record `json:"records,omitempty"`
}

type Tree struct {
	buckets map[string][]store.Record
	hashes  map[string]string
}

func Bucket(path string) string {
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:1])
}

func ValidID(id string) bool {
	if len(id) > 2 {
		return false
	}
	for _, c := range id {
		if !strings.ContainsRune(hexDigits, c) {
			return false
		}
	}
	return true
}

func IsLeaf(id string) bool {
	return len(id) == 2
}

func Children(id string) []string {
	var children []string
	for _, c := range hexDigits {
		children = append(children, id+string(c))
	}
	return children
}

func Build(records []store.Record) *Tree {
	t := &Tree{buckets: map[string][]store.Record{}, hashes: map[string]string{}}
	for _, r := range records {
		b := Bucket(r.Path)
		t.buckets[b] = append(t.buckets[b], r)
	}
	for _, inner := range Children("") {
		var innerInput strings.Builder
		for _, leaf := range Children(inner) {
			t.hashes[leaf] = leafHash(t.buckets[leaf])
			innerInput.WriteString(t.hashes[leaf])
		}
		t.hashes[inner] = hashString(innerInput.String())
	}
	var rootInput strings.Builder
	for _, inner := range Children("") {
		rootInput.WriteString(t.hashes[inner])
	}
	t.hashes[""] = hashString(rootInput.String())
	return t
}

func leafHash(records []store.Record) string {
	sort.Slice(records, func(i, j int) bool { return records[i].Path < records[j].Path })
	var input strings.Builder
	for _, r := range records {
		fmt.Fprintf(&input, "%s|%s|%t|%s\n", r.Path, vv.Encode(r.Version), r.Deleted, r.Hash)
	}
	return hashString(input.String())
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (t *Tree) Hash(id string) string {
	return t.hashes[id]
}

func (t *Tree) Node(id string) Node {
	n := Node{ID: id, Hash: t.hashes[id]}
	if IsLeaf(id) {
		n.Records = t.buckets[id]
		return n
	}
	for _, child := range Children(id) {
		n.Children = append(n.Children, t.hashes[child])
	}
	return n
}
