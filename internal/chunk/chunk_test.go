package chunk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

func hashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func randomData(n int) []byte {
	rng := rand.New(rand.NewPCG(1, 2))
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rng.IntN(256))
	}
	return data
}

func checkBlocks(t *testing.T, data []byte, blocks []Block, fileHash string) {
	t.Helper()
	if fileHash != hashOf(data) {
		t.Errorf("file hash = %s, want %s", fileHash, hashOf(data))
	}
	var rebuilt []byte
	for i, b := range blocks {
		start := i * Size
		end := min(start+Size, len(data))
		part := data[start:end]
		if b.Size != int64(len(part)) {
			t.Errorf("block %d size = %d, want %d", i, b.Size, len(part))
		}
		if b.Hash != hashOf(part) {
			t.Errorf("block %d hash mismatch", i)
		}
		rebuilt = append(rebuilt, part...)
	}
	if !bytes.Equal(rebuilt, data) {
		t.Error("blocks do not cover the data exactly")
	}
}

func TestSplitBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		wantBlocks int
	}{
		{"empty", 0, 0},
		{"one byte", 1, 1},
		{"smaller than a chunk", Size - 1, 1},
		{"exactly one chunk", Size, 1},
		{"one chunk plus one byte", Size + 1, 2},
		{"exactly three chunks", 3 * Size, 3},
		{"many chunks", 5*Size + 1234, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := randomData(tt.size)
			blocks, fileHash, err := Split(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if len(blocks) != tt.wantBlocks {
				t.Fatalf("got %d blocks, want %d", len(blocks), tt.wantBlocks)
			}
			checkBlocks(t, data, blocks, fileHash)
		})
	}
}

func TestOneByteChangeAffectsOneChunk(t *testing.T) {
	data := randomData(5 * Size)
	before, beforeHash, _ := Split(bytes.NewReader(data))

	changed := bytes.Clone(data)
	changed[2*Size+100] ^= 0xff
	after, afterHash, _ := Split(bytes.NewReader(changed))

	if beforeHash == afterHash {
		t.Error("file hash did not change")
	}
	for i := range before {
		same := before[i].Hash == after[i].Hash
		if i == 2 && same {
			t.Errorf("chunk %d should have changed", i)
		}
		if i != 2 && !same {
			t.Errorf("chunk %d should not have changed", i)
		}
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("disk error")
}

func TestSplitReturnsReadError(t *testing.T) {
	if _, _, err := Split(failingReader{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestHashFile(t *testing.T) {
	data := randomData(2*Size + 7)
	path := filepath.Join(t.TempDir(), "file.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	blocks, fileHash, err := HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	checkBlocks(t, data, blocks, fileHash)

	if _, _, err := HashFile(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("expected error for missing file")
	}
}

func TestReadBlock(t *testing.T) {
	data := randomData(2*Size + 10)
	path := filepath.Join(t.TempDir(), "file.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	blocks, _, _ := HashFile(path)

	got, err := ReadBlock(path, 2*Size, blocks[2].Size, blocks[2].Hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[2*Size:]) {
		t.Error("wrong bytes for last block")
	}

	if _, err := ReadBlock(path, Size, blocks[1].Size, blocks[0].Hash); !errors.Is(err, ErrHashMismatch) {
		t.Errorf("expected ErrHashMismatch, got %v", err)
	}
	if _, err := ReadBlock(path, 3*Size, Size, blocks[0].Hash); err == nil {
		t.Error("expected error reading past end of file")
	}
}
