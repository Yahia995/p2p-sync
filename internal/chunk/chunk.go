package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

const Size = 128 * 1024

var ErrHashMismatch = errors.New("block hash mismatch")

type Block struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func Split(r io.Reader) ([]Block, string, error) {
	var blocks []Block
	fileHash := sha256.New()
	buf := make([]byte, Size)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			blocks = append(blocks, Block{Hash: Hash(buf[:n]), Size: int64(n)})
			fileHash.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, "", err
		}
	}
	return blocks, hex.EncodeToString(fileHash.Sum(nil)), nil
}

func HashFile(path string) ([]Block, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	return Split(f)
}

func ReadBlock(path string, offset, size int64, hash string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data := make([]byte, size)
	if _, err := f.ReadAt(data, offset); err != nil {
		return nil, err
	}
	if Hash(data) != hash {
		return nil, ErrHashMismatch
	}
	return data, nil
}
