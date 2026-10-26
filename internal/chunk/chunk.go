package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

const Size = 128 * 1024

type Block struct {
	Hash string
	Size int64
}

func Split(r io.Reader) ([]Block, string, error) {
	var blocks []Block
	fileHash := sha256.New()
	buf := make([]byte, Size)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			sum := sha256.Sum256(buf[:n])
			blocks = append(blocks, Block{Hash: hex.EncodeToString(sum[:]), Size: int64(n)})
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
