package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"p2psync/internal/chunk"
	"p2psync/internal/merkle"
	"p2psync/internal/store"
)

type FileInfo struct {
	store.Record
	Blocks []chunk.Block `json:"blocks"`
}

type handler struct {
	store *store.Store
	root  string
}

func NewHandler(st *store.Store, root string) *http.ServeMux {
	h := &handler{store: st, root: root}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /index", h.index)
	mux.HandleFunc("GET /file", h.file)
	mux.HandleFunc("GET /block", h.block)
	mux.HandleFunc("GET /merkle", h.merkle)
	return mux
}

func (h *handler) index(w http.ResponseWriter, r *http.Request) {
	records, err := h.store.All()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	WriteJSON(w, records)
}

func (h *handler) file(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	rec, found, err := h.store.Get(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	blocks, err := h.store.Blocks(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	WriteJSON(w, FileInfo{Record: rec, Blocks: blocks})
}

func (h *handler) block(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	path, offset, size, found, err := h.store.FindBlock(hash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	data, err := chunk.ReadBlock(filepath.Join(h.root, filepath.FromSlash(path)), offset, size, hash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

func (h *handler) merkle(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("node")
	if !merkle.ValidID(id) {
		http.Error(w, "node must be empty, one or two lowercase hex digits", http.StatusBadRequest)
		return
	}
	records, err := h.store.All()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	WriteJSON(w, merkle.Build(records).Node(id))
}

func WriteJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(value)
}

type Client struct {
	http   *http.Client
	self   string
	faults *Faults
}

func NewClient(self string, timeout time.Duration, faults *Faults) *Client {
	return &Client{http: &http.Client{Timeout: timeout}, self: self, faults: faults}
}

func (c *Client) Index(addr string) ([]store.Record, error) {
	var records []store.Record
	err := c.getJSON(addr, "/index", nil, &records)
	return records, err
}

func (c *Client) File(addr, path string) (FileInfo, error) {
	var info FileInfo
	err := c.getJSON(addr, "/file", url.Values{"path": {path}}, &info)
	return info, err
}

func (c *Client) Merkle(addr, id string) (merkle.Node, error) {
	var node merkle.Node
	err := c.getJSON(addr, "/merkle", url.Values{"node": {id}}, &node)
	return node, err
}

func (c *Client) Block(addr, hash string) ([]byte, error) {
	resp, err := c.get(addr, "/block", url.Values{"hash": {hash}})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, chunk.Size))
}

func (c *Client) getJSON(addr, path string, query url.Values, out any) error {
	resp, err := c.get(addr, path, query)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) PostJSON(addr, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := c.do(http.MethodPost, addr, path, nil, bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *Client) get(addr, path string, query url.Values) (*http.Response, error) {
	return c.do(http.MethodGet, addr, path, query, nil)
}

func (c *Client) do(method, addr, path string, query url.Values, body io.Reader) (*http.Response, error) {
	rule := c.faults.rule(addr)
	if rule.Drop {
		return nil, fmt.Errorf("%s %s: dropped by fault filter", addr, path)
	}
	time.Sleep(time.Duration(rule.DelayMs) * time.Millisecond)

	target := url.URL{Scheme: "http", Host: addr, Path: path, RawQuery: query.Encode()}
	req, err := http.NewRequest(method, target.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(peerHeader, c.self)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		resp.Body.Close()
		return nil, fmt.Errorf("%s %s: %s", addr, path, resp.Status)
	}
	return resp, nil
}
