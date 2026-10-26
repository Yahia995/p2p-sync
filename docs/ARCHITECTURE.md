# Architecture

Leaderless peer-to-peer folder synchronization. Every node runs the same binary and has the same
components. There is no coordinator, no leader election, and no global clock. Nodes converge by
exchanging metadata (version vectors) and pulling missing content (chunks).

## 1. Scope and assumptions

| Topic | Decision |
|---|---|
| Shared folders | One shared folder per node |
| File types | Regular files in nested directories. No empty directories, symlinks, permissions, or ownership |
| Renames | Treated as delete + create |
| Network | Trusted LAN. No authentication, no TLS |
| Nodes | Several nodes per machine (different ports and folders), two physical machines |
| Identity | Random 128-bit node ID generated on first start and stored in SQLite |
| Clocks | Wall clocks are never used to order versions. They are only used locally for timeouts and change detection |
| Tombstones | Kept forever (see section 8.4) |
| Chunk size | 128 KiB fixed |

## 2. Components

```
                 +-------------------------------------------------+
                 |                     node                        |
                 |                                                 |
  disk  <------> |  scanner + watcher  ----->  store (SQLite)      |
  folder         |         |                     ^      ^          |
                 |         v                     |      |          |
                 |     sync engine  -------------+      |          |
                 |         |   ^                        |          |
                 |         v   |                        |          |
                 |     transport (net/http, JSON) <-- membership   |
                 |         ^                                       |
                 |         |              dashboard (net/http)     |
                 +---------|---------------------------------------+
                           v
                     other nodes
```

| Component | Package (planned) | Responsibility |
|---|---|---|
| Version vectors | `internal/vv` | Compare, merge, increment, canonical encoding |
| Chunking | `internal/chunk` | Split a file into 128 KiB chunks, SHA-256 per chunk and per file |
| Store | `internal/store` | SQLite access: files, blocks, members, meta |
| Scanner / watcher | `internal/scan` | Full rescan of the folder, fsnotify watcher, turns disk changes into new local versions |
| Sync engine | `internal/syncer` | Merge rule for remote records, conflict resolution, fetching chunks, applying files to disk |
| Merkle tree | `internal/merkle` | Bucket hashes over the index for anti-entropy |
| Transport | `internal/transport` | HTTP server and client, JSON messages, in-app fault filter |
| Membership | `internal/membership` | Static peers, heartbeats, seed + gossip, SWIM |
| Dashboard | `internal/dashboard` | Read-only HTML status page |
| Wiring / CLI | `cmd/p2psync` | Flags, starts all goroutines |

Concurrency model: one `sync.Mutex` per node guards every change to the store and the folder
(local scan updates and remote applies). Network I/O and chunk downloads happen outside the lock.
SQLite runs in WAL mode with `busy_timeout`, and the node uses a single `*sql.DB` with
`SetMaxOpenConns(1)`.

## 3. Data model

### 3.1 Version vector

`map[nodeID]uint64`. Missing entries mean 0.

```
compare(a, b):
    aBigger = false
    bBigger = false
    for id in keys(a) ∪ keys(b):
        if a[id] > b[id]: aBigger = true
        if b[id] > a[id]: bBigger = true
    if !aBigger and !bBigger: return Equal
    if aBigger and !bBigger: return After
    if bBigger and !aBigger: return Before
    return Concurrent

merge(a, b):   result[id] = max(a[id], b[id])
bump(a, self): copy of a with result[self] = a[self] + 1
```

Canonical encoding: JSON object with keys sorted (Go's `encoding/json` sorts map keys). That makes
the same vector always produce the same bytes, which the Merkle tree needs.

### 3.2 File record

| Field | Meaning |
|---|---|
| `path` | Relative path with `/` separators, e.g. `photos/a.jpg` |
| `version` | Version vector |
| `deleted` | `true` for a tombstone |
| `hash` | SHA-256 of the whole content (hex). Empty for tombstones |
| `size` | Bytes. 0 for tombstones |
| `blocks` | Ordered list of `{hash, size}`. Only sent when a peer asks for one file |

Local-only fields (never sent): `mtime` (nanoseconds) and the on-disk `size`. The scanner uses
them to decide whether a file needs rehashing.

### 3.3 Ignored paths

`.p2psync/` (database, temp files) is never scanned or synced. Temp downloads go to
`.p2psync/tmp/` and are removed at startup.

## 4. SQLite schema

```sql
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE files (
    path    TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    deleted INTEGER NOT NULL,
    hash    TEXT NOT NULL,
    size    INTEGER NOT NULL,
    mtime   INTEGER NOT NULL
);

CREATE TABLE blocks (
    path TEXT NOT NULL,
    idx  INTEGER NOT NULL,
    hash TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (path, idx)
);
CREATE INDEX blocks_by_hash ON blocks(hash);

CREATE TABLE members (
    id          TEXT PRIMARY KEY,
    addr        TEXT NOT NULL,
    incarnation INTEGER NOT NULL,
    state       TEXT NOT NULL
);
```

| Table | Notes |
|---|---|
| `meta` | Holds `node_id` |
| `files.version` | Canonical JSON of the version vector |
| `files.mtime` | Local only, used by the scanner |
| `blocks` | Chunk list of the current version of each file. `blocks_by_hash` lets the node serve a chunk by hash and reuse local chunks when building a new file |
| `members` | Last known membership, so a restarted node can rejoin without the seed |

## 5. Messages (HTTP + JSON)

All endpoints are on the node's listen address. Every request carries header `X-Node-ID` so the
fault filter can identify the sender.

| Method | Path | Request | Response | Used by |
|---|---|---|---|---|
| GET | `/index` | — | `[]Record` (no blocks) | Full index exchange |
| GET | `/file?path=P` | — | `Record` with `blocks` | Before downloading |
| GET | `/block?hash=H` | — | raw bytes | Chunk transfer |
| GET | `/merkle?node=X` | — | `{"node":X,"hash":..,"children":[16 hashes]}` or leaf records | Merkle anti-entropy |
| POST | `/push` | `{"from":id,"records":[]Record}` | 204 | Push on change |
| POST | `/heartbeat` | `{"from":id,"addr":..}` | 204 | All-to-all heartbeats |
| POST | `/join` | `Member` | `[]Member` | Seed + gossip |
| POST | `/gossip` | `[]Member` | `[]Member` | Seed + gossip |
| POST | `/ping` | `{"from":id,"updates":[]Member}` | `{"updates":[]Member}` | SWIM |
| POST | `/ping-req` | `{"from":id,"target":id,"updates":[]Member}` | `{"ok":bool,"updates":[]Member}` | SWIM |
| GET | `/status` | — | node state as JSON | Dashboard, CLI, tests |
| POST | `/admin/faults` | `{"peer":id,"drop":bool,"delay_ms":int}` | 204 | Fault injection |
| GET | `/` | — | HTML | Dashboard |

Example record:

```json
{
  "path": "notes/todo.txt",
  "version": {"3f9a...": 4, "b21c...": 2},
  "deleted": false,
  "hash": "9e107d9d372bb6826bd81d3542a419d6...",
  "size": 300000,
  "blocks": [
    {"hash": "a1...", "size": 131072},
    {"hash": "b2...", "size": 131072},
    {"hash": "c3...", "size": 37856}
  ]
}
```

Member:

```json
{"id": "3f9a...", "addr": "192.168.1.20:7001", "incarnation": 3, "state": "alive"}
```

`--advertise` sets the address a node announces. It is needed because the listen address
(`0.0.0.0:7001`) is not reachable by others.

## 6. Local change detection (scanner and watcher)

```
rescan():
    seen = {}
    for each regular file f under root, skipping .p2psync/:
        seen.add(f.path)
        rec = store.get(f.path)
        if rec exists and !rec.deleted and rec.size == f.size and rec.mtime == f.mtime:
            continue
        if !stable(f): continue
        blocks, fileHash = chunk.hashFile(f)
        lock()
        if rec exists and !rec.deleted and rec.hash == fileHash:
            store.updateMtime(f.path, f.mtime)
        else:
            newVersion = bump(rec.version or {}, self)
            store.put(Record{f.path, newVersion, false, fileHash, f.size}, blocks, f.mtime)
            changed.add(f.path)
        unlock()
    for each rec in store where !rec.deleted and rec.path not in seen:
        lock()
        store.put(Record{rec.path, bump(rec.version, self), true, "", 0}, no blocks, 0)
        changed.add(rec.path)
        unlock()
    if changed not empty: push(changed)
```

`stable(f)`: size and mtime unchanged over a short interval (500 ms), so half-written files are
not hashed.

Watcher: fsnotify does not watch subfolders on its own. The watcher adds a watch on every directory at start
and on every newly created directory. Each event schedules a debounced rescan of the affected path
(rescan of a single path uses the same logic as above). A full rescan still runs every 60 s
to catch missed events and changes made while the node was down.

Echo suppression: when the sync engine writes a file received from a peer, it stores the record
with the new hash and the post-rename mtime while holding the lock. When the watcher event for that
write arrives, the scanner sees `size` and `mtime` equal to the record (or the same hash after
rehashing), so it does not bump the version and does not push. No timers or "ignore lists" are
involved; the rule is purely "content equal to the stored record means no new version".

## 7. Sync algorithm

### 7.1 Merge rule for one remote record

```
handleRemote(peer, r):
    l = store.get(r.path)
    if l is none:
        apply(peer, r)
        return
    switch compare(r.version, l.version):
        Equal, Before:
            return
        After:
            apply(peer, r)
        Concurrent:
            if r.deleted == l.deleted and r.hash == l.hash:
                store.setVersion(r.path, merge(l.version, r.version))
                return
            resolveConflict(peer, l, r)
```

### 7.2 Apply a remote version

```
apply(peer, r):
    if r.deleted:
        lock()
        if localDiskUnchanged(r.path): remove file from disk
        store.put(r)
        unlock()
        return
    full = peer.GET /file?path=r.path
    if full.version != r.version: return
    tmp = new file in .p2psync/tmp
    for each block b in full.blocks:
        data = localBlock(b.hash) or peer.GET /block?hash=b.hash
        if sha256(data) != b.hash: abort
        write data to tmp
    if sha256(tmp) != full.hash: abort
    lock()
    if !localDiskUnchanged(r.path):
        unlock(); delete tmp; return
    rename tmp -> root/r.path
    store.put(full, blocks, mtime after rename)
    unlock()
```

`localDiskUnchanged(path)`: the file on disk matches the stored record (same size and mtime, or
absent if the record is a tombstone or missing). If the user edited the file while it was being
downloaded, the apply is abandoned; the scanner will turn the edit into a new local version, which
is then concurrent with `r` and goes through conflict resolution. No user data is overwritten.

Crash safety: the rename happens before the store update. If the node crashes between the two,
the next rescan sees content that differs from the stored record and creates a local version with
the same hash as `r`. That version is concurrent with `r` but has equal content, so 7.1 merges the
vectors without a conflict.

`localBlock(hash)`: looks up `blocks_by_hash`, reads the bytes from the local file, and verifies the
hash. This reuses unchanged chunks of a modified file.

### 7.3 Anti-entropy: full index exchange (first version)

```
every 5 s:
    peer = random alive member
    for r in peer.GET /index:
        handleRemote(peer, r)
```

Pull-only: every node pulls from others. Because the peer is random and every node does this,
each update eventually reaches every node that can communicate (epidemic propagation).

### 7.4 Anti-entropy: Merkle tree (replaces 7.3 as the main path)

Each record goes into one of 256 buckets: the first byte of `SHA-256(path)`, as two hex chars.
The tree has fixed shape: root, 16 inner nodes (first hex char), 256 leaves (two hex chars).

```
leafHash(bucket)   = SHA-256 of concatenated lines "path|canonicalVersion|deleted|hash\n"
                     for all records in the bucket, sorted by path
innerHash(c)       = SHA-256 of the 16 leaf hashes c0..cf
rootHash           = SHA-256 of the 16 inner hashes 0..f
```

A fixed shape matters: if the tree were built over the sorted path list, one insert would shift
every leaf after it and nothing would match. With fixed buckets one change touches one leaf, one
inner node, and the root.

```
syncMerkle(peer):
    walk(peer, "")

walk(peer, node):
    remote = peer.GET /merkle?node=node
    if remote.hash == local.hash(node): return
    if node is a leaf:
        for r in remote.records: handleRemote(peer, r)
        return
    for i, child in children(node):
        if remote.children[i] != local.hash(child): walk(peer, child)
```

Hashes are recomputed from the store when needed (cached and invalidated on writes if profiling
shows it matters). When two nodes agree, one request (the root) is enough.

### 7.5 Push on change

After the scanner creates new local versions, the node sends `POST /push` with those records to
every alive member. The receiver runs `handleRemote(sender, r)` for each one in a goroutine.
Push only lowers latency; if a push is lost, anti-entropy still delivers the change. Received
remote changes are not re-pushed, so there is no flooding; other nodes get them from anti-entropy
or from their own pull.

## 8. Conflict rules

### 8.1 When is it a conflict

Two versions conflict when their version vectors are `Concurrent` and their content differs
(different `hash` or different `deleted`). Concurrent versions with equal content are merged
silently (false conflict).

### 8.2 Choosing the winner

The winner must be the same on every node, no matter in which order a node sees the versions,
and must not depend on clocks. The rule is a total order on content only:

1. A non-deleted version beats a tombstone (edit beats delete, so no data is lost).
2. Otherwise the larger `hash` (string compare) wins.

Because the key depends only on the versions themselves, `winner(winner(a, b), c) ==
winner(a, winner(b, c))`. Three-way conflicts resolve the same everywhere.

### 8.3 Keep-both

```
resolveConflict(peer, l, r):
    w = winner(l, r)
    lose = the other one
    merged = merge(l.version, r.version)
    if !lose.deleted:
        copyPath = conflictName(lose.path, lose.hash)
        handleLocalCopy(copyPath, lose)
    install w at path with version = merged
```

- `conflictName("dir/report.txt", h) = "dir/report.conflict-" + h[0:8] + ".txt"`.
- The conflict copy record is `{path: copyPath, version: lose.version, hash: lose.hash, blocks:
  lose.blocks}`. Every node that resolves the same conflict creates byte-identical records, so
  the copies never conflict with each other.
- The winning record gets `merge(l.version, r.version)` with **no** increment. Every node computes the
  same vector and content, so they end up `Equal`. A real later edit increments a counter and
  dominates the merged vector.
- If the loser is the local version, its content is already on disk: it is copied to `copyPath`
  before the winner is written. If the loser is remote, its chunks are fetched like in `apply`.
- The conflict copy goes through the same merge rule as any remote record: if a record at
  `copyPath` already exists and is `Equal` or `After` (for example the user already deleted the
  copy on another node), nothing is created.
- Nodes that never saw both sides simply receive the merged winner and the conflict copy through
  normal sync.

This changes the naming proposed in Step 1 (`<nodeID8>-<hash8>`): a node ID is not a property of
the content, so using only the hash keeps the name deterministic.

### 8.4 Tombstones

- Deleting a file creates a record `{deleted: true, version: bump(old, self)}`.
- Recreating a file at a tombstoned path bumps the tombstone's vector, so the new file dominates.
- Delete concurrent with edit: the edit wins (8.2), the file survives on every node.
- Tombstones are never removed. Removing one safely requires knowing that every node, including
  nodes that are currently offline, has seen it. Otherwise an offline node that still has the file
  would bring it back on reconnect. Collecting that knowledge needs stable, complete membership,
  which a leaderless system with nodes joining and leaving does not have. The cost is a few
  hundred bytes per deleted path.

## 9. Membership and failure detection

Member states: `alive`, `suspect`, `dead`. Sync and push only target `alive` members.

### 9.1 Static peers + all-to-all heartbeats (first version)

```
every 1 s: send POST /heartbeat to every configured peer
on heartbeat from p: lastHeard[p] = now; state[p] = alive
every 1 s: for p:
    if now - lastHeard[p] > 3 s:  state[p] = suspect
    if now - lastHeard[p] > 10 s: state[p] = dead
```

Message cost: O(n²) per second. Fine for under 10 nodes, and the reason to move to SWIM.

### 9.2 Seed + gossip discovery

```
start: POST /join to the seed address -> member list
every 1 s: pick 2 random alive members, POST /gossip with the local list, merge the answer
merge(m):
    if m.id unknown: add
    else if m.incarnation > local.incarnation: replace
    else if equal incarnation and m.state is worse (alive < suspect < dead): take m.state
```

Only the node itself increases its own incarnation. That number lets a node refute a false
"suspect" rumor about itself.

### 9.3 SWIM (replaces 9.1)

```
every 1 s (protocol period):
    target = next member in a shuffled round-robin list
    if ping(target) answers within 300 ms: alive
    else:
        ask k = 3 other members: ping-req(target)
        if any reports ok before the period ends: alive
        else: mark target suspect, disseminate
suspect for longer than 5 s: mark dead, disseminate
a node that hears it is suspected: incarnation += 1, disseminate alive
```

Dissemination: each ping and ack carries up to 6 recent membership updates (piggybacking), each
sent about `3 * log(n)` times. Message cost per node per period is constant, independent of `n`.

## 10. Failure scenarios

| Scenario | What happens |
|---|---|
| Peer crashes during chunk download | Request fails, temp file deleted, nothing committed. The next anti-entropy round pulls the same version from another peer |
| Node crashes while writing a received file | Temp file deleted on restart. Rename-before-commit plus the false-conflict rule keep the store consistent (7.2) |
| Node is offline while files change | On restart: full rescan detects its own offline edits, anti-entropy fetches the rest. Persisted node ID keeps its version vector entries valid |
| Network partition, both sides edit the same file | Versions become concurrent. After healing, anti-entropy detects it and keep-both resolution runs identically on every node |
| Partition, one side deletes, other edits | Edit wins, file restored on the deleting side |
| Message lost or duplicated | All handlers are idempotent: applying the same record twice compares `Equal` the second time |
| Push lost | Anti-entropy delivers the change later |
| False failure suspicion (slow node) | SWIM indirect ping and incarnation refutation; even a wrong `dead` only pauses syncing with that node |
| File changes while a peer reads a chunk | Chunk hash check fails at the receiver, download aborted and retried later |
| Clock skew between machines | No effect: ordering uses version vectors and hashes only |
| User edits a file while it is being replaced | `localDiskUnchanged` check aborts the apply; the edit becomes a new version (7.2) |

## 11. Fault injection hooks

- In-app filter in the transport: per peer ID, `drop` (reject incoming and refuse outgoing
  requests) and `delay_ms`. Set from tests directly, or at runtime via `POST /admin/faults`.
  Works for nodes on the same machine, where tc and firewalld cannot separate them.
- Network namespaces (`scripts/netns.sh`): each local node in its own namespace connected through
  a bridge, so `tc netem` and `nft` rules apply per node. Needs root.
- Between machines: `tc netem` on the physical interface and firewalld rich rules (see TESTING.md).

## 12. Dependencies

| Module | Why |
|---|---|
| `modernc.org/sqlite` | SQLite driver in pure Go. No cgo, so `CGO_ENABLED=0` static builds run on the old PC regardless of its distro |
| `github.com/fsnotify/fsnotify` | Go wrapper around inotify. Writing raw inotify syscalls by hand adds nothing to the distributed-systems part |

Everything else uses the standard library (`net/http`, `encoding/json`, `crypto/sha256`,
`database/sql`, `math/rand/v2`, `html/template`).
