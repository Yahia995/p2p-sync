# Plan

Phases are ordered by dependency and sized by effort (S ≈ 1 session, M ≈ 2–3 sessions,
L ≈ 4+ sessions). Each phase ends with passing `make test`, an updated PROGRESS.md, a suggested
commit message, the list of changed files, and a short summary of the concept it demonstrates.
The "Never cut" items (version vectors, conflict handling, failure handling, convergence test)
are all complete by the end of Phase 7.

| # | Phase | Effort | Core or stretch |
|---|---|---|---|
| 0 | Scaffold | S | core |
| 1 | Version vectors | S | core |
| 2 | Chunking | S | core |
| 3 | Store + scanner | M | core |
| 4 | Transport + full index exchange | M | core |
| 5 | Conflicts + tombstones over the network | M | core |
| 6 | Watcher + echo suppression | S | core |
| 7 | Heartbeats, fault filter, convergence test | L | core |
| 8 | Merkle anti-entropy | M | stretch |
| 9 | Push on change | S | stretch |
| 10 | Seed + gossip discovery | M | stretch |
| 11 | SWIM | L | stretch |
| 12 | Web dashboard | S | stretch |
| 13 | Two-machine deployment and demo | M | core |

---

## Phase 0 — Scaffold

Deliverables
- `go.mod` (module `p2psync`, Go 1.25), directory layout from ARCHITECTURE.md section 2.
- `Makefile`: `build`, `test`, `race`, `vet`, `build-old` (static `CGO_ENABLED=0` build for the old PC), `clean`.
- `scripts/start-node.sh`, `scripts/firewall.sh`.
- `cmd/p2psync/main.go` that parses flags and exits.

Acceptance
- `make build` and `make test` succeed on Fedora.

Tests
- None yet (empty packages compile).

## Phase 1 — Version vectors

Deliverables
- `internal/vv`: `Compare`, `Merge`, `Bump`, canonical JSON encoding.

Acceptance
- All four comparison outcomes covered, including missing keys treated as 0.

Tests
- Table tests: equal, before, after, concurrent, empty vs non-empty, missing keys.
- Properties on random vectors: `Compare(a,b)` is the mirror of `Compare(b,a)`; `Merge` is
  commutative, associative, idempotent; `Merge(a,b)` is `Equal` or `After` both inputs;
  `Bump(a,x)` is `After` a.
- Encoding: same vector built in different insertion orders gives identical bytes.

## Phase 2 — Chunking

Deliverables
- `internal/chunk`: split a reader into 128 KiB chunks, SHA-256 per chunk, SHA-256 of the whole file.

Acceptance
- Hashes match `sha256sum` for the whole file.

Tests
- Empty file, file smaller than one chunk, exactly one chunk, one chunk + 1 byte, multi-chunk.
- Changing one byte in chunk 3 changes only chunk 3's hash and the file hash.
- Concatenating chunks reproduces the original bytes.

## Phase 3 — Store + scanner

Deliverables
- `internal/store`: open DB (WAL, busy_timeout, one connection), schema, node ID creation,
  get/put/list records and blocks, lookup block by hash.
- `internal/scan`: full rescan (ARCHITECTURE.md section 6) including tombstone creation, stability
  check, skip `.p2psync/`.
- `cmd/p2psync`: `--dir` creates `.p2psync/`, runs one rescan, prints the index.

Acceptance
- New, modified, unchanged, deleted, and re-created files produce the right version vectors.

Tests
- New file → version `{self:1}`.
- Rewrite with same content → no bump.
- Modify → bump; delete → tombstone with bump; re-create → bump over tombstone.
- Nested directories; `.p2psync/` ignored.
- Node ID stays the same after reopening the DB.

## Phase 4 — Transport + full index exchange

Deliverables
- `internal/transport`: HTTP server with `/index`, `/file`, `/block`; client with timeouts. (`/status` moved to Phase 7.)
- `internal/syncer`: `handleRemote` for `After` / `Before` / `Equal`, `apply` with temp file,
  hash verification, rename-before-commit, `localDiskUnchanged`, local chunk reuse.
- Static peer list `--peers`, anti-entropy loop pulling `/index` from a random peer every 5 s.

Acceptance
- Two nodes on localhost with different folders converge after files are added, modified and
  deleted on either side (no concurrent edits yet).

Tests
- Two in-process nodes: create on A → appears on B with the same hash and version.
- Large multi-chunk file transfers correctly; modifying one chunk transfers only that chunk
  (count `/block` requests).
- Corrupted block from a peer is rejected.
- Temp files cleaned on startup.

## Phase 5 — Conflicts + tombstones over the network

Deliverables
- `Concurrent` branch: false conflict merge, `winner`, `conflictName`, keep-both
  (ARCHITECTURE.md section 8).
- Tombstone propagation and edit-beats-delete.

Acceptance
- Concurrent edits on two nodes end with identical folders on both: winner at the original path,
  loser at the conflict path.

Tests
- Unit: `winner` is order-independent for 3 random versions (all 6 permutations give the same result).
- Unit: `conflictName` for `a.txt`, `dir/a.tar.gz`, `noext`, `.hidden`.
- Two nodes edit the same file while separated → identical result, one conflict copy, no
  conflict-of-conflict.
- Three nodes all edit the same file → same result regardless of sync order.
- Delete on A, edit on B concurrently → file exists on both with B's content.
- Delete on both → tombstones merge, no conflict.

## Phase 6 — Watcher + echo suppression

Deliverables
- fsnotify watcher with recursive directory registration and debounce, periodic 60 s rescan.
- Push of nothing yet; the watcher only updates the local index faster.

Acceptance
- A file received from a peer does not cause a version bump on the receiver.

Tests
- Receive a file → receiver's version vector equals the sender's (no entry for the receiver).
- Create a nested directory with files → all files indexed.
- Write a file slowly in pieces → indexed once, after it is stable.

## Phase 7 — Heartbeats, fault filter, convergence test

Deliverables
- `/status` endpoint (node ID, members, file count) used by tests to wait for convergence.
- `internal/membership`: all-to-all heartbeats, alive/suspect/dead with timeouts; sync only with alive peers.
- Transport fault filter (drop, delay per peer) and `POST /admin/faults`.
- Randomized convergence test (TESTING.md section 3).
- `scripts/netns.sh` for same-machine network namespaces.

Acceptance
- Stopping a node marks it suspect then dead on all others within the timeouts; restarting it
  brings it back to alive and it catches up.
- Convergence test passes for 50 random seeds.

Tests
- Partition A|B, edits on both sides, heal → converge with conflict copies.
- Node killed mid-download → no partial file, converges after restart.
- Delay filter of 2 s → still converges.
- Randomized convergence test.

## Phase 8 — Merkle anti-entropy

Deliverables
- `internal/merkle`: bucket assignment, leaf/inner/root hashes, `/merkle?node=` endpoint, `walk`.
- Anti-entropy uses Merkle instead of `/index`; `/index` stays for debugging and the first sync.

Acceptance
- Two converged nodes exchange exactly one request per anti-entropy round.

Tests
- Identical stores → identical roots, independent of insertion order.
- One changed record → exactly one leaf, one inner node, and the root differ.
- Two nodes differing in one file → `walk` visits root, one inner, one leaf only.
- Convergence test passes with Merkle sync.

## Phase 9 — Push on change

Deliverables
- `/push` endpoint; scanner pushes new local versions to alive members.

Acceptance
- Change visible on peers in about 1 s instead of up to one anti-entropy period.

Tests
- With anti-entropy disabled, a change still reaches all alive peers via push.
- Received records are not re-pushed (count `/push` requests).
- Convergence test still passes with pushes randomly dropped.

## Phase 10 — Seed + gossip discovery

Deliverables
- `/join`, `/gossip`, incarnation-based merge, `members` table persistence, `--seed` flag.

Acceptance
- Five nodes started with only the seed address all know each other within a few seconds.
- A restarted node rejoins using its stored members even if the seed is down.

Tests
- Member merge rules (unknown, higher incarnation, same incarnation worse state).
- Five in-process nodes discover each other from one seed.

## Phase 11 — SWIM

Deliverables
- `/ping`, `/ping-req`, round-robin probing, suspicion timeout, incarnation refutation,
  piggybacked dissemination. Replaces all-to-all heartbeats.

Acceptance
- Dead node detected by all members; a slow-but-alive node refutes suspicion and is not declared dead.

Tests
- Drop A→C link only: C is still alive thanks to ping-req through B.
- Kill C: all members mark C dead within suspicion timeout + a few periods.
- Suspected node increments incarnation and is marked alive again.
- Count messages per period with 3 vs 8 nodes: per-node cost stays constant.

## Phase 12 — Web dashboard

Deliverables
- `internal/dashboard`: `html/template` page at `/` showing node ID, members and states, file
  index with version vectors, conflict copies, recent sync events; auto-refresh.

Acceptance
- Dashboard shows a partition and its healing live during the demo.

Tests
- Handler returns 200 and contains the node ID and a known file path.

## Phase 13 — Two-machine deployment and demo

Deliverables
- Old PC setup notes, `make build-old`, final `scripts/` for firewall, netns, tc.
- Rehearsed demo script (TESTING.md section 5).

Acceptance
- Full demo runs end to end on both machines with at least 3 nodes each.

Tests
- Convergence test passes with `-race`.
- Manual demo checklist completed.
