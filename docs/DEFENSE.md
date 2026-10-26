# Defense notes

Where each concept lives and how to explain it. File paths are the planned ones from
ARCHITECTURE.md and get filled in as phases are completed.

## Concepts and where they are

| Concept | Where | One-line explanation |
|---|---|---|
| Leaderless / symmetric design | `cmd/p2psync`, every package | Every node runs the same code; no node has a special role after startup (the seed is only a first contact) |
| Version vectors | `internal/vv` | One counter per node per file; captures the "happened-before" relation between versions |
| Causality and concurrency | `vv.Compare` | `After` = the other version is an ancestor; `Concurrent` = neither saw the other, so it is a conflict |
| Conflict detection | `syncer.handleRemote` | Concurrent vectors and different content |
| Deterministic conflict resolution | `syncer.winner`, `syncer.conflictName` | A total order on content (non-deleted first, then larger hash). It does not depend on arrival order, so all nodes pick the same winner |
| Keep-both | `syncer.resolveConflict` (`internal/syncer/conflict.go`) | Loser saved as `name.conflict-<hash8>.ext`; no data loss |
| Tombstones | `scan`, `syncer.apply` | A delete is a new version with `deleted=true`, so it propagates and can be ordered against edits |
| Content addressing / chunking | `internal/chunk`, `/block` | Chunks named by SHA-256; transfer only missing chunks; integrity check for free |
| Anti-entropy | `syncer` loop | Periodic pull from a random peer repairs anything that push missed |
| Merkle tree | `internal/merkle` | Compare whole indexes with one hash, then descend only into differing buckets |
| Epidemic / gossip dissemination | `membership` gossip, SWIM piggyback | Random peer selection spreads information in O(log n) rounds |
| Failure detection | `internal/membership` | Heartbeat timeouts first, then SWIM; failures can only be suspected, never known for sure |
| SWIM | `membership` ping/ping-req | Constant per-node load, indirect probes reduce false positives, incarnation numbers refute rumors |
| Idempotence | `handleRemote` | Same record twice → `Equal` → no-op; makes retries and duplicates safe |
| Eventual consistency | convergence test | No guarantee about when, only that nodes agree once updates stop and the network heals |
| Atomic local updates | `syncer.apply` | Temp file + hash check + rename, then commit; crash between steps is handled by the false-conflict rule |
| Echo suppression | `scan` | Content equal to the stored record never creates a new version |

## CAP position

During a partition, both sides keep accepting writes (availability). Consistency is eventual: after
healing, concurrent writes are detected and resolved with keep-both. The system is AP.

## Likely questions

**Why version vectors and not timestamps?**
Clocks on two machines drift, and a timestamp cannot tell "B edited after seeing A's edit" from
"A and B edited independently". Version vectors can, which is exactly what conflict detection needs.

**Why a vector per file and not per node?**
A per-node vector would make every edit to any file look concurrent with every other node's edits.
Per-file vectors only report conflicts on the file that really diverged.

**Why not use the latest modification time as the conflict winner?**
It depends on clocks, and two nodes can have the same mtime. The winner must be a pure function of
the two versions so every node chooses the same one regardless of when it learns about them.

**Is the winner "fair"?**
No, it is arbitrary but deterministic. Fairness does not matter because keep-both means nothing is lost.

**Why does the merged winner not increment any counter?**
Without an increment, every node that resolves the same conflict on its own produces exactly the
same record (same vector, same content), so they agree immediately. With an increment, each
resolver would produce a different vector. They would still converge, because those versions are
concurrent with equal content and get merged as a false conflict, but each resolver would leave its
ID in the vector and cost an extra sync round. This was checked by a mutation test: adding the
increment makes `TestConcurrentEditsKeepBoth/both_resolve` fail on the "identical right after
resolving" check, while the final convergence still holds.

**What if the node crashes in the middle of resolving a conflict?**
The loser's copy is written first and the winner second. A crash after the copy leaves the copy
(harmless, it is the same record every node would create) and the next round installs the winner.
A crash before the copy changes nothing.

**Why keep tombstones forever?**
To remove a tombstone safely you must know that every node has seen it, including offline nodes.
Without that, an old node would re-introduce the deleted file. Leaderless membership cannot
guarantee that knowledge, so the trade-off is a small, growing amount of metadata.

**What if a node is wrongly declared dead?**
Nothing breaks: sync just pauses with that node. When it answers again, it is marked alive and
anti-entropy catches up. Failure detection here affects efficiency, not correctness.

**How do you avoid the node re-announcing a file it just received?**
After writing it, the stored record already has that content and mtime. The scanner only creates a
version when the disk differs from the stored record. The mtime check is just a shortcut; the hash
check is what really guarantees it. No "ignore this path for 2 seconds" timers.

**Why does every watcher event trigger a full rescan instead of handling just that file?**
It is simpler and avoids the inotify race where files are created inside a new directory before
the watch on that directory exists. A rescan of unchanged files only costs a `stat` each.

**Why fixed-size chunks? What is their weakness?**
Simple and enough to show content addressing. Weakness: inserting a byte near the start shifts all
later chunks, so they all look new. Content-defined chunking (rolling hash) fixes that and is out of scope.

**Why a fixed-shape Merkle tree?**
Both nodes must put the same file in the same leaf. Bucketing by hash of the path gives that;
a tree over the sorted list would shift on every insert.

**How do you know it converges?**
The randomized convergence test: random edits, partitions, delays, restarts, then heal and check
that all folders and indexes are identical. Any failing seed can be replayed exactly.

**What happens on a partition?**
Both sides keep working. Edits to the same file on both sides become concurrent; after healing, the
deterministic keep-both rule runs on every node and they agree.

**Why HTTP/JSON?**
Standard library only, easy to inspect with `curl`, and the payloads are small except chunks,
which are sent as raw bytes.
