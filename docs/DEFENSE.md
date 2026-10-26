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
| Merkle tree | `internal/merkle`, `syncer.SyncMerkle` | Compare whole indexes with one hash, then descend only into differing buckets |
| Epidemic / gossip dissemination | `membership` gossip, SWIM piggyback | Random peer selection spreads information in O(log n) rounds |
| Failure detection | `internal/membership` (heartbeats), `internal/node` (sync only with alive peers) | Heartbeat timeouts first, then SWIM; failures can only be suspected, never known for sure |
| SWIM | `membership` ping/ping-req | Constant per-node load, indirect probes reduce false positives, incarnation numbers refute rumors |
| Idempotence | `handleRemote` | Same record twice → `Equal` → no-op; makes retries and duplicates safe |
| Eventual consistency | `internal/node/convergence_test.go` | No guarantee about when, only that nodes agree once updates stop and the network heals |
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

**Why does the node that resolves a conflict add its own counter to the merged vector?**
To keep the invariant "same version vector ⇒ same content". Without the increment, two different
pairs of concurrent versions can merge to the same vector and pick different winners, for example
`{P:1}`+`{Q:3}` and `{P:1,Q:2}`+`{Q:3}` both give `{P:1,Q:3}`. Two nodes then hold `Equal` vectors with
different files, and nothing ever repairs it, because `Equal` means "already in sync". Any operation
that chooses content needs a fresh counter, like a local edit. The cost: two nodes that resolve the
same conflict independently get concurrent vectors with equal content, and the false-conflict rule
merges them one round later.

**Did the tests ever find a real bug?**
Yes, this one. Phase 5 used the merge without the increment, and I had even argued it was only an
optimization. The randomized convergence test in Phase 7 failed for seed 2 about 1 run in 8, with
`docs/d.txt` showing the same vector `{966a…:1, fb74…:3}` on two nodes but different hashes. Fixing it
meant adding the resolver's counter and changing the Phase 5 test that had asserted the wrong
behavior. Seed 2 then passed 30 out of 30 runs. Lesson: hand-written reasoning about concurrent
histories misses cases; randomized testing with partitions and restarts finds them.

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

**What does the Merkle tree save?**
Two nodes that agree exchange one small request per anti-entropy round (the root and its 16 child
hashes) instead of the whole index. One changed file costs 3 requests. The tests count these.

**What goes into a leaf hash, and why not mtime?**
`path|version|deleted|hash` for each record in the bucket, sorted by path. mtime is local (it differs
between nodes for the same content) and would make in-sync nodes look different forever. Leaving
the version vector out would be a bug: two nodes with the same content but different vectors (for
example after a resolution) would look in sync and never exchange the newer vector. A mutation test
checks both.

**How do you know it converges?**
The randomized convergence test: random edits, partitions, delays, restarts, then heal and check
that all folders and indexes are identical. Any failing seed can be replayed exactly.

**What happens on a partition?**
Both sides keep working. Edits to the same file on both sides become concurrent; after healing, the
deterministic keep-both rule runs on every node and they agree.

**Why HTTP/JSON?**
Standard library only, easy to inspect with `curl`, and the payloads are small except chunks,
which are sent as raw bytes.
