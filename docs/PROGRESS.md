# Progress

## Planning
- [x] Step 1: brainstorm, risks, clarifying questions
- [x] Step 2: ARCHITECTURE.md, PLAN.md, TESTING.md, PROGRESS.md, DEFENSE.md
- [ ] Old PC: distro installed, CPU arch checked (`uname -m`), Go binary runs

## Phase 0 — Scaffold
- [x] go.mod, cmd/p2psync with flag parsing and tests
- [x] Makefile (build, test, race, vet, build-old, clean)
- [x] scripts/start-node.sh, scripts/firewall.sh
- [x] `make build` and `make test` pass

## Phase 1 — Version vectors
- [x] Compare, Merge, Bump
- [x] Canonical encoding (zero entries dropped, sorted keys)
- [x] Table tests and property tests pass

## Phase 2 — Chunking
- [x] 128 KiB chunks, SHA-256 per chunk and per file (matches sha256sum)
- [x] Boundary tests pass

## Phase 3 — Store + scanner
- [x] SQLite schema, WAL, node ID
- [x] Full rescan with tombstones and stability check
- [x] Store and scanner tests pass

## Phase 4 — Transport + full index exchange
- [x] /index, /file, /block (/status moved to Phase 7, first phase that uses it)
- [x] apply: temp file, hash check, rename before commit, local chunk reuse
- [x] Shared node lock; scanner re-checks disk under the lock
- [x] Anti-entropy loop with static peers (rescan every 10 s until Phase 6)
- [x] Two-node sync tests pass
- [x] Manual check: two processes on localhost converge (create, 1 MB file, delete)

## Phase 5 — Conflicts + tombstones
- [x] False conflict merge
- [x] Deterministic winner, conflict naming, keep-both (copy before winner)
- [x] Edit beats delete
- [x] Two-node and three-node conflict tests pass
- [x] Mutation check: tests fail if the winner rule or copy-first order is broken
- [x] Fixed in Phase 7: the resolver now adds its own counter to the merged vector (the "no increment" rule was wrong, found by the convergence test)

## Phase 6 — Watcher + echo suppression
- [x] Recursive fsnotify watcher with debounce (200 ms, then full rescan)
- [x] 60 s periodic rescan in the same goroutine
- [x] No version bump on received files or deletes (test + mutation check)
- [x] Manual check: new nested folder on node A reaches node B in about 4 s

## Phase 7 — Failure detection + convergence
- [x] /status endpoint
- [x] internal/node wiring (Start/Stop), main.go reduced to flags + Ctrl-C
- [x] All-to-all heartbeats, alive/suspect/dead
- [x] In-app fault filter (by peer address) and /admin/faults
- [x] scripts/netns.sh (syntax-checked; not yet run with sudo)
- [x] Crash / partition / delay / mid-download crash tests pass
- [x] Randomized convergence test passes (10 seeds in make test, 200 seeds before closing)
- [x] Bug found and fixed: equal version vectors with different content after conflict resolution
- [x] Manual check: two processes, cut with /admin/faults → suspect → dead, heal → alive, file arrives
- [ ] Run scripts/netns.sh up/run/down on the main PC (needs sudo)

## Phase 8 — Merkle anti-entropy
- [x] Fixed 16x16 bucket tree, /merkle endpoint, walk
- [x] One request per round when in sync, three for one changed file (tests)
- [x] Node anti-entropy uses SyncMerkle; convergence test passes over Merkle (200 seeds)
- [x] Mutation check: leaving the version out of the leaf, or not sorting a bucket, fails the tests

## Phase 9 — Push on change
- [ ] /push, scanner pushes to alive members
- [ ] No re-push of received records (test)

## Phase 10 — Seed + gossip
- [ ] /join, /gossip, incarnation merge, members persisted
- [ ] Five-node discovery test passes

## Phase 11 — SWIM
- [ ] ping, ping-req, suspicion, refutation, piggyback dissemination
- [ ] Indirect ping and refutation tests pass

## Phase 12 — Dashboard
- [ ] Status page with members, index, conflicts, events

## Phase 13 — Deployment and demo
- [ ] Static build runs on old PC
- [ ] Firewall opened on both machines
- [ ] Demo script rehearsed end to end
- [ ] Convergence test with 1000 seeds and -race
