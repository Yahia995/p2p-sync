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
- [ ] /index, /file, /block, /status
- [ ] apply: temp file, hash check, rename before commit, local chunk reuse
- [ ] Anti-entropy loop with static peers
- [ ] Two-node sync tests pass

## Phase 5 — Conflicts + tombstones
- [ ] False conflict merge
- [ ] Deterministic winner, conflict naming, keep-both
- [ ] Edit beats delete
- [ ] Two-node and three-node conflict tests pass

## Phase 6 — Watcher + echo suppression
- [ ] Recursive fsnotify watcher with debounce
- [ ] 60 s periodic rescan
- [ ] No version bump on received files (test)

## Phase 7 — Failure detection + convergence
- [ ] All-to-all heartbeats, alive/suspect/dead
- [ ] In-app fault filter and /admin/faults
- [ ] scripts/netns.sh
- [ ] Partition / crash / delay tests pass
- [ ] Randomized convergence test passes for 50 seeds

## Phase 8 — Merkle anti-entropy
- [ ] Fixed 16x16 bucket tree, /merkle endpoint, walk
- [ ] One request per round when in sync (test)

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
