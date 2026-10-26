# Testing

Commands:

```
make test    # go test ./...
make race    # go test -race ./...
go test ./internal/syncer -run Convergence -count=1 -v
```

## 1. Unit tests

| Package | What is tested |
|---|---|
| `internal/vv` | Compare (4 outcomes, missing keys), Merge (commutative, associative, idempotent), Bump, canonical encoding |
| `internal/chunk` | Boundaries (0, <1 chunk, =1 chunk, +1 byte, many), one-byte change affects one chunk, reassembly |
| `internal/store` | Schema creation, node ID persistence, put/get records, block lookup by hash |
| `internal/scan` | New / unchanged / same-content rewrite / modify / delete / re-create, nested dirs, `.p2psync/` ignored, stability check |
| `internal/syncer` | `winner` order independence, `conflictName`, false conflict, edit beats delete, apply aborts when local file changed |
| `internal/merkle` | Root independent of insertion order, one change touches one path in the tree |
| `internal/membership` | Heartbeat timeouts, gossip merge rules, SWIM suspicion and refutation |

Random property tests use `math/rand/v2` with a fixed seed printed on failure.

## 2. Integration tests (in-process nodes)

Tests start several nodes in one process, each with its own `t.TempDir()` folder and an
`httptest`-style listener on `127.0.0.1:0`. Timers (anti-entropy period, heartbeat timeouts) are
shortened through the node's config struct so tests run in seconds.

Waiting: tests poll `/status` (or the store directly) until a condition holds or a deadline
expires. No fixed sleeps.

Scenarios:
- Create / modify / delete on A, check B.
- Concurrent edit on A and B while the link is dropped, heal, check both folders.
- Three-way concurrent edit.
- Delete vs edit.
- Kill a node mid-download, restart it.
- Delay filter on all links.
- Partition and heal with membership (dead → alive).

## 3. Randomized convergence test

Goal: whatever happens, once the network heals and edits stop, all nodes end with identical
folders and identical indexes.

```
TestConvergence(seed):
    rng = rand.New(seed)
    start N = 5 nodes, full mesh, short timers
    repeat 200 steps:
        pick an action with rng:
            create file   (random node, random path from a small set of 10 paths, random content)
            modify file   (existing path on random node)
            delete file   (existing path on random node)
            partition     (random split of nodes into two groups, drop links between them)
            heal          (remove all drop rules)
            delay link    (random pair, random delay up to 200 ms)
            restart node  (stop, keep folder and DB, start again)
        short random pause (0–50 ms)
    heal everything, stop issuing edits
    wait until every node reports the same Merkle root (or same index), with a deadline
    assert:
        all folders have the same set of paths and the same bytes per path
        all indexes have the same records (path, version, deleted, hash)
        no file in .p2psync/tmp
        every conflict copy name matches its content hash
    on failure: print seed, the action log, and a diff of the indexes
```

A small path set (10 paths) is chosen on purpose: it forces many concurrent edits on the same files.
The test runs with 50 seeds in `make test`; a longer run (`-run Convergence -count=1` with a
`CONVERGENCE_SEEDS=1000` environment variable) is used before the demo.

## 4. Fault injection

### 4.1 In-app filter (tests and same-machine demo)

```
curl -X POST localhost:7001/admin/faults -d '{"peer":"<id of node2>","drop":true}'
curl -X POST localhost:7001/admin/faults -d '{"peer":"<id of node2>","drop":false,"delay_ms":500}'
```

A dropped link fails in both directions (incoming requests from that peer get 503, outgoing
requests to it are not sent).

### 4.2 Network namespaces (same machine, real network faults)

`scripts/netns.sh up 3` creates namespaces `p2p1..p2p3`, a bridge `br-p2p`, one veth pair per
namespace, addresses `10.77.0.1..3/24`. Each node is started inside its namespace:

```
sudo ip netns exec p2p1 ./bin/p2psync --dir nodes/n1 --listen 10.77.0.1:7001 --advertise 10.77.0.1:7001 ...
```

Faults on one node's link:

```
sudo ip netns exec p2p2 tc qdisc add dev veth0 root netem delay 200ms 50ms loss 10%
sudo ip netns exec p2p2 tc qdisc del dev veth0 root
sudo ip netns exec p2p2 nft add table inet p2p
sudo ip netns exec p2p2 nft add chain inet p2p in '{ type filter hook input priority 0; }'
sudo ip netns exec p2p2 nft add rule inet p2p in ip saddr 10.77.0.1 drop
sudo ip netns exec p2p2 nft delete table inet p2p
```

`scripts/netns.sh down` removes everything.

### 4.3 Between the two machines

Open the port range on Fedora (main PC):

```
sudo firewall-cmd --add-port=7000-7010/tcp
sudo firewall-cmd --permanent --add-port=7000-7010/tcp
```

On the old PC, use the same commands if it runs firewalld, or `sudo ufw allow 7000:7010/tcp` if it
runs ufw.

Latency and loss (replace `enp3s0` with the interface from `ip -br link`):

```
sudo tc qdisc add dev enp3s0 root netem delay 150ms 30ms loss 5%
sudo tc qdisc del dev enp3s0 root
```

Partition (runtime only, gone after reload):

```
sudo firewall-cmd --add-rich-rule='rule family=ipv4 source address=OLD_PC_IP port port=7000-7010 protocol=tcp drop'
sudo firewall-cmd --remove-rich-rule='rule family=ipv4 source address=OLD_PC_IP port port=7000-7010 protocol=tcp drop'
```

This rule only blocks new connections from the old PC to the main PC's node ports. Connections the
main PC opens to the old PC still work, because their replies go to an ephemeral port. For a full
partition, add the same rule on the old PC with `MAIN_PC_IP`. A one-way cut is also an interesting
case to show: it is an asymmetric failure, and SWIM's indirect ping does not help when all paths
cross the cut.

Do not run `tc` on the interface you use for SSH to the old PC without a way back; prefer
running the commands locally on each machine.

## 5. Demo script

Setup: main PC runs nodes n1–n3, old PC runs n4–n6. Dashboards open for n1 and n4.

1. **Start and discovery.** Start n1 as seed, then n2–n6 with `--seed`. Dashboard shows all six
   alive. Explain seed + gossip.
2. **Basic sync.** Copy a 20 MB file into n1's folder. It appears everywhere. Show its version
   vector `{n1:1}` on all nodes. Explain chunking.
3. **Delta transfer.** Append to the file on n5. Show only the changed chunks were transferred
   (dashboard counter). Version vector is now `{n1:1, n5:1}`.
4. **Delete.** Delete a file on n2; it disappears everywhere; dashboard shows the tombstone.
5. **Failure detection.** Kill n6. Show suspect → dead on the dashboard (SWIM). Restart it; it
   becomes alive and catches up.
6. **Partition.** Firewall rules on both machines split {n1–n3} from {n4–n6}. Dashboards show the
   other side dead. Edit `report.txt` on n1 and on n4 differently. Delete `old.txt` on n2 and edit it on n5.
7. **Heal.** Remove the rules. Within seconds every node has `report.txt` (winner) and
   `report.conflict-xxxxxxxx.txt` (loser), the same on all six nodes, and `old.txt` restored with n5's content.
   Explain concurrency detection, deterministic winner, edit beats delete.
8. **Slow network.** `tc netem` delay + loss on the main PC. Make edits; they still converge,
   slower. Remove netem.
9. **Merkle.** Show the dashboard counter: one Merkle request per round while in sync.
10. **Convergence test.** Run `go test -run Convergence -v` with a few seeds, show the passing output.
