# gossip-go

A working implementation of Cassandra-style gossip in Go: digest-based anti-entropy over UDP, with a phi accrual failure detector.

This is a teaching artifact. It implements the protocol faithfully enough that the interesting failure modes are reproducible on a laptop — restart detection, partition healing, conviction latency — without the 200k lines of Cassandra surrounding it.

---

## What it implements

| Mechanism | Status |
|---|---|
| Three-phase digest exchange (SYN / ACK / ACK2) | ✅ |
| Generation + version state versioning | ✅ |
| Delta transfer (only fields above the peer's watermark) | ✅ |
| Phi accrual failure detection | ✅ |
| Random live-peer selection, configurable fanout | ✅ |
| Probabilistic dead-peer probing (partition recovery) | ✅ |
| Probabilistic seed contact (logical-split recovery) | ✅ |
| Restart detection via generation bump | ✅ |
| Cluster-name isolation | ✅ |
| Application state propagation (`STATUS`, `LOAD`, `DC`, …) | ✅ |
| Digest capping so messages stay inside one datagram | ✅ |

**Not implemented**, and out of scope: token ownership, replica placement, schema agreement, streaming, or anything that reads or writes actual data. This is the membership layer only.

---

## Quick start

```bash
make build
make up            # 5 nodes on 127.0.0.1:7001-7005
./scripts/cluster.sh status
```

```
  ADDRESS                STATE      PHI    GENERATION  VERSION  DC
* 127.0.0.1:7001         UN           0    1789922455       11  DC1
  127.0.0.1:7002         UN           0    1789922455       11  DC1
  127.0.0.1:7003         UN        0.44    1789922455       10  DC1
  127.0.0.1:7004         UN        0.02    1789922455       11  DC2
  127.0.0.1:7005         UN        0.02    1789922455       11  DC2
```

`UN` / `DN` and the column layout deliberately mirror `nodetool status`.

### Watch state propagate

```bash
curl -X PUT 127.0.0.1:8001/state/LOAD -d "42.5GB"
./scripts/cluster.sh probe
```

### Watch a node get convicted

```bash
./scripts/cluster.sh kill 5
watch -n1 ./scripts/cluster.sh status     # phi climbs ~0.38/s until it crosses 8.0
```

### Watch it come back

```bash
./bin/gossipd -bind 127.0.0.1:7005 -http 127.0.0.1:8005 -seeds 127.0.0.1:7001,127.0.0.1:7002
```

The cluster logs `RESTART` rather than `UP`, because the generation increased. That distinction is the whole reason generations exist.

```bash
make down
```

---

## Measured behaviour

Numbers below are from this implementation on a single host, 5 nodes, 1s gossip interval, fanout 1, phi threshold 8.0. Loopback, so they isolate protocol latency from network latency — real-world figures will be higher and more variable. Reproduce with `./scripts/cluster.sh probe`.

**Application-state propagation to all 4 remaining nodes** (n=5 trials):

| | |
|---|---|
| min | 1.05s |
| mean | 1.99s |
| max | 2.98s |

The spread is the protocol working as designed, not noise. A node gossips to one random peer per second; whether your update catches the next round or the one after is a coin flip, so full propagation lands within a small number of round intervals rather than at a fixed time.

**Failure detection after `SIGKILL`** (no graceful leave — worst case):

| Observer | Conviction |
|---|---|
| node1 | 21.8s |
| node2 | 19.8s |
| node3 | 23.8s |
| node4 | 24.8s |

Phi grew linearly at roughly 0.38/s against a ~1s mean inter-arrival time, crossing 8.0 around 21s. Each node convicts independently on its own evidence, which is why the times differ — nobody votes, and nobody is told.

**Restart rejoin:** 0.3s from process start to `live=true` with an incremented generation, because the restarted node contacts its seeds immediately rather than waiting to be discovered.

To trade detection latency for false-positive risk, lower `-phi`. At 5.0 conviction lands near 13s on the same setup.

---

## Architecture

```
cmd/gossipd            CLI + HTTP introspection API
internal/gossip/
  state.go             HeartbeatState, VersionedValue, EndpointState, delta logic
  message.go           Digest + SYN/ACK/ACK2 envelope
  failuredetector.go   phi accrual
  transport.go         UDP + JSON codec
  gossiper.go          round scheduling, peer selection, reconciliation
```

### The three-phase exchange

```
A                                    B
|-- SYN   digests(everything A knows) ->|
|                                       |  B compares against its own state
|<- ACK   deltas(B is ahead)            |
|         digests(B is behind)          |
|                                       |
|-- ACK2  deltas(A is ahead) ---------->|
```

A digest is `endpoint:generation:maxVersion` — about 60 bytes regardless of how much state that endpoint actually carries. Only the parts that turn out to be stale get transferred as payload. In a converged cluster the ACK carries nothing and the ACK2 is never sent, so a steady-state round costs a few hundred bytes no matter how large the cluster's state has grown.

### Two numbers do all the work

**Generation** is the node's boot timestamp. Constant while the process lives, strictly increasing across restarts. It answers "is this a newer incarnation?"

**Version** is a node-local counter, incremented on every mutation. It answers "is this newer information from the same incarnation?"

The comparison rules, in `applyDeltas`:

| Condition | Action | Why |
|---|---|---|
| remote generation > local | replace **wholesale** | merging would leave state from the dead incarnation alongside the new one |
| remote generation = local | merge per field, highest version wins | normal delta application |
| remote generation < local | **ignore entirely** | stale gossip about a dead incarnation, still circulating |
| endpoint is self | **ignore entirely** | we are the sole authority on our own heartbeat |

Each of those four rules exists because omitting it produces a specific, reproducible bug. They are covered by tests in `gossiper_test.go`.

### Peer selection per round

1. One random **live** peer (configurable via `-fanout`). Random selection is what gives gossip O(log N) spread with no topology knowledge.
2. One random **dead** peer, with probability `dead/(live+1)`. Without this, a node convicted once is never contacted again and can never rejoin without operator intervention.
3. One **seed**, if step 1 didn't reach one, with probability `seeds/(live+dead+1)`. This is the fix for a genuinely nasty failure mode: a partition heals, but each half has independently convicted the other, so neither ever picks a peer from the far side. Seeds are the fixed rendezvous points that guarantee the halves find each other.

Step 3 is also why seed lists must be identical across the cluster, and why two or three seeds is enough. Seeds are not masters — they have no special authority, only a special probability of being contacted.

### Why UDP

Gossip is already a redundancy protocol: it sends overlapping information to random peers every second, forever. Layering TCP's retransmission and ordering underneath buys nothing — a dropped packet is repaired by the next round — while costing a connection per peer, head-of-line blocking, and a teardown storm when a node dies.

JSON on the wire is the wrong call for production and the right one here: you can `tcpdump` it and read it. Swapping in protobuf is a codec change, not a protocol change.

---

## HTTP API

| Endpoint | Purpose |
|---|---|
| `GET /members` | full cluster view with phi, generation, version, app state |
| `GET /stats` | round and message counters |
| `PUT /state/{key}` | publish application state (body is the value) |
| `GET /healthz` | liveness |

---

## Flags

```
-bind         UDP listen address              (127.0.0.1:7000)
-advertise    address peers reply to          (defaults to -bind)
-http         introspection API address       (127.0.0.1:8000)
-seeds        comma-separated seed addresses
-cluster      cluster name                    (test-cluster)
-dc -rack     published as application state  (DC1 / RAC1)
-interval     gossip round interval           (1s)
-phi          conviction threshold            (8.0)
-fanout       live peers contacted per round  (1)
-v            debug logging
```

`-advertise` matters behind NAT or in containers with port mapping. Bind `0.0.0.0`, advertise the routable address — get this wrong and you build a cluster whose members can send but never receive.

---

## Testing

```bash
make test    # unit tests
make race    # with the race detector
```

Tests cover the four reconciliation rules, digest classification, delta windowing, phi growth and reset, digest capping, and cluster-name isolation.

---

## Known limitations

- **JSON over UDP.** A ~200-node digest list is ~12KB, which IP-fragments past a 1500-byte MTU; a single lost fragment discards the whole datagram. `MaxDigestsPerMessage` caps this, with shuffling so every endpoint is eventually advertised — but a compact codec would raise the ceiling considerably.
- **No authentication or encryption.** Anyone who can reach the UDP port can inject state. Cassandra has the same property, which is why gossip traffic belongs on a trusted network.
- **`examine` only answers what it was asked about.** Endpoints the responder knows and the initiator doesn't are not pushed; they propagate when the responder initiates its own round. Faithful to Cassandra, and it costs a round.
- **Generation is `time.Unix()`.** Two restarts inside the same second produce the same generation. Cassandra has this bug too.
- **Convergence is measured on loopback.** Real networks will be slower and the phi distribution wider.

---

## References

- Hayashibara et al., *The φ Accrual Failure Detector* (2004) — the algorithm, with a normal-distribution model. Cassandra (and this code) use the cheaper exponential form.
- DeCandia et al., *Dynamo: Amazon's Highly Available Key-value Store* (2007) — where Cassandra's membership design comes from.
- Demers et al., *Epidemic Algorithms for Replicated Database Maintenance* (1987) — the original anti-entropy and rumour-mongering analysis.
- Apache Cassandra `Gossiper.java`, `EndpointState.java`, `FailureDetector.java`.

## License

MIT
