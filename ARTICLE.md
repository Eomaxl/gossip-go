# I Built Cassandra's Gossip Protocol in Go to Understand Why It Takes 20 Seconds to Notice a Dead Node

*What 500 lines of Go taught me about the difference between a protocol and a heartbeat loop.*

---

I've run Cassandra in production for a while. I can recite the summary: nodes gossip once a second, there's no master, failure detection is "phi accrual," seeds help new nodes join.

Then someone asked me why a node that gets `SIGKILL`ed still shows as `UN` in `nodetool status` twenty seconds later, and I realised my understanding was a vocabulary, not a model. I knew the words. I couldn't derive the behaviour.

So I built it. Not a toy heartbeat loop — the actual protocol: three-phase digest exchange, generation/version state reconciliation, phi accrual detection, probabilistic peer selection. About 500 lines of Go. Then I measured it.

The answer to the twenty-second question turns out to be interesting, and it isn't "the timeout is set to twenty seconds." There is no timeout.

---

## The problem gossip actually solves

Start with the constraint that forces the design. You have N nodes. Every node needs to know, approximately and eventually:

- who else exists
- who is currently reachable
- some per-node facts: which rack, how much load, what schema version, which tokens

No coordinator. No node is special. Adding a node shouldn't require reconfiguring the others.

The obvious approaches fail on contact with N:

**Everyone pings everyone.** O(N²) connections. At 300 nodes that's 90,000 heartbeat flows, and the monitoring traffic outgrows the work traffic.

**Elect a coordinator.** Now you have a leader election problem, which is strictly harder than the membership problem you started with, plus a failure domain that takes the cluster with it.

**A registry — ZooKeeper, etcd.** Legitimate, and plenty of systems do it. But you've added an external dependency with its own quorum, its own operational surface, and its own outages. Cassandra's design goal was that a cluster is just Cassandra nodes.

Gossip's answer: **each node contacts one random peer per second and they reconcile everything they know about everyone.**

That's it. The properties fall out of it:

- Load per node is **constant**, not O(N). One peer per second whether the cluster is 5 nodes or 500.
- Information spreads in **O(log N) rounds**, the same curve as an epidemic. Each round roughly doubles the number of informed nodes.
- **No single point of failure**, because there is nothing to fail — all roles are symmetric.
- It is **eventually consistent about membership**, which is a real limitation and not a marketing word. At any instant, two nodes may legitimately disagree about whether a third is alive.

That last point is the price. Everything else on this list is paid for with it.

---

## Fact one: gossip is anti-entropy, not broadcast

The first thing I got wrong.

I pictured gossip as rumour-spreading: node A learns something, tells node B, B tells C, the news propagates outward. That's *rumour-mongering*, and it's a real gossip variant — but it has a stopping problem. When does a node stop repeating a rumour? Too early and the tail of the cluster never hears it. Too late and you're spending bandwidth on news everyone already has.

Cassandra uses **anti-entropy** instead, and it's a different shape. There is no "news." Two nodes meet, compare their entire view of the world, and reconcile the differences. Nothing is ever "done spreading" — the exchange just gets cheaper as the cluster converges, until in the steady state the two nodes compare notes, find they agree, and send nothing.

The mechanism that makes this affordable is the **digest**.

A digest is three fields:

```go
type Digest struct {
    Endpoint   string  // 10.0.1.5:7000
    Generation int64   // when that node booted
    MaxVersion int64   // highest version we hold for it
}
```

About 60 bytes on the wire. Critically: **60 bytes regardless of how much state that endpoint actually carries**. A node holding forty application-state entries and one holding two produce identically sized digests.

So a node advertises what it knows in constant space per endpoint, and only the parts that turn out to be stale become payload.

---

## Fact two: the three-phase handshake isn't ceremony

Cassandra's exchange is `SYN` → `ACK` → `ACK2`. I'd assumed two phases would do and the third was legacy.

It isn't. Watch what each phase buys:

```
A                                        B
|-- SYN: digests of everything A knows -->|
|                                         |  B compares against its own state
|<- ACK: state B has that A lacks         |
|        digests for what B wants         |
|                                         |
|-- ACK2: state A has that B lacks ------>|
```

With two phases, A must choose between sending its full state speculatively — wasteful, and most of it is usually redundant — or finishing the round still behind B. Three phases mean **each side transfers exactly the delta the other is missing, in one round trip**, with the discovery step costing a constant.

Here's the part I found genuinely elegant. In a converged cluster:

- the SYN carries digests
- the ACK carries **nothing** — B compared and found no differences
- the ACK2 is **never sent**

A steady-state gossip round between two agreeing nodes costs a few hundred bytes, no matter how much state the cluster holds. The protocol gets quiet when there's nothing to say. Most systems I've worked on have the opposite property: the health-check traffic is a constant tax you pay forever.

---

## Fact three: two integers replace a distributed clock

This is the piece that made everything else click, and it's the part summaries skip.

Every node's state carries two numbers:

```go
type HeartbeatState struct {
    Generation int64  // boot timestamp — constant while alive, increases on restart
    Version    int64  // node-local counter — increments on every mutation
}
```

**Generation** answers *"is this a newer incarnation of that process?"* It's the node's boot time. It never changes while the process lives.

**Version** answers *"is this newer information from the same incarnation?"* It's a node-local counter, bumped on every heartbeat and every state write.

There's no cluster-wide clock, no vector clock, no coordination. Versions are only meaningful within a single `(endpoint, generation)` pair — node A's version 42 and node B's version 42 have nothing to do with each other. That constraint is what makes "who has newer data" answerable with an integer comparison instead of a consensus round.

And then the reconciliation rules, which are four lines of logic that each exist because omitting one produces a specific bug:

| Condition | Action | Bug it prevents |
|---|---|---|
| remote generation **>** local | replace **wholesale** | merged state from the dead incarnation surviving into the new one |
| remote generation **=** local | merge per field, highest version wins | — (normal path) |
| remote generation **<** local | **ignore entirely** | a restarted node getting resurrected into its old identity by a slow peer |
| endpoint is **self** | **ignore entirely** | your own version counter going backwards |

Rule one is the subtle one, and it's why generation exists at all.

A node restarts. It comes back with `STATUS=BOOTSTRAPPING` at version 1. Somewhere in the cluster, a peer still holds `STATUS=NORMAL` at version 50 from the previous life. If you merge per-field on version alone, **the dead node's status wins** — 50 > 1 — and the cluster believes a bootstrapping node is ready to serve reads.

Generation makes the comparison unambiguous. Higher generation means everything below it is garbage, regardless of version. That's why the replacement is wholesale rather than a merge.

I wrote a test for it, because I wanted to watch it fail with the rule removed:

```go
func TestApplyDeltas_HigherGenerationReplacesWholesale(t *testing.T) {
    // peer at generation 100, STATUS=NORMAL at version 50
    // then restarts: generation 200, STATUS=BOOTSTRAPPING at version 1
    // BOOTSTRAPPING must win despite the lower version
}
```

Rule four — never accept another node's account of yourself — is the one I'd have skipped if I hadn't been writing tests. If you accept a peer's stale copy of your own heartbeat, your version counter can go *backwards*, and every comparison downstream silently stops meaning anything.

---

## Fact four: phi accrual is not a timeout with extra steps

Back to the twenty seconds.

The naive failure detector is a timeout: no heartbeat in N seconds, declare dead. It has a problem you can't tune your way out of, because the right N depends on conditions that change.

Pick N=5s. Works on a quiet LAN. Then a GC pause, a routing blip, a noisy neighbour on shared hardware — and you've convicted a healthy node. In Cassandra, a false positive means requests get routed away from a node that was fine, hints start accumulating, and when it "returns" the cluster does repair work it never needed to do.

Pick N=60s. No false positives. Now you're serving reads from a dead node's replica set for a minute.

There's no correct N, because you're comparing an absolute time against a distribution whose shape you don't know and which differs per link.

Phi accrual reframes it. Instead of answering *"is this node dead?"* with a boolean, it answers *"how surprised should I be that I haven't heard from this node?"* with a continuous value — and leaves the threshold to the caller.

The detector keeps a sliding window of inter-arrival intervals, learns their distribution, and expresses current silence in units of that distribution. Cassandra models arrivals as exponential, which collapses the survival function into a division:

```go
// φ = -log₁₀ P(silence this long | observed mean)
//   = (1 / ln 10) × (time since last heartbeat / mean interval)
func (f *FailureDetector) Phi(now time.Time) float64 {
    t := float64(now.Sub(f.last).Milliseconds())
    return phiFactor * t / f.mean()
}
```

Two consequences worth stating plainly:

**It self-calibrates.** A threshold tuned on a 1ms LAN behaves sanely on a 40ms cross-region link, because phi is measured in units of the observed distribution, not milliseconds. This is the property a fixed timeout can never have.

**The threshold becomes a business decision.** φ=8 means roughly "a 1-in-10⁸ chance this is a false positive." How much false-positive risk you'll trade for faster failover is a product question, not a networking one. Phi lets you express it as one number.

Here's what the climb actually looks like, from my 5-node cluster after a `SIGKILL`:

```
t= 0.0s  phi=  0.11
t= 3.5s  phi=  1.43  #
t= 7.0s  phi=  2.74  ##
t=10.5s  phi=  4.05  ####
t=14.0s  phi=  5.36  #####
t=17.5s  phi=  6.68  ######
t=20.9s  phi=  7.98  #######
                     ^ threshold 8.0 crossed at ~21s
```

Linear, at about 0.38/s against a ~1s mean interval. That's the exponential model: forgiving of a pause, decisive about an absence.

And the conviction times across observers:

| Observer | Convicted at |
|---|---|
| node1 | 21.8s |
| node2 | 19.8s |
| node3 | 23.8s |
| node4 | 24.8s |

**Each node convicted independently, at a different time.** Nobody voted. Nobody was told. Each one crossed its own threshold against its own observed arrival distribution — which is exactly why a single node having a bad network moment can't drag the cluster's opinion with it.

So: twenty seconds is not a configured timeout. It's `phi_convict_threshold = 8` divided by the rate phi accumulates at a 1-second gossip interval. Change either and the number moves. Lowering the threshold to 5.0 on the same setup convicts around 13s.

---

## Fact five: seeds exist for a failure mode I'd never have predicted

I always understood seeds as bootstrap-only: a new node needs *some* address to contact, seeds are that address, done.

That's not why they're in the steady-state gossip loop.

Each round, after picking a random live peer, a node does this:

```go
// If we didn't reach a seed this round, contact one anyway with
// probability seeds/(live+dead+1).
if !g.reachedSeed(targets) && len(g.seedSet) > 0 {
    p := float64(len(g.seedSet)) / float64(len(live)+len(dead)+1)
    if g.cfg.Rand.Float64() < p {
        g.sendSyn(randomSeed(), digests)
    }
}
```

The failure mode it prevents:

A network partition splits the cluster. Both halves keep gossiping internally. Both halves independently convict every node on the other side — correctly, given their evidence.

**The partition heals.** The network is fine now.

But each node picks gossip targets from its *live* set. Everyone on the far side is in the dead set. Neither half ever randomly selects a peer from the other, so neither discovers the other came back. **The logical split outlives the physical one**, indefinitely, until an operator notices.

Seeds are the fixed rendezvous points that guarantee the halves find each other. Both sides keep contacting the same small set of addresses regardless of their liveness opinions, so reconciliation is inevitable.

This is why seed lists must be **identical across the cluster** — a rendezvous point only works if both parties agree on it — and why two or three is enough. It's also why seeds are *not* masters: they carry no special authority, only a special probability of being contacted.

There's a companion mechanism for the same class of problem: each round, a node also probes a *dead* peer with probability `dead/(live+1)`. Without it, a node convicted once is never contacted again and can never rejoin on its own. The probability scales with the dead/live ratio, so a mostly-dead cluster spends most of its effort trying to recover rather than chatting among survivors.

---

## Why UDP, which I'd assumed was a legacy decision

It isn't. Gossip is *already* a redundancy protocol — it sends overlapping information to random peers every second, forever.

Layering TCP underneath buys nothing. A dropped packet is repaired by the next round, which was going to happen anyway. Meanwhile you'd pay for a connection per peer, head-of-line blocking, and a connection-teardown storm every time a node dies.

The reliability guarantee you want is already provided by the protocol above. Asking for it twice just costs money.

---

## What I measured

5 nodes, 1s interval, fanout 1, φ threshold 8.0, single host. Loopback — these isolate protocol latency from network latency, so treat them as a floor rather than a forecast.

**Application state propagation to all 4 peers** (n=5 trials): min 1.05s, mean 1.99s, max 2.98s.

The spread is the protocol working, not measurement noise. A node gossips to one random peer per second; whether your write catches the next round or the one after is a coin flip. Full propagation lands within a small number of round intervals rather than at a fixed time. If you need a predictable upper bound on membership propagation, gossip is the wrong layer to get it from.

**Restart rejoin**: 0.3s from process start to `live=true` with an incremented generation — the restarting node contacts its seeds immediately rather than waiting to be discovered.

---

## The part that changed how I read `nodetool status`

Before this, I read `UN` / `DN` as facts about the cluster.

They aren't. They're **one node's opinion**, derived from evidence that node observed directly, evaluated against a distribution it learned locally. Run `nodetool status` against two different nodes during an incident and you can legitimately get two different answers. Neither is wrong. The cluster has no consensus on liveness, by design, because reaching consensus on liveness would require the coordination that gossip exists to avoid.

Which reframes a category of production question. "Why does node A think node C is down when node B doesn't?" isn't a bug report. It's the expected behaviour of a system that deliberately traded agreement for availability — and if you need agreement about membership, you need a different mechanism layered on top, not a different `phi_convict_threshold`.

---

## Build it yourself

The code is on GitHub: **[github.com/Eomaxl/gossip-go](https://github.com/Eomaxl/gossip-go)**

```bash
make up                      # 5-node cluster on localhost
./scripts/cluster.sh status  # nodetool-status equivalent
./scripts/cluster.sh kill 5  # SIGKILL a node, watch phi climb
./scripts/cluster.sh probe   # measure propagation latency yourself
```

It implements the digest exchange, generation/version reconciliation, phi accrual, probabilistic peer selection, seed contact, and restart detection. It does **not** implement token ownership, replica placement, schema agreement, or anything that reads or writes data — membership only.

The most useful thing I did was write tests for the four reconciliation rules and then delete each rule to watch what broke. Reading `Gossiper.java` teaches you what the code does. Deleting a line and watching a restarted node get resurrected into its old identity teaches you why it's there.

If you take one thing from this: **the interesting parts of a distributed protocol are the cases it handles, not the happy path.** The happy path in gossip is thirty lines. The other 470 are partition recovery, restart disambiguation, and refusing to believe what other nodes say about you.

---

*Notes, corrections, and arguments welcome — particularly if you've operated gossip-based systems at a scale where these numbers look quaint.*

---

**References**

- Hayashibara et al., *The φ Accrual Failure Detector* (2004) — assumes a normal distribution; Cassandra uses the cheaper exponential form.
- DeCandia et al., *Dynamo: Amazon's Highly Available Key-value Store* (2007)
- Demers et al., *Epidemic Algorithms for Replicated Database Maintenance* (1987)
- Apache Cassandra: `Gossiper.java`, `EndpointState.java`, `FailureDetector.java`
