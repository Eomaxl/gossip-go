package gossip

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"
)

// MaxDigestsPerMessage caps how many digests ride in one SYN.
//
// Each digest is roughly 60 bytes of JSON. At 200 digests a SYN is ~12KB, which
// already fragments across an Ethernet MTU. Real Cassandra has the same problem
// and handles it the same way: shuffle the digest list before truncating, so
// that over several rounds every endpoint gets advertised even if no single
// message can carry them all.
//
// This is the main thing that stops gossip bandwidth from growing with cluster
// size. Message size stays bounded; convergence time grows logarithmically.
const MaxDigestsPerMessage = 200

// Standard application-state keys, mirroring Cassandra's naming.
const (
	AppStatus  = "STATUS"
	AppLoad    = "LOAD"
	AppDC      = "DC"
	AppRack    = "RACK"
	AppRelease = "RELEASE_VERSION"
	AppSchema  = "SCHEMA"
)

type Config struct {
	// Bind is the local UDP listen address.
	Bind string
	// Advertise is the address peers should reply to. Defaults to Bind.
	// These differ behind NAT, in containers with port mapping, and on hosts
	// that bind 0.0.0.0 — all cases where getting it wrong produces a cluster
	// whose members can send but never receive.
	Advertise string
	Seeds     []string
	Cluster   string

	GossipInterval time.Duration
	PhiThreshold   float64
	// FanOut is how many live peers receive a SYN per round. Cassandra uses 1.
	// Raising it trades bandwidth for convergence latency; the relationship is
	// logarithmic, so the second peer buys much more than the tenth.
	FanOut int

	Logger *slog.Logger
	Rand   *rand.Rand
}

func (c *Config) withDefaults() {
	if c.GossipInterval == 0 {
		c.GossipInterval = time.Second
	}
	if c.PhiThreshold == 0 {
		c.PhiThreshold = DefaultPhiThreshold
	}
	if c.FanOut == 0 {
		c.FanOut = 1
	}
	if c.Cluster == "" {
		c.Cluster = "test-cluster"
	}
	if c.Advertise == "" {
		c.Advertise = c.Bind
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
}

// EventKind describes a membership transition.
type EventKind string

const (
	EventJoin    EventKind = "JOIN"
	EventUp      EventKind = "UP"
	EventDown    EventKind = "DOWN"
	EventRestart EventKind = "RESTART"
)

type Event struct {
	Kind     EventKind
	Endpoint string
	Phi      float64
	At       time.Time
}

type Gossiper struct {
	cfg  Config
	self string
	log  *slog.Logger

	mu        sync.RWMutex
	endpoints map[string]*endpointRecord
	versions  versionGenerator

	transport *Transport
	seedSet   map[string]struct{}

	subMu sync.RWMutex
	subs  []func(Event)

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// metrics, read via Stats()
	statMu     sync.Mutex
	roundsRun  int64
	synSent    int64
	synRecv    int64
	bytesRough int64
}

func New(cfg Config) (*Gossiper, error) {
	cfg.withDefaults()
	if cfg.Bind == "" {
		return nil, fmt.Errorf("gossip: Bind is required")
	}

	tr, err := NewTransport(cfg.Bind, cfg.Logger)
	if err != nil {
		return nil, err
	}

	g := &Gossiper{
		cfg:       cfg,
		self:      cfg.Advertise,
		log:       cfg.Logger.With("self", cfg.Advertise),
		endpoints: make(map[string]*endpointRecord),
		transport: tr,
		seedSet:   make(map[string]struct{}, len(cfg.Seeds)),
		stop:      make(chan struct{}),
	}
	for _, s := range cfg.Seeds {
		if s == "" || s == g.self {
			continue
		}
		g.seedSet[s] = struct{}{}
	}

	now := time.Now()
	// Generation is the boot time in seconds. It is the only monotonic signal
	// available without coordination, and it is what makes restarts
	// distinguishable from mere silence.
	g.endpoints[g.self] = &endpointRecord{
		state: EndpointState{
			Heartbeat: HeartbeatState{Generation: now.Unix(), Version: g.versions.next()},
			AppState:  map[string]VersionedValue{},
		},
		live:        true,
		fd:          NewFailureDetector(cfg.GossipInterval),
		lastUpdated: now,
		firstSeen:   now,
	}

	// Seeds start as known-but-unproven. They are not marked live: liveness is
	// earned by evidence, and we have none yet.
	for s := range g.seedSet {
		g.endpoints[s] = &endpointRecord{
			state:     EndpointState{Heartbeat: HeartbeatState{}},
			live:      false,
			fd:        NewFailureDetector(cfg.GossipInterval),
			firstSeen: now,
		}
	}

	tr.Handle(g.onMessage)
	return g, nil
}

func (g *Gossiper) Self() string { return g.self }

func (g *Gossiper) Subscribe(fn func(Event)) {
	g.subMu.Lock()
	g.subs = append(g.subs, fn)
	g.subMu.Unlock()
}

func (g *Gossiper) emit(e Event) {
	g.subMu.RLock()
	subs := append([]func(Event){}, g.subs...)
	g.subMu.RUnlock()
	for _, fn := range subs {
		fn(e)
	}
}

func (g *Gossiper) Start() {
	g.wg.Add(2)
	go func() { defer g.wg.Done(); g.transport.Serve() }()
	go func() { defer g.wg.Done(); g.loop() }()
	g.log.Info("gossip started",
		"bind", g.cfg.Bind, "cluster", g.cfg.Cluster,
		"seeds", g.cfg.Seeds, "interval", g.cfg.GossipInterval)
}

func (g *Gossiper) Stop() {
	g.stopOnce.Do(func() {
		close(g.stop)
		_ = g.transport.Close()
	})
	g.wg.Wait()
}

func (g *Gossiper) loop() {
	t := time.NewTicker(g.cfg.GossipInterval)
	defer t.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-t.C:
			g.round()
		}
	}
}

// ---------------------------------------------------------------------------
// One gossip round
// ---------------------------------------------------------------------------

// round is the entire protocol, once per second.
//
// The ordering matters: bump our own heartbeat first so the digest we send
// reflects this instant, then choose peers, then re-evaluate liveness using the
// arrivals that landed since last time.
func (g *Gossiper) round() {
	g.beat()

	digests := g.buildDigests()
	live, dead := g.partition()

	// 1. One (or FanOut) random live peers. This is the workhorse. Random
	//    selection is what gives gossip its epidemic spread: information
	//    reaches the whole cluster in O(log N) rounds with no topology
	//    knowledge and no coordinator.
	targets := sample(g.cfg.Rand, live, g.cfg.FanOut)
	for _, p := range targets {
		g.sendSyn(p, digests)
	}

	// 2. A dead peer, probabilistically. Without this, a node convicted once is
	//    never contacted again and can never rejoin — the cluster would need an
	//    operator to heal from a transient partition. The probability is scaled
	//    by the dead/live ratio so that a mostly-dead cluster spends most of its
	//    effort trying to recover rather than chatting among survivors.
	if len(dead) > 0 {
		p := float64(len(dead)) / float64(len(live)+1)
		if g.cfg.Rand.Float64() < p {
			g.sendSyn(dead[g.cfg.Rand.Intn(len(dead))], digests)
		}
	}

	// 3. A seed, if we did not already reach one.
	//
	//    This is the subtle one, and it is the fix for a real failure mode:
	//    a network partition heals, but each half has independently convicted
	//    the other, so neither ever picks a peer from the far side and the
	//    logical split outlives the physical one. Seeds are the fixed
	//    rendezvous points that guarantee the halves find each other again.
	//    It is also why seed lists must be identical across the cluster, and
	//    why two or three is enough.
	if !g.reachedSeed(targets) && len(g.seedSet) > 0 {
		p := float64(len(g.seedSet)) / float64(len(live)+len(dead)+1)
		if g.cfg.Rand.Float64() < p {
			seeds := g.seedList()
			if len(seeds) > 0 {
				g.sendSyn(seeds[g.cfg.Rand.Intn(len(seeds))], digests)
			}
		}
	}

	g.evaluateLiveness()

	g.statMu.Lock()
	g.roundsRun++
	g.statMu.Unlock()
}

// beat advances our own heartbeat. This is the only liveness claim we ever
// make about ourselves, and it is the only one anyone else will believe.
func (g *Gossiper) beat() {
	g.mu.Lock()
	defer g.mu.Unlock()
	me := g.endpoints[g.self]
	me.state.Heartbeat.Version = g.versions.next()
	me.lastUpdated = time.Now()
}

// SetAppState publishes a fact about this node. It will reach the whole cluster
// within O(log N) rounds without this function knowing anything about topology.
func (g *Gossiper) SetAppState(key, value string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	me := g.endpoints[g.self]
	if me.state.AppState == nil {
		me.state.AppState = map[string]VersionedValue{}
	}
	me.state.AppState[key] = VersionedValue{Value: value, Version: g.versions.next()}
}

func (g *Gossiper) buildDigests() []Digest {
	g.mu.RLock()
	out := make([]Digest, 0, len(g.endpoints))
	for ep, rec := range g.endpoints {
		out = append(out, Digest{
			Endpoint:   ep,
			Generation: rec.state.Heartbeat.Generation,
			MaxVersion: rec.state.MaxVersion(),
		})
	}
	g.mu.RUnlock()

	// Shuffle before truncating so every endpoint eventually gets advertised
	// even in clusters too large for one datagram.
	g.cfg.Rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	if len(out) > MaxDigestsPerMessage {
		out = out[:MaxDigestsPerMessage]
	}
	return out
}

func (g *Gossiper) partition() (live, dead []string) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for ep, rec := range g.endpoints {
		if ep == g.self {
			continue
		}
		if rec.live {
			live = append(live, ep)
		} else {
			dead = append(dead, ep)
		}
	}
	return
}

func (g *Gossiper) seedList() []string {
	out := make([]string, 0, len(g.seedSet))
	for s := range g.seedSet {
		out = append(out, s)
	}
	return out
}

func (g *Gossiper) reachedSeed(targets []string) bool {
	for _, t := range targets {
		if _, ok := g.seedSet[t]; ok {
			return true
		}
	}
	return false
}

func sample(r *rand.Rand, pool []string, n int) []string {
	if len(pool) == 0 || n <= 0 {
		return nil
	}
	if n >= len(pool) {
		out := make([]string, len(pool))
		copy(out, pool)
		return out
	}
	idx := r.Perm(len(pool))[:n]
	out := make([]string, 0, n)
	for _, i := range idx {
		out = append(out, pool[i])
	}
	return out
}

func (g *Gossiper) sendSyn(to string, digests []Digest) {
	msg := Message{Type: MsgSyn, From: g.self, Cluster: g.cfg.Cluster, Digests: digests}
	if err := g.transport.Send(to, msg); err != nil {
		g.log.Debug("syn send failed", "to", to, "err", err)
		return
	}
	g.statMu.Lock()
	g.synSent++
	g.statMu.Unlock()
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

func (g *Gossiper) onMessage(m Message, src *net.UDPAddr) {
	if m.Cluster != g.cfg.Cluster {
		g.log.Warn("dropping message from foreign cluster",
			"theirs", m.Cluster, "ours", g.cfg.Cluster, "src", src.String())
		return
	}
	if m.From == "" || m.From == g.self {
		return
	}

	switch m.Type {
	case MsgSyn:
		g.statMu.Lock()
		g.synRecv++
		g.statMu.Unlock()
		g.handleSyn(m)
	case MsgAck:
		g.handleAck(m)
	case MsgAck2:
		g.applyDeltas(m.Deltas)
	default:
		g.log.Debug("unknown message type", "type", m.Type)
	}
}

// handleSyn: the peer told us everything it knows. Work out what we're missing
// and what it's missing, and answer both in one message.
func (g *Gossiper) handleSyn(m Message) {
	g.noteContact(m.From)

	wanted, newer := g.examine(m.Digests)
	reply := Message{
		Type:    MsgAck,
		From:    g.self,
		Cluster: g.cfg.Cluster,
		Digests: wanted,
		Deltas:  newer,
	}
	if err := g.transport.Send(m.From, reply); err != nil {
		g.log.Debug("ack send failed", "to", m.From, "err", err)
	}
}

// handleAck: absorb what the peer sent us, then answer the parts it asked for.
func (g *Gossiper) handleAck(m Message) {
	g.applyDeltas(m.Deltas)

	if len(m.Digests) == 0 {
		return
	}
	deltas := g.collect(m.Digests)
	if len(deltas) == 0 {
		return
	}
	reply := Message{Type: MsgAck2, From: g.self, Cluster: g.cfg.Cluster, Deltas: deltas}
	if err := g.transport.Send(m.From, reply); err != nil {
		g.log.Debug("ack2 send failed", "to", m.From, "err", err)
	}
}

// examine compares the peer's digests against our own state and splits the
// endpoints three ways: ones where we are behind (request), ones where we are
// ahead (push), ones where we are level (ignore entirely).
//
// Note the asymmetry: we only reason about endpoints the peer mentioned.
// Endpoints we know and the peer doesn't are NOT pushed here. They propagate
// when we initiate our own round a moment later. Cassandra behaves the same
// way, and it is why convergence is measured in rounds rather than messages.
func (g *Gossiper) examine(remote []Digest) (wanted []Digest, newer map[string]EndpointState) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	newer = make(map[string]EndpointState)
	for _, d := range remote {
		rec, known := g.endpoints[d.Endpoint]
		if !known {
			// Never heard of it. Ask for everything from generation zero.
			wanted = append(wanted, Digest{Endpoint: d.Endpoint})
			continue
		}
		lg := rec.state.Heartbeat.Generation
		lv := rec.state.MaxVersion()

		switch {
		case d.Generation > lg:
			// The peer knows about a newer incarnation. Everything we hold is
			// stale by definition — ask for the lot.
			wanted = append(wanted, Digest{Endpoint: d.Endpoint, Generation: d.Generation})
		case d.Generation < lg:
			// The peer is behind a restart. Push the full current state; a
			// delta would be meaningless across a generation boundary.
			newer[d.Endpoint] = rec.state.clone()
		default:
			if d.MaxVersion > lv {
				wanted = append(wanted, Digest{Endpoint: d.Endpoint, Generation: lg, MaxVersion: lv})
			} else if d.MaxVersion < lv {
				newer[d.Endpoint] = rec.state.since(d.MaxVersion)
			}
			// Equal versions: send nothing. This is the common case in a
			// converged cluster, and it is why a steady-state gossip round
			// costs a few hundred bytes regardless of how much state exists.
		}
	}
	return wanted, newer
}

// collect answers a request list with the deltas above each requested version.
func (g *Gossiper) collect(requests []Digest) map[string]EndpointState {
	g.mu.RLock()
	defer g.mu.RUnlock()

	out := make(map[string]EndpointState, len(requests))
	for _, d := range requests {
		rec, ok := g.endpoints[d.Endpoint]
		if !ok {
			continue
		}
		if d.Generation < rec.state.Heartbeat.Generation || d.Generation == 0 {
			out[d.Endpoint] = rec.state.clone()
			continue
		}
		if d.Generation == rec.state.Heartbeat.Generation {
			if s := rec.state.since(d.MaxVersion); len(s.AppState) > 0 || s.Heartbeat.Version > d.MaxVersion {
				out[d.Endpoint] = s
			}
		}
	}
	return out
}

// applyDeltas is the reconciliation core. Every rule here exists to prevent a
// specific, observable bug.
func (g *Gossiper) applyDeltas(deltas map[string]EndpointState) {
	if len(deltas) == 0 {
		return
	}
	now := time.Now()

	type transition struct {
		kind EventKind
		ep   string
	}
	var events []transition

	g.mu.Lock()
	for ep, remote := range deltas {
		// Rule 1: never accept someone else's account of us. We are the sole
		// authority on our own heartbeat and generation. Accepting a third
		// party's stale copy would let us un-increment our own clock, and
		// every version comparison downstream would stop meaning anything.
		if ep == g.self {
			continue
		}

		rec, known := g.endpoints[ep]
		if !known {
			g.endpoints[ep] = &endpointRecord{
				state:       remote.clone(),
				live:        true,
				fd:          NewFailureDetector(g.cfg.GossipInterval),
				lastUpdated: now,
				firstSeen:   now,
			}
			g.endpoints[ep].fd.Report(now)
			events = append(events, transition{EventJoin, ep})
			continue
		}

		switch {
		case remote.Heartbeat.Generation > rec.state.Heartbeat.Generation:
			// Rule 2: a higher generation means the process restarted. Replace
			// wholesale rather than merging. Merging would leave application
			// state from the dead incarnation alongside the new one — the exact
			// bug that makes a restarted node appear to hold two conflicting
			// loads or two token assignments at once.
			rec.state = remote.clone()
			rec.fd.Reset()
			rec.fd.Report(now)
			rec.lastUpdated = now
			events = append(events, transition{EventRestart, ep})
			if !rec.live {
				rec.live = true
				events = append(events, transition{EventUp, ep})
			}

		case remote.Heartbeat.Generation == rec.state.Heartbeat.Generation:
			advanced := false
			if remote.Heartbeat.Version > rec.state.Heartbeat.Version {
				rec.state.Heartbeat = remote.Heartbeat
				advanced = true
			}
			for k, v := range remote.AppState {
				cur, ok := rec.state.AppState[k]
				if !ok || v.Version > cur.Version {
					if rec.state.AppState == nil {
						rec.state.AppState = map[string]VersionedValue{}
					}
					rec.state.AppState[k] = v
					advanced = true
				}
			}
			// Rule 3: only a heartbeat we had not already seen counts as
			// evidence of liveness. A packet echoing state we already hold may
			// be a third party replaying something it heard minutes ago. Treat
			// that as a heartbeat and you build a detector that never convicts.
			if advanced {
				rec.fd.Report(now)
				rec.lastUpdated = now
				if !rec.live {
					rec.live = true
					events = append(events, transition{EventUp, ep})
				}
			}

		default:
			// Rule 4: a lower generation is stale gossip about a dead
			// incarnation, still circulating. Drop it silently. Without this
			// check, a node that restarts can be "resurrected" into its old
			// identity by a slow peer, and will flap.
			continue
		}
	}
	g.mu.Unlock()

	for _, t := range events {
		g.emit(Event{Kind: t.kind, Endpoint: t.ep, At: now})
	}
}

// noteContact registers a peer we had never heard of, discovered because it
// contacted us. This is how a joining node becomes visible to a seed.
func (g *Gossiper) noteContact(ep string) {
	g.mu.Lock()
	if _, ok := g.endpoints[ep]; !ok {
		g.endpoints[ep] = &endpointRecord{
			state:     EndpointState{},
			live:      false, // still unproven: we have no heartbeat from it yet
			fd:        NewFailureDetector(g.cfg.GossipInterval),
			firstSeen: time.Now(),
		}
	}
	g.mu.Unlock()
}

// evaluateLiveness asks the failure detector about every peer and flips the
// live flag when phi crosses the threshold.
//
// Conviction is one-directional in effect: phi rises with silence, and falls to
// near zero the moment a fresh heartbeat lands (applyDeltas calls Report, which
// resets the clock). So revival needs no special case — it falls out of the
// same comparison.
func (g *Gossiper) evaluateLiveness() {
	now := time.Now()
	var events []Event

	g.mu.Lock()
	for ep, rec := range g.endpoints {
		if ep == g.self {
			continue
		}
		// A peer we have never heard from has no arrival distribution. Leave it
		// alone rather than convicting it on no evidence — the dead-node gossip
		// path in round() will keep probing.
		if rec.lastUpdated.IsZero() {
			continue
		}
		phi := rec.fd.Phi(now)
		switch {
		case rec.live && phi > g.cfg.PhiThreshold:
			rec.live = false
			events = append(events, Event{Kind: EventDown, Endpoint: ep, Phi: phi, At: now})
		case !rec.live && phi <= g.cfg.PhiThreshold:
			rec.live = true
			events = append(events, Event{Kind: EventUp, Endpoint: ep, Phi: phi, At: now})
		}
	}
	g.mu.Unlock()

	for _, e := range events {
		g.emit(e)
	}
}

// ---------------------------------------------------------------------------
// Introspection
// ---------------------------------------------------------------------------

func (g *Gossiper) Members() []MemberView {
	now := time.Now()
	g.mu.RLock()
	defer g.mu.RUnlock()

	out := make([]MemberView, 0, len(g.endpoints))
	for ep, rec := range g.endpoints {
		app := make(map[string]string, len(rec.state.AppState))
		for k, v := range rec.state.AppState {
			app[k] = v.Value
		}
		phi := 0.0
		if ep != g.self {
			phi = rec.fd.Phi(now)
		}
		out = append(out, MemberView{
			Endpoint:   ep,
			Live:       ep == g.self || rec.live,
			Phi:        round2(phi),
			Generation: rec.state.Heartbeat.Generation,
			Version:    rec.state.MaxVersion(),
			AppState:   app,
			Self:       ep == g.self,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Endpoint < out[j].Endpoint })
	return out
}

type Stats struct {
	Self      string `json:"self"`
	Cluster   string `json:"cluster"`
	Rounds    int64  `json:"rounds"`
	SynSent   int64  `json:"syn_sent"`
	SynRecv   int64  `json:"syn_received"`
	Known     int    `json:"endpoints_known"`
	LiveCount int    `json:"live"`
	DeadCount int    `json:"dead"`
}

func (g *Gossiper) Stats() Stats {
	g.mu.RLock()
	known := len(g.endpoints)
	live, dead := 0, 0
	for ep, rec := range g.endpoints {
		if ep == g.self || rec.live {
			live++
		} else {
			dead++
		}
	}
	g.mu.RUnlock()

	g.statMu.Lock()
	defer g.statMu.Unlock()
	return Stats{
		Self: g.self, Cluster: g.cfg.Cluster,
		Rounds: g.roundsRun, SynSent: g.synSent, SynRecv: g.synRecv,
		Known: known, LiveCount: live, DeadCount: dead,
	}
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
