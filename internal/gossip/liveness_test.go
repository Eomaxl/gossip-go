package gossip

import (
	"testing"
	"time"
)

// forceSilence makes ep's failure detector look like it has heard nothing for
// the given duration, without sleeping in real time. Same package, so we can
// reach into the unexported fd/lastUpdated fields directly.
func forceSilence(g *Gossiper, ep string, ago time.Duration) {
	g.mu.Lock()
	rec := g.endpoints[ep]
	rec.fd.mu.Lock()
	rec.fd.last = time.Now().Add(-ago)
	rec.fd.mu.Unlock()
	rec.lastUpdated = time.Now().Add(-ago)
	g.mu.Unlock()
}

func hasEvent(events []Event, kind EventKind, ep string) bool {
	for _, e := range events {
		if e.Kind == kind && e.Endpoint == ep {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// evaluateLiveness
// ---------------------------------------------------------------------------

func TestEvaluateLiveness_ConvictsAfterSilence(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0") // 50ms gossip interval
	peer := "10.0.0.9:7000"

	g.applyDeltas(map[string]EndpointState{peer: {Heartbeat: HeartbeatState{Generation: 1, Version: 1}}})
	forceSilence(g, peer, time.Hour)

	var events []Event
	g.Subscribe(func(e Event) { events = append(events, e) })
	g.evaluateLiveness()

	g.mu.RLock()
	live := g.endpoints[peer].live
	g.mu.RUnlock()
	if live {
		t.Fatal("peer should have been convicted after an hour of silence on a 50ms interval")
	}
	if !hasEvent(events, EventDown, peer) {
		t.Errorf("no DOWN event emitted, got %+v", events)
	}
}

// The doc comment on evaluateLiveness claims revival "falls out of the same
// comparison" with no special case, because applyDeltas' Report() resets the
// failure detector's clock. Verify that claim end to end: convict, then feed
// a fresh heartbeat, and confirm the peer comes back.
func TestEvaluateLiveness_RevivesAfterFreshHeartbeatFollowingConviction(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"

	g.applyDeltas(map[string]EndpointState{peer: {Heartbeat: HeartbeatState{Generation: 1, Version: 1}}})
	forceSilence(g, peer, time.Hour)
	g.evaluateLiveness()

	g.mu.RLock()
	convicted := !g.endpoints[peer].live
	g.mu.RUnlock()
	if !convicted {
		t.Fatal("setup failed: peer should have been convicted before testing revival")
	}

	var events []Event
	g.Subscribe(func(e Event) { events = append(events, e) })

	g.applyDeltas(map[string]EndpointState{peer: {Heartbeat: HeartbeatState{Generation: 1, Version: 2}}})
	g.evaluateLiveness()

	g.mu.RLock()
	live := g.endpoints[peer].live
	g.mu.RUnlock()
	if !live {
		t.Fatal("peer should have revived after a fresh heartbeat")
	}
	if !hasEvent(events, EventUp, peer) {
		t.Errorf("no UP event emitted across the revival, got %+v", events)
	}
}

// Self must never be convicted, however stale its bookkeeping looks — a node
// declaring itself dead makes no sense and would wreck Members()/Stats().
func TestEvaluateLiveness_NeverConvictsSelf(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	forceSilence(g, g.self, time.Hour)

	g.evaluateLiveness()

	g.mu.RLock()
	live := g.endpoints[g.self].live
	g.mu.RUnlock()
	if !live {
		t.Fatal("self must never be convicted by evaluateLiveness")
	}
}

// A peer we only know about because it contacted us (noteContact) has no
// arrival distribution yet. evaluateLiveness must leave it alone rather than
// convict on zero evidence.
func TestEvaluateLiveness_NeverHeardFromIsLeftAlone(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"
	g.noteContact(peer)

	var events []Event
	g.Subscribe(func(e Event) { events = append(events, e) })
	g.evaluateLiveness()

	if len(events) != 0 {
		t.Errorf("expected no events for a peer with no arrival evidence, got %+v", events)
	}
	g.mu.RLock()
	live := g.endpoints[peer].live
	g.mu.RUnlock()
	if live {
		t.Error("an unproven peer must not be marked live")
	}
}

// ---------------------------------------------------------------------------
// noteContact
// ---------------------------------------------------------------------------

func TestNoteContact_AddsUnprovenPeer(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"

	g.noteContact(peer)

	g.mu.RLock()
	rec, ok := g.endpoints[peer]
	g.mu.RUnlock()
	if !ok {
		t.Fatal("noteContact did not add the peer")
	}
	if rec.live {
		t.Error("a peer discovered only via contact must start unproven, not live")
	}
	if !rec.lastUpdated.IsZero() {
		t.Error("lastUpdated should stay zero until real state arrives")
	}
}

func TestNoteContact_DoesNotClobberKnownPeer(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"

	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 1, Version: 5},
		AppState:  map[string]VersionedValue{"STATUS": {Value: "NORMAL", Version: 5}},
	}})

	g.noteContact(peer) // must be a no-op: we already know real state

	g.mu.RLock()
	rec := g.endpoints[peer]
	g.mu.RUnlock()
	if rec.state.Heartbeat.Version != 5 {
		t.Error("noteContact overwrote known heartbeat state for an already-known peer")
	}
	if rec.state.AppState["STATUS"].Value != "NORMAL" {
		t.Error("noteContact clobbered application state for an already-known peer")
	}
}

// ---------------------------------------------------------------------------
// Members / Stats
// ---------------------------------------------------------------------------

func TestMembers_ReflectsSelfAndPeers(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	g.SetAppState(AppDC, "DC7")

	peer := "10.0.0.9:7000"
	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 1, Version: 3},
		AppState:  map[string]VersionedValue{"DC": {Value: "DC9", Version: 3}},
	}})

	members := g.Members()
	if len(members) != 2 {
		t.Fatalf("Members() returned %d entries, want 2", len(members))
	}
	if members[0].Endpoint > members[1].Endpoint {
		t.Errorf("Members() not sorted by endpoint: %s before %s", members[0].Endpoint, members[1].Endpoint)
	}

	var self, other *MemberView
	for i := range members {
		if members[i].Self {
			self = &members[i]
		} else {
			other = &members[i]
		}
	}
	if self == nil || other == nil {
		t.Fatal("expected exactly one self entry and one peer entry")
	}
	if !self.Live {
		t.Error("self must always report live")
	}
	if self.Phi != 0 {
		t.Errorf("self phi = %v, want 0", self.Phi)
	}
	if self.AppState["DC"] != "DC7" {
		t.Errorf("self app state DC = %q, want DC7", self.AppState["DC"])
	}
	if other.Endpoint != peer {
		t.Errorf("other endpoint = %q, want %q", other.Endpoint, peer)
	}
	if other.AppState["DC"] != "DC9" {
		t.Errorf("peer app state DC = %q, want DC9", other.AppState["DC"])
	}
	if other.Version != 3 {
		t.Errorf("peer version = %d, want 3", other.Version)
	}
	if other.Self {
		t.Error("peer entry incorrectly marked as self")
	}
}

func TestStats_CountsLiveAndDead(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")

	live := "10.0.0.1:7000"
	dead := "10.0.0.2:7000"
	g.applyDeltas(map[string]EndpointState{
		live: {Heartbeat: HeartbeatState{Generation: 1, Version: 1}},
		dead: {Heartbeat: HeartbeatState{Generation: 1, Version: 1}},
	})
	g.mu.Lock()
	g.endpoints[dead].live = false
	g.mu.Unlock()

	stats := g.Stats()
	if stats.Self != g.self {
		t.Errorf("Self = %q, want %q", stats.Self, g.self)
	}
	if stats.Known != 3 { // self + live peer + dead peer
		t.Errorf("Known = %d, want 3", stats.Known)
	}
	if stats.LiveCount != 2 { // self + live peer
		t.Errorf("LiveCount = %d, want 2", stats.LiveCount)
	}
	if stats.DeadCount != 1 {
		t.Errorf("DeadCount = %d, want 1", stats.DeadCount)
	}
}

func TestStats_TracksSynCounters(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")

	// A loopback address is always routable, even with nothing listening on
	// it, so the send itself succeeds and the counter increments. A private
	// LAN address like 10.0.0.9 may have no route at all in a sandboxed test
	// environment, failing Send() synchronously before the counter bumps.
	const target = "127.0.0.1:59999"
	g.sendSyn(target, g.buildDigests())
	g.onMessage(Message{Type: MsgSyn, From: target, Cluster: g.cfg.Cluster}, nil)

	stats := g.Stats()
	if stats.SynSent != 1 {
		t.Errorf("SynSent = %d, want 1", stats.SynSent)
	}
	if stats.SynRecv != 1 {
		t.Errorf("SynRecv = %d, want 1", stats.SynRecv)
	}
}

func TestRound2_RoundsToTwoDecimalPlaces(t *testing.T) {
	cases := map[float64]float64{
		0:      0,
		8.1234: 8.12,
		8.129:  8.13,
	}
	for in, want := range cases {
		if got := round2(in); got != want {
			t.Errorf("round2(%v) = %v, want %v", in, got, want)
		}
	}
}
