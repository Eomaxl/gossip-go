package gossip

import (
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"
)

func newTestGossiper(t *testing.T, bind string, seeds ...string) *Gossiper {
	t.Helper()
	g, err := New(Config{
		Bind:           bind,
		Seeds:          seeds,
		Cluster:        "unit-test",
		GossipInterval: 50 * time.Millisecond,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Rand:           rand.New(rand.NewSource(1)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(g.Stop)
	return g
}

// A newer generation must replace state wholesale, not merge.
//
// This is the rule that stops a restarted node from appearing to hold state
// from both incarnations at once. Merging here is the classic bug: the node
// comes back with STATUS=BOOTSTRAPPING, but the old STATUS=NORMAL has a higher
// version number within the merged record and wins.
func TestApplyDeltas_HigherGenerationReplacesWholesale(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"

	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 100, Version: 50},
		AppState: map[string]VersionedValue{
			"STATUS": {Value: "NORMAL", Version: 50},
			"STALE":  {Value: "old-incarnation", Version: 49},
		},
	}})

	// Restart: lower version numbers, higher generation.
	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 200, Version: 1},
		AppState: map[string]VersionedValue{
			"STATUS": {Value: "BOOTSTRAPPING", Version: 1},
		},
	}})

	g.mu.RLock()
	rec := g.endpoints[peer]
	g.mu.RUnlock()

	if got := rec.state.Heartbeat.Generation; got != 200 {
		t.Fatalf("generation = %d, want 200", got)
	}
	if got := rec.state.AppState["STATUS"].Value; got != "BOOTSTRAPPING" {
		t.Errorf("STATUS = %q, want BOOTSTRAPPING (lower version must still win across a generation bump)", got)
	}
	if _, ok := rec.state.AppState["STALE"]; ok {
		t.Error("state from the previous incarnation survived the restart; replacement must be wholesale")
	}
}

// Gossip about a dead incarnation is still circulating for a while after a
// restart. Accepting it would resurrect the old identity and make the node flap.
func TestApplyDeltas_LowerGenerationIgnored(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"

	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 200, Version: 5},
		AppState:  map[string]VersionedValue{"STATUS": {Value: "NORMAL", Version: 5}},
	}})
	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 100, Version: 9999},
		AppState:  map[string]VersionedValue{"STATUS": {Value: "GHOST", Version: 9999}},
	}})

	g.mu.RLock()
	rec := g.endpoints[peer]
	g.mu.RUnlock()

	if rec.state.Heartbeat.Generation != 200 {
		t.Fatalf("generation = %d, want 200", rec.state.Heartbeat.Generation)
	}
	if got := rec.state.AppState["STATUS"].Value; got != "NORMAL" {
		t.Errorf("STATUS = %q; stale-generation gossip must not be applied even with a huge version", got)
	}
}

// We are the only authority on our own heartbeat. Accepting a peer's copy would
// let our own version counter go backwards.
func TestApplyDeltas_NeverAcceptsStateAboutSelf(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")

	g.mu.RLock()
	before := g.endpoints[g.self].state.Heartbeat
	g.mu.RUnlock()

	g.applyDeltas(map[string]EndpointState{g.self: {
		Heartbeat: HeartbeatState{Generation: before.Generation + 500, Version: 1},
		AppState:  map[string]VersionedValue{"STATUS": {Value: "HIJACKED", Version: 9999}},
	}})

	g.mu.RLock()
	after := g.endpoints[g.self].state
	g.mu.RUnlock()

	if after.Heartbeat != before {
		t.Errorf("self heartbeat mutated by remote delta: %+v -> %+v", before, after.Heartbeat)
	}
	if _, ok := after.AppState["STATUS"]; ok {
		t.Error("remote delta wrote application state about self")
	}
}

// Same generation: merge field by field, highest version wins per key.
func TestApplyDeltas_SameGenerationMergesPerField(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	peer := "10.0.0.9:7000"

	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 100, Version: 10},
		AppState: map[string]VersionedValue{
			"STATUS": {Value: "NORMAL", Version: 10},
			"LOAD":   {Value: "500", Version: 9},
		},
	}})
	g.applyDeltas(map[string]EndpointState{peer: {
		Heartbeat: HeartbeatState{Generation: 100, Version: 12},
		AppState: map[string]VersionedValue{
			"LOAD":   {Value: "750", Version: 12},
			"STATUS": {Value: "REGRESSED", Version: 3}, // older, must lose
		},
	}})

	g.mu.RLock()
	rec := g.endpoints[peer]
	g.mu.RUnlock()

	if got := rec.state.AppState["LOAD"].Value; got != "750" {
		t.Errorf("LOAD = %q, want 750", got)
	}
	if got := rec.state.AppState["STATUS"].Value; got != "NORMAL" {
		t.Errorf("STATUS = %q, want NORMAL (lower version must not overwrite)", got)
	}
	if got := rec.state.Heartbeat.Version; got != 12 {
		t.Errorf("heartbeat version = %d, want 12", got)
	}
}

// examine must classify each endpoint into exactly one of: request, push, ignore.
func TestExamine_ClassifiesDigestsThreeWays(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")

	g.applyDeltas(map[string]EndpointState{
		"behind:7000": {Heartbeat: HeartbeatState{Generation: 1, Version: 5}},
		"ahead:7000":  {Heartbeat: HeartbeatState{Generation: 1, Version: 50}},
		"level:7000":  {Heartbeat: HeartbeatState{Generation: 1, Version: 20}},
	})

	wanted, newer := g.examine([]Digest{
		{Endpoint: "behind:7000", Generation: 1, MaxVersion: 99}, // peer ahead -> request
		{Endpoint: "ahead:7000", Generation: 1, MaxVersion: 10},  // we ahead   -> push
		{Endpoint: "level:7000", Generation: 1, MaxVersion: 20},  // equal      -> silence
		{Endpoint: "unknown:7000", Generation: 7, MaxVersion: 3}, // new        -> request
	})

	want := map[string]bool{"behind:7000": true, "unknown:7000": true}
	if len(wanted) != len(want) {
		t.Fatalf("wanted %d digests, got %d: %+v", len(want), len(wanted), wanted)
	}
	for _, d := range wanted {
		if !want[d.Endpoint] {
			t.Errorf("unexpected request for %s", d.Endpoint)
		}
	}
	if _, ok := newer["ahead:7000"]; !ok {
		t.Error("expected to push state for ahead:7000")
	}
	if _, ok := newer["level:7000"]; ok {
		t.Error("pushed state for an endpoint at an equal version; converged clusters must go quiet")
	}
}

// since() is what bounds gossip bandwidth: only entries above the peer's
// watermark travel.
func TestEndpointState_SinceReturnsOnlyNewerEntries(t *testing.T) {
	es := EndpointState{
		Heartbeat: HeartbeatState{Generation: 1, Version: 30},
		AppState: map[string]VersionedValue{
			"A": {Value: "a", Version: 10},
			"B": {Value: "b", Version: 25},
			"C": {Value: "c", Version: 30},
		},
	}
	got := es.since(20)
	if len(got.AppState) != 2 {
		t.Fatalf("since(20) returned %d entries, want 2: %+v", len(got.AppState), got.AppState)
	}
	if _, ok := got.AppState["A"]; ok {
		t.Error("entry at version 10 leaked into since(20)")
	}
	if es.MaxVersion() != 30 {
		t.Errorf("MaxVersion = %d, want 30", es.MaxVersion())
	}
}

// Phi must stay near zero while heartbeats arrive and climb past the threshold
// once they stop.
func TestFailureDetector_PhiRisesWithSilence(t *testing.T) {
	fd := NewFailureDetector(time.Second)
	base := time.Now()
	for i := 0; i < 50; i++ {
		fd.Report(base.Add(time.Duration(i) * time.Second))
	}
	last := base.Add(49 * time.Second)

	if phi := fd.Phi(last.Add(500 * time.Millisecond)); phi > 1.0 {
		t.Errorf("phi = %.2f half an interval after a heartbeat; want < 1.0", phi)
	}
	quiet := fd.Phi(last.Add(30 * time.Second))
	if quiet <= DefaultPhiThreshold {
		t.Errorf("phi = %.2f after 30s of silence on a 1s cadence; want > %.1f", quiet, DefaultPhiThreshold)
	}
	// Linear growth under the exponential model: doubling silence doubles phi.
	if double := fd.Phi(last.Add(60 * time.Second)); double < quiet*1.9 {
		t.Errorf("phi(60s)=%.2f not ~2x phi(30s)=%.2f", double, quiet)
	}
}

// A restart invalidates the arrival distribution; Reset must clear it.
func TestFailureDetector_ResetClearsWindow(t *testing.T) {
	fd := NewFailureDetector(time.Second)
	base := time.Now()
	for i := 0; i < 10; i++ {
		fd.Report(base.Add(time.Duration(i) * time.Second))
	}
	fd.Reset()
	if phi := fd.Phi(base.Add(time.Hour)); phi != 0 {
		t.Errorf("phi = %.2f after Reset with no new arrivals, want 0", phi)
	}
}

// Digest lists must stay inside one datagram no matter how big the cluster is.
func TestBuildDigests_CapsAtMaxPerMessage(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	deltas := make(map[string]EndpointState)
	for i := 0; i < MaxDigestsPerMessage*3; i++ {
		deltas[string(rune('a'+i%26))+time.Duration(i).String()] =
			EndpointState{Heartbeat: HeartbeatState{Generation: 1, Version: int64(i)}}
	}
	g.applyDeltas(deltas)

	if n := len(g.buildDigests()); n > MaxDigestsPerMessage {
		t.Errorf("buildDigests returned %d digests, cap is %d", n, MaxDigestsPerMessage)
	}
}

// Two nodes on different cluster names share a network but must not merge.
func TestOnMessage_RejectsForeignCluster(t *testing.T) {
	g := newTestGossiper(t, "127.0.0.1:0")
	g.onMessage(Message{
		Type: MsgAck2, From: "10.0.0.1:7000", Cluster: "some-other-cluster",
		Deltas: map[string]EndpointState{
			"10.0.0.1:7000": {Heartbeat: HeartbeatState{Generation: 1, Version: 1}},
		},
	}, nil)

	g.mu.RLock()
	_, leaked := g.endpoints["10.0.0.1:7000"]
	g.mu.RUnlock()
	if leaked {
		t.Error("state from a foreign cluster was absorbed")
	}
}
