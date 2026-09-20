package gossip

import (
	"io"
	"log/slog"
	"math/rand"
	"net"
	"testing"
	"time"
)

// freeUDPAddr reserves an ephemeral port and hands back its address, then
// releases it. Gossiper.New treats Advertise (defaulting to Bind) as the
// literal address peers reply to, so a real two-node test needs a concrete
// resolvable address up front — binding to "127.0.0.1:0" would leave the
// node advertising the literal, unroutable string "127.0.0.1:0" as itself.
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("reserve a free port: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	return addr
}

func newTestGossiperAt(t *testing.T, bind string, seeds ...string) *Gossiper {
	t.Helper()
	g, err := New(Config{
		Bind:           bind,
		Seeds:          seeds,
		Cluster:        "integration-test",
		GossipInterval: 30 * time.Millisecond,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Rand:           rand.New(rand.NewSource(time.Now().UnixNano())),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(g.Stop)
	return g
}

func allLive(members []MemberView) bool {
	for _, m := range members {
		if !m.Live {
			return false
		}
	}
	return true
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

// End-to-end over real UDP sockets: two nodes discover each other through a
// seed, converge on liveness, propagate application state, and one convicts
// the other once it goes silent. This is the path none of the unit tests
// touch — real Transport.Serve/Send, onMessage's full SYN/ACK/ACK2 dance,
// and evaluateLiveness running off the actual gossip ticker rather than a
// hand-cranked call.
func TestIntegration_TwoNodesConvergeAndDetectFailure(t *testing.T) {
	a := freeUDPAddr(t)
	b := freeUDPAddr(t)

	g1 := newTestGossiperAt(t, a)    // no seeds; discovered via being contacted
	g2 := newTestGossiperAt(t, b, a) // seeds off g1

	g1.Start()
	g2.Start()

	waitFor(t, 4*time.Second, "nodes did not discover each other and converge on liveness", func() bool {
		m1, m2 := g1.Members(), g2.Members()
		return len(m1) == 2 && len(m2) == 2 && allLive(m1) && allLive(m2)
	})

	g1.SetAppState("FOO", "bar")
	waitFor(t, 3*time.Second, "application state never propagated from node1 to node2", func() bool {
		for _, m := range g2.Members() {
			if m.Endpoint == a {
				return m.AppState["FOO"] == "bar"
			}
		}
		return false
	})

	// Simulate a crash: stop node1 entirely. Node2 must convict it via phi
	// once it stops hearing heartbeats, with no special-cased logic.
	g1.Stop()
	waitFor(t, 8*time.Second, "node2 never convicted the stopped peer", func() bool {
		for _, m := range g2.Members() {
			if m.Endpoint == a {
				return !m.Live
			}
		}
		return false
	})
}
