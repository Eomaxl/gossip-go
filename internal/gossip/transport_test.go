package gossip

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func newTestTransport(t *testing.T) *Transport {
	t.Helper()
	tr, err := NewTransport("127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// A message sent by one Transport must arrive at the other, decoded intact,
// with the sender's real UDP source address attached.
func TestTransport_SendReceiveRoundTrip(t *testing.T) {
	recv := newTestTransport(t)
	send := newTestTransport(t)

	got := make(chan Message, 1)
	recv.Handle(func(m Message, src *net.UDPAddr) { got <- m })
	go recv.Serve()

	msg := Message{
		Type: MsgSyn, From: send.LocalAddr(), Cluster: "test",
		Digests: []Digest{{Endpoint: "x:1", Generation: 1, MaxVersion: 2}},
	}
	if err := send.Send(recv.LocalAddr(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-got:
		if m.Type != MsgSyn || m.From != send.LocalAddr() || m.Cluster != "test" {
			t.Fatalf("received message mismatch: %+v", m)
		}
		if len(m.Digests) != 1 || m.Digests[0].Endpoint != "x:1" || m.Digests[0].MaxVersion != 2 {
			t.Fatalf("digest payload mismatch: %+v", m.Digests)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message never arrived")
	}
}

// A malformed (non-JSON) datagram must be dropped, not crash the read loop or
// wedge it — a real risk on a socket that also receives garbage from the
// occasional stray packet or a peer running an incompatible version.
func TestTransport_MalformedDatagramDoesNotStopServe(t *testing.T) {
	recv := newTestTransport(t)
	send := newTestTransport(t)

	got := make(chan Message, 1)
	recv.Handle(func(m Message, src *net.UDPAddr) { got <- m })
	go recv.Serve()

	raddr, err := net.ResolveUDPAddr("udp", recv.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("not-json-at-all")); err != nil {
		t.Fatal(err)
	}
	conn.Close()

	select {
	case m := <-got:
		t.Fatalf("handler invoked for a malformed datagram: %+v", m)
	case <-time.After(200 * time.Millisecond):
	}

	// Serve must still be alive and correctly process a real message after
	// the garbage.
	msg := Message{Type: MsgSyn, From: send.LocalAddr(), Cluster: "test"}
	if err := send.Send(recv.LocalAddr(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case m := <-got:
		if m.Type != MsgSyn {
			t.Fatalf("unexpected message: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not recover after a malformed datagram")
	}
}

// Send must refuse a message that would not fit in one UDP datagram rather
// than let it fragment or silently drop.
func TestTransport_SendRejectsOversizedMessage(t *testing.T) {
	send := newTestTransport(t)

	digests := make([]Digest, 0, 3000)
	for i := 0; i < 3000; i++ {
		digests = append(digests, Digest{
			Endpoint: "some-fairly-long-endpoint-hostname.example.com:70000", Generation: 1, MaxVersion: int64(i),
		})
	}
	msg := Message{Type: MsgSyn, From: "x", Cluster: "c", Digests: digests}

	err := send.Send("127.0.0.1:1", msg)
	if err == nil {
		t.Fatal("expected an error for a message over the datagram limit, got nil")
	}
}

// resolve() must cache a resolved address rather than re-resolve on every
// send — DNS (or even parsing) on the hot path is exactly the latency
// landmine the comment on Transport warns about.
func TestTransport_ResolveCachesAddress(t *testing.T) {
	tr := newTestTransport(t)

	a, err := tr.resolve("127.0.0.1:9999")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(tr.cache) != 1 {
		t.Fatalf("cache size = %d, want 1", len(tr.cache))
	}
	b, err := tr.resolve("127.0.0.1:9999")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if a != b {
		t.Error("second resolve() for the same host did not return the cached *net.UDPAddr")
	}
	if len(tr.cache) != 1 {
		t.Fatalf("cache grew on a repeat lookup: size = %d", len(tr.cache))
	}
}

// Close must stop Serve promptly, reject further sends, and tolerate being
// called twice (Stop() calling Close() after a prior Close() must not panic).
func TestTransport_CloseStopsServeAndRejectsSend(t *testing.T) {
	tr := newTestTransport(t)

	done := make(chan struct{})
	go func() { tr.Serve(); close(done) }()

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Close")
	}

	if err := tr.Send("127.0.0.1:1", Message{Type: MsgSyn}); err == nil {
		t.Error("Send after Close should fail")
	}
	if err := tr.Close(); err != nil {
		t.Errorf("second Close returned an error, want idempotent no-op: %v", err)
	}
}

// Handle() has no documented restriction to "before Serve() starts" — it
// happens to be race-free today only because every caller sequences it that
// way. Verify the API itself is actually safe to call concurrently with a
// running Serve(), not just safe in practice because nobody does otherwise.
// Run with -race: this fails on the plain, unsynchronized handler field.
func TestTransport_HandleIsSafeConcurrentWithServe(t *testing.T) {
	recv := newTestTransport(t)
	send := newTestTransport(t)

	go recv.Serve()

	swapping := make(chan struct{})
	go func() {
		defer close(swapping)
		for i := 0; i < 200; i++ {
			recv.Handle(func(Message, *net.UDPAddr) {})
		}
	}()

	got := make(chan Message, 1)
	for i := 0; i < 50; i++ {
		recv.Handle(func(m Message, src *net.UDPAddr) {
			select {
			case got <- m:
			default:
			}
		})
		_ = send.Send(recv.LocalAddr(), Message{Type: MsgSyn, From: send.LocalAddr(), Cluster: "test"})
	}

	<-swapping
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("no message ever reached a handler while Handle() was being swapped concurrently")
	}
}

// LocalAddr must report the real OS-assigned port, not the ":0" wildcard we
// bound with — every peer's ability to reply depends on this.
func TestTransport_LocalAddrReportsBoundPort(t *testing.T) {
	tr := newTestTransport(t)
	addr := tr.LocalAddr()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("LocalAddr() = %q not a host:port: %v", addr, err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host = %q, want 127.0.0.1", host)
	}
	if port == "0" {
		t.Error("LocalAddr() still reports the wildcard port; the OS should have assigned a real one")
	}
}
