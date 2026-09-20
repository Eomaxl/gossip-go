package gossip

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// maxDatagram bounds a single read. 64KB is the theoretical UDP payload ceiling;
// in practice anything over the path MTU (~1472 bytes on Ethernet) gets
// IP-fragmented, and a single lost fragment discards the whole datagram. See
// MaxDigestsPerMessage in gossiper.go for how we keep messages small enough that
// this stays theoretical.
const maxDatagram = 65536

// Transport is a UDP socket with a JSON codec.
//
// UDP is the right choice here and the reasoning is worth stating: gossip is
// already a redundancy protocol. It sends overlapping information to random
// peers every second, forever. Layering TCP's retransmission and ordering
// guarantees underneath that buys nothing — a dropped packet is repaired by the
// next round — while costing a connection per peer, head-of-line blocking, and
// a teardown storm whenever a node dies. Cassandra made the same call.
//
// JSON is the wrong choice for production and the right one for a teaching
// artifact: you can tcpdump the wire and read it. Swapping in protobuf or
// msgpack is a codec change, not a protocol change.
type Transport struct {
	conn *net.UDPConn
	log  *slog.Logger

	// mu guards both handler and cache. In current usage Handle() is only
	// ever called once, before Serve() starts, so program order already
	// makes this race-free — but that's an implicit property of the caller,
	// not something this type should require. Locking it costs one RLock
	// per received datagram, which is cheap next to the JSON decode next to
	// it, and it means Handle() can be called at any time, by any goroutine,
	// without the API silently depending on how callers happen to sequence
	// things today.
	mu      sync.RWMutex
	handler func(Message, *net.UDPAddr)
	cache   map[string]*net.UDPAddr // resolved peers; DNS on the hot path is a latency landmine

	closeOnce sync.Once
	done      chan struct{}
}

func NewTransport(bind string, log *slog.Logger) (*Transport, error) {
	addr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, fmt.Errorf("resolve bind %q: %w", bind, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", bind, err)
	}
	return &Transport{
		conn:  conn,
		log:   log,
		cache: make(map[string]*net.UDPAddr),
		done:  make(chan struct{}),
	}, nil
}

func (t *Transport) LocalAddr() string { return t.conn.LocalAddr().String() }

func (t *Transport) Handle(fn func(Message, *net.UDPAddr)) {
	t.mu.Lock()
	t.handler = fn
	t.mu.Unlock()
}

func (t *Transport) currentHandler() func(Message, *net.UDPAddr) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.handler
}

func (t *Transport) Serve() {
	buf := make([]byte, maxDatagram)
	for {
		select {
		case <-t.done:
			return
		default:
		}
		n, src, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-t.done:
				return
			default:
				t.log.Debug("udp read failed", "err", err)
				continue
			}
		}
		var m Message
		if err := json.Unmarshal(buf[:n], &m); err != nil {
			// A malformed datagram is not worth a round trip to complain
			// about. Drop it, count it, move on.
			t.log.Debug("malformed datagram", "src", src.String(), "bytes", n)
			continue
		}
		if h := t.currentHandler(); h != nil {
			h(m, src)
		}
	}
}

func (t *Transport) Send(to string, m Message) error {
	addr, err := t.resolve(to)
	if err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode %s: %w", m.Type, err)
	}
	if len(b) > maxDatagram {
		return fmt.Errorf("message %s is %d bytes, over datagram limit", m.Type, len(b))
	}
	if _, err := t.conn.WriteToUDP(b, addr); err != nil {
		return fmt.Errorf("send %s to %s: %w", m.Type, to, err)
	}
	return nil
}

func (t *Transport) resolve(host string) (*net.UDPAddr, error) {
	t.mu.RLock()
	if a, ok := t.cache[host]; ok {
		t.mu.RUnlock()
		return a, nil
	}
	t.mu.RUnlock()

	a, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", host, err)
	}
	t.mu.Lock()
	t.cache[host] = a
	t.mu.Unlock()
	return a, nil
}

func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.done)
		err = t.conn.Close()
	})
	return err
}
