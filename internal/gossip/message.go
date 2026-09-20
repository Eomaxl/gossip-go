package gossip

// Digest is a fixed-size summary of one endpoint's state.
//
// endpoint:generation:maxVersion — about 60 bytes on the wire regardless of how
// much application state that endpoint actually carries. A node advertises what
// it knows using digests, and only the parts that turn out to be stale get
// transferred as real payload.
//
// This is the single most important design decision in the protocol. Without
// digests, every gossip round would ship the full cluster state to a random
// peer every second, and bandwidth would scale with O(N) state per node.
type Digest struct {
	Endpoint   string `json:"endpoint"`
	Generation int64  `json:"generation"`
	MaxVersion int64  `json:"max_version"`
}

type MessageType string

const (
	// MsgSyn opens a round: "here is a digest of everything I know."
	MsgSyn MessageType = "SYN"
	// MsgAck answers: "here is what I have that you don't, and here is a
	// digest of what I want from you."
	MsgAck MessageType = "ACK"
	// MsgAck2 closes it: "here is what you asked for."
	MsgAck2 MessageType = "ACK2"
)

// Message is the single envelope for all three phases.
//
// The three-phase handshake is not ceremony. A two-phase exchange would force
// the initiator to either send its full state speculatively (wasteful) or
// finish a round knowing less than its peer (slow). Three phases mean each side
// transfers exactly the delta the other is missing, in one round trip, with the
// digest exchange costing a constant.
type Message struct {
	Type MessageType `json:"type"`
	// From is the advertised address of the sender, not the UDP source. On a
	// multi-homed or NAT'd host these differ, and every reply must go to the
	// advertised one or the cluster forms a one-way view.
	From string `json:"from"`
	// Cluster mirrors Cassandra's cluster_name guard. Two logically separate
	// clusters sharing a subnet will exchange packets; without this check they
	// will also merge into one cluster, which is a genuinely bad afternoon.
	Cluster string `json:"cluster"`

	// Digests carries the SYN's full advertisement, or the ACK's request list.
	Digests []Digest `json:"digests,omitempty"`
	// Deltas carries actual state, keyed by endpoint.
	Deltas map[string]EndpointState `json:"deltas,omitempty"`
}
