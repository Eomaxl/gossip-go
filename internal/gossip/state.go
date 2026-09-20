package gossip

import (
	"sync/atomic"
	"time"
)

// versionGenerator is a node-local, monotonically increasing counter.
//
// Every mutation to any piece of state this node owns — a heartbeat bump, an
// application-state write — consumes the next value. There is no cluster-wide
// clock and no coordination: versions are only ever meaningful within a single
// (endpoint, generation) pair. Node A's version 42 and node B's version 42 have
// nothing to do with each other.
//
// This is the same trick Cassandra's VersionGenerator uses. It is what makes
// "who has newer data" answerable with an integer comparison instead of a
// distributed consensus round.
type versionGenerator struct{ n int64 }

func (v *versionGenerator) next() int64 { return atomic.AddInt64(&v.n, 1) }

// HeartbeatState is the liveness signal for one endpoint.
//
// Generation is the wall-clock second at which the process booted. It never
// changes while the process is alive, and it strictly increases across
// restarts. That single fact is what lets the cluster distinguish "this node
// has new information" from "this node restarted and everything I knew about it
// is now garbage".
//
// Version increments once per gossip round. A peer whose Version stops
// advancing is the input to the failure detector.
type HeartbeatState struct {
	Generation int64 `json:"generation"`
	Version    int64 `json:"version"`
}

// VersionedValue is one application-level fact about an endpoint, stamped with
// the version of the owning node at the time it was written.
//
// Cassandra carries STATUS, LOAD, SCHEMA, DC, RACK, RELEASE_VERSION,
// HOST_ID, TOKENS and others in exactly this shape.
type VersionedValue struct {
	Value   string `json:"value"`
	Version int64  `json:"version"`
}

// EndpointState is everything the cluster knows about one node. This is the
// unit that travels over the wire.
type EndpointState struct {
	Heartbeat HeartbeatState            `json:"heartbeat"`
	AppState  map[string]VersionedValue `json:"app_state,omitempty"`
}

// MaxVersion returns the highest version present anywhere in this state.
//
// This single number is what goes into a digest. It is a cheap, lossy summary:
// "everything I know about this endpoint is at or below version N". A peer can
// compare it against its own and decide, without transferring any payload,
// whether it is ahead, behind, or level.
func (e EndpointState) MaxVersion() int64 {
	m := e.Heartbeat.Version
	for _, vv := range e.AppState {
		if vv.Version > m {
			m = vv.Version
		}
	}
	return m
}

// clone produces a deep copy. Gossip state is read concurrently by the HTTP
// handler and the gossip loop; handing out the live map would be a data race.
func (e EndpointState) clone() EndpointState {
	out := EndpointState{Heartbeat: e.Heartbeat}
	if e.AppState != nil {
		out.AppState = make(map[string]VersionedValue, len(e.AppState))
		for k, v := range e.AppState {
			out.AppState[k] = v
		}
	}
	return out
}

// since returns a partial EndpointState containing only the application entries
// written after version v.
//
// This is the delta that makes gossip's bandwidth bounded. If a peer says "I
// have everything up to version 90" and we are at 95, we ship the five entries
// that changed — not the whole endpoint record.
//
// The heartbeat is always included. It is 16 bytes, it changes every round
// anyway, and carrying the generation lets the receiver validate the delta
// against a restart it may not have heard about yet.
func (e EndpointState) since(v int64) EndpointState {
	out := EndpointState{Heartbeat: e.Heartbeat}
	for k, vv := range e.AppState {
		if vv.Version > v {
			if out.AppState == nil {
				out.AppState = make(map[string]VersionedValue)
			}
			out.AppState[k] = vv
		}
	}
	return out
}

// endpointRecord wraps EndpointState with data that is strictly local and never
// gossiped: our own liveness verdict about the peer, and the arrival-time window
// the failure detector reasons over.
//
// Keeping these out of EndpointState is deliberate. Liveness is an opinion each
// node forms independently from evidence it observed directly. Gossiping the
// opinion instead of the evidence is how you build a system where one node's
// false positive becomes everyone's.
type endpointRecord struct {
	state       EndpointState
	live        bool
	fd          *FailureDetector
	lastUpdated time.Time
	firstSeen   time.Time
}

// MemberView is the read-only projection exposed over HTTP.
type MemberView struct {
	Endpoint   string            `json:"endpoint"`
	Live       bool              `json:"live"`
	Phi        float64           `json:"phi"`
	Generation int64             `json:"generation"`
	Version    int64             `json:"version"`
	AppState   map[string]string `json:"app_state"`
	Self       bool              `json:"self"`
}
