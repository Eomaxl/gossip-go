package gossip

import (
	"math"
	"sync"
	"time"
)

const (
	// defaultWindowSize is how many inter-arrival intervals we keep. Cassandra
	// uses 1000. Large enough to absorb a GC pause without the mean lurching,
	// small enough to adapt when the network's baseline genuinely shifts.
	defaultWindowSize = 1000

	// DefaultPhiThreshold matches Cassandra's phi_convict_threshold default.
	// Under the exponential model below, phi 8 corresponds to roughly 18x the
	// observed mean inter-arrival time.
	DefaultPhiThreshold = 8.0

	// phiFactor converts the natural-log-based exponential survival function
	// into a base-10 suspicion value, so phi = 8 reads as "probability of a
	// false positive is about 1 in 10^8".
	phiFactor = 1.0 / math.Ln10
)

// FailureDetector implements the phi accrual algorithm (Hayashibara et al.,
// 2004) in the form Cassandra actually ships.
//
// The point of accrual detection is that it does not answer "is this node
// dead?" with a boolean. It answers "how surprised should I be that I haven't
// heard from this node?" with a continuous value, and leaves the threshold to
// the caller. That matters because the right threshold is a business decision,
// not a networking one: a system that tolerates a 30-second blip wants a
// different number than one that must fail over in two.
//
// It also self-calibrates. A fixed timeout tuned for a 1ms LAN will convict
// constantly on a 40ms cross-region link. Phi is expressed in units of the
// observed distribution, so the same threshold behaves sanely on both.
type FailureDetector struct {
	mu         sync.Mutex
	intervals  []float64 // milliseconds, ring buffer
	windowSize int
	idx        int
	full       bool
	sum        float64
	last       time.Time
	// bootstrapInterval seeds the window so the first few rounds have a mean
	// to divide by. Without it a node that has been heard from exactly once
	// has an undefined phi, and the obvious fallbacks (0 or +Inf) are both
	// wrong in a way that shows up as either blindness or instant conviction.
	bootstrapInterval float64
}

func NewFailureDetector(gossipInterval time.Duration) *FailureDetector {
	return &FailureDetector{
		intervals:         make([]float64, 0, defaultWindowSize),
		windowSize:        defaultWindowSize,
		bootstrapInterval: float64(gossipInterval.Milliseconds()),
	}
}

// Report records that fresh evidence of liveness arrived at time now.
//
// "Fresh evidence" is deliberately narrow: it means we applied a heartbeat with
// a higher version than the one we held. Receiving a packet that tells us
// nothing new is not evidence the peer is alive — it may be a third party
// replaying stale state it heard about minutes ago. Conflating the two is a
// subtle way to build a detector that never convicts anyone.
func (f *FailureDetector) Report(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.last.IsZero() {
		f.last = now
		f.push(f.bootstrapInterval)
		return
	}
	delta := float64(now.Sub(f.last).Milliseconds())
	f.last = now
	if delta <= 0 {
		delta = 1
	}
	f.push(delta)
}

func (f *FailureDetector) push(v float64) {
	if len(f.intervals) < f.windowSize {
		f.intervals = append(f.intervals, v)
		f.sum += v
		return
	}
	f.sum -= f.intervals[f.idx]
	f.intervals[f.idx] = v
	f.sum += v
	f.idx = (f.idx + 1) % f.windowSize
}

func (f *FailureDetector) mean() float64 {
	if len(f.intervals) == 0 {
		return f.bootstrapInterval
	}
	return f.sum / float64(len(f.intervals))
}

// Phi returns the current suspicion level.
//
// Cassandra models inter-arrival times as exponentially distributed, which
// collapses the survival function to a division:
//
//	P(no heartbeat for t | mean m) = e^(-t/m)
//	phi = -log10(P) = (1/ln 10) * t/m
//
// The original paper assumes a normal distribution and computes the CDF
// properly. The exponential form is cheaper and, on real networks where
// arrivals cluster near the mean with a long right tail, arguably the better
// fit. It is also why phi grows linearly with silence rather than exploding:
// the curve is forgiving of a pause and decisive about an absence.
func (f *FailureDetector) Phi(now time.Time) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.last.IsZero() {
		return 0
	}
	t := float64(now.Sub(f.last).Milliseconds())
	if t <= 0 {
		return 0
	}
	m := f.mean()
	if m <= 0 {
		return 0
	}
	return phiFactor * t / m
}

// Reset clears the window. Called when a peer's generation increases, i.e. the
// process restarted. Its old arrival distribution describes a process that no
// longer exists, and carrying it forward would mean judging the new instance by
// the dying moments of the old one.
func (f *FailureDetector) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intervals = f.intervals[:0]
	f.idx = 0
	f.sum = 0
	f.last = time.Time{}
}
