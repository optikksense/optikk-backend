// Package indexer assembles per-trace summary rows from the stream of raw
// spans consumed by the spans pipeline. State is bounded in memory; on LRU
// eviction the partial summary is emitted with truncated=true so the trace
// is still visible in the explorer (loses span-set completeness only).
//
// Flow:
//   Observe(span) → Pending[trace_id] updated
//   Quiesce scan (every 10s) → any pending that hit:
//     (a) root span seen AND no spans for 10s, OR
//     (b) first span older than 60s (wall-clock hard timeout),
//   → emit TraceIndexRow via Emitter; remove from Pending.
package indexer

import (
	"sync"
	"time"
)

// TraceIndexRow is the wire shape emitted to observability.traces_index.
// Mirrors the schema in 38_traces_index.sql.
type TraceIndexRow struct {
	TeamID         uint32
	TraceID        string
	StartMs        int64
	EndMs          int64
	DurationNs     int64
	RootService    string
	RootOperation  string
	RootStatus     string
	RootHTTPMethod string
	RootHTTPStatus string
	SpanCount      uint32
	HasError       bool
	ErrorCount     uint32
	ServiceSet     []string
	PeerServiceSet []string
	ErrorFp        string
	Truncated      bool
	LastSeenMs     time.Time
	TsBucketStart  uint32
}

// pending is the in-memory accumulator for one in-flight trace.
type pending struct {
	teamID        uint32
	traceID       string
	firstSeen     time.Time
	lastSeen      time.Time
	rootSeen      bool
	tsBucketStart uint32

	// Populated from the root span (when seen).
	rootService    string
	rootOperation  string
	rootStatus     string
	rootHTTPMethod string
	rootHTTPStatus string
	startMs        int64
	endMs          int64

	// Running over the whole trace.
	spanCount  uint32
	errorCount uint32
	services   map[string]struct{}
	peers      map[string]struct{}
	errorFp    string
}

// state is an LRU-bounded map of in-flight traces keyed by trace_id.
// Capacity bound is enforced on every Insert; eviction emits a truncated
// summary via the eviction callback (wired by the assembler).
type state struct {
	mu       sync.Mutex
	byTrace  map[string]*pending
	capacity int
}

func newState(capacity int) *state {
	if capacity <= 0 {
		capacity = 100_000
	}
	return &state{byTrace: make(map[string]*pending, capacity/4), capacity: capacity}
}

// Len is safe for concurrent reads from metrics.
func (s *state) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byTrace)
}

// Upsert fetches or creates the pending entry for (teamID, traceID). Returns
// the pending plus an `evicted` slice of entries the caller should emit as
// truncated so the memory bound is respected.
func (s *state) Upsert(teamID uint32, traceID string, now time.Time, tsBucketStart uint32) (*pending, []*pending) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.byTrace[traceID]
	if !ok {
		p = &pending{
			teamID:        teamID,
			traceID:       traceID,
			firstSeen:     now,
			tsBucketStart: tsBucketStart,
			services:      map[string]struct{}{},
			peers:         map[string]struct{}{},
		}
		s.byTrace[traceID] = p
	}
	p.lastSeen = now
	var evicted []*pending
	for len(s.byTrace) > s.capacity {
		evicted = append(evicted, s.evictOldestLocked())
	}
	return p, evicted
}

// Sweep removes traces that meet completion criteria. Caller owns emitting
// the returned rows — Sweep itself only mutates internal state.
func (s *state) Sweep(now time.Time, quietWindow, hardTimeout time.Duration) []*pending {
	s.mu.Lock()
	defer s.mu.Unlock()
	var done []*pending
	for tid, p := range s.byTrace {
		quiet := now.Sub(p.lastSeen)
		age := now.Sub(p.firstSeen)
		if (p.rootSeen && quiet >= quietWindow) || age >= hardTimeout {
			done = append(done, p)
			delete(s.byTrace, tid)
		}
	}
	return done
}

func (s *state) evictOldestLocked() *pending {
	var oldestID string
	var oldest *pending
	for id, p := range s.byTrace {
		if oldest == nil || p.firstSeen.Before(oldest.firstSeen) {
			oldestID = id
			oldest = p
		}
	}
	if oldest != nil {
		delete(s.byTrace, oldestID)
	}
	return oldest
}

// ToRow materialises the pending entry into the CH insert shape. truncated
// is set by caller when emission came from eviction or hard timeout.
func (p *pending) ToRow(truncated bool) TraceIndexRow {
	services := setToSlice(p.services)
	peers := setToSlice(p.peers)
	endMs := p.endMs
	if endMs == 0 {
		endMs = p.lastSeen.UnixMilli()
	}
	durNs := (endMs - p.startMs) * 1_000_000
	if durNs < 0 {
		durNs = 0
	}
	return TraceIndexRow{
		TeamID:         p.teamID,
		TraceID:        p.traceID,
		StartMs:        p.startMs,
		EndMs:          endMs,
		DurationNs:     durNs,
		RootService:    p.rootService,
		RootOperation:  p.rootOperation,
		RootStatus:     p.rootStatus,
		RootHTTPMethod: p.rootHTTPMethod,
		RootHTTPStatus: p.rootHTTPStatus,
		SpanCount:      p.spanCount,
		HasError:       p.errorCount > 0,
		ErrorCount:     p.errorCount,
		ServiceSet:     services,
		PeerServiceSet: peers,
		ErrorFp:        p.errorFp,
		Truncated:      truncated,
		LastSeenMs:     p.lastSeen,
		TsBucketStart:  p.tsBucketStart,
	}
}

func setToSlice(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
