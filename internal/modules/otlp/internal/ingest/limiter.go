package ingest

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultTeamBurstRows  = int64(5000) // max burst per team
	DefaultTeamRatePerSec = int64(2000) // sustained rows/sec per team
)

type teamBucket struct {
	tokens    atomic.Int64
	lastRefil atomic.Int64 // unix nanoseconds
}

// TeamLimiter enforces per-team token-bucket rate limits on ingest rows.
// One bucket is created lazily per team and never removed (teams are long-lived).
type TeamLimiter struct {
	buckets sync.Map // int64 teamID → *teamBucket
	rate    int64    // tokens per second
	burst   int64    // max tokens
}

// NewTeamLimiter creates a limiter and starts the background refill ticker.
func NewTeamLimiter(ratePerSec, burst int64) *TeamLimiter {
	l := &TeamLimiter{rate: ratePerSec, burst: burst}
	go l.refillLoop()
	return l
}

// Allow returns true and consumes n tokens for teamID if the bucket has enough tokens.
// Returns false (deny) if the team has exceeded its rate.
func (l *TeamLimiter) Allow(teamID int64, n int64) bool {
	v, _ := l.buckets.LoadOrStore(teamID, &teamBucket{})
	b := v.(*teamBucket)
	l.refill(b)
	for {
		cur := b.tokens.Load()
		if cur < n {
			log.Printf("ingest: rate limit exceeded for team %d (tokens=%d want=%d)", teamID, cur, n)
			return false
		}
		if b.tokens.CompareAndSwap(cur, cur-n) {
			return true
		}
	}
}

// refill adds tokens proportional to elapsed time since last refill.
// Skips if called within 100ms of last refill to reduce contention.
func (l *TeamLimiter) refill(b *teamBucket) {
	now := time.Now().UnixNano()
	last := b.lastRefil.Load()
	if now-last < 1e8 { // 100ms minimum between refills
		return
	}
	if !b.lastRefil.CompareAndSwap(last, now) {
		return // another goroutine is refilling
	}
	elapsedSec := float64(now-last) / 1e9
	add := int64(float64(l.rate) * elapsedSec)
	for {
		cur := b.tokens.Load()
		next := cur + add
		if next > l.burst {
			next = l.burst
		}
		if b.tokens.CompareAndSwap(cur, next) {
			return
		}
	}
}

// refillLoop periodically refills all known team buckets.
// This ensures tokens accumulate even for teams that are idle.
func (l *TeamLimiter) refillLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		l.buckets.Range(func(_, v any) bool {
			l.refill(v.(*teamBucket))
			return true
		})
	}
}
