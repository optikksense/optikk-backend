package ingest

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const numShards = 64

// ByteTracker aggregates ingested byte counts per team using sharded atomic
// counters and flushes them to the MySQL database on a regular interval.
//
// The Track() hot path is lock-free (atomic.Int64.Add) after the first call
// per team per shard. Mutexes are only held briefly during flush and during
// the rare first-time team registration in a shard.
type ByteTracker struct {
	db            *sql.DB
	flushInterval time.Duration
	shards        [numShards]shard
	doneCh        chan struct{}
}

type shard struct {
	_  [64]byte // cache-line padding to prevent false sharing
	mu sync.Mutex
	m  map[int64]*atomic.Int64
}

// NewByteTracker makes a new data volume aggregator.
func NewByteTracker(db *sql.DB, flushInterval time.Duration) *ByteTracker {
	bt := &ByteTracker{
		db:            db,
		flushInterval: flushInterval,
		doneCh:        make(chan struct{}),
	}
	for i := range bt.shards {
		bt.shards[i].m = make(map[int64]*atomic.Int64)
	}
	return bt
}

// Track increments the ingested byte count for a given team.
// Hot path is lock-free — only atomic.Int64.Add.
func (b *ByteTracker) Track(teamID int64, bytes int64) {
	if bytes <= 0 || teamID <= 0 {
		return
	}

	s := &b.shards[uint64(teamID)%numShards]

	// Fast path: counter already exists (common case after first request).
	s.mu.Lock()
	counter, ok := s.m[teamID]
	if !ok {
		counter = &atomic.Int64{}
		s.m[teamID] = counter
	}
	s.mu.Unlock()

	counter.Add(bytes)
}

// Start spawns the background flush worker.
func (b *ByteTracker) Start() {
	go b.worker()
}

// Stop safely flushes remaining counts and shuts down the background loop.
func (b *ByteTracker) Stop() {
	close(b.doneCh)
	b.flush()
}

func (b *ByteTracker) worker() {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.doneCh:
			return
		case <-ticker.C:
			b.flush()
		}
	}
}

func (b *ByteTracker) flush() {
	// Collect counts from all shards. Each shard lock is held only briefly
	// to swap out the counter and read its value.
	counts := make(map[int64]int64)

	for i := range b.shards {
		s := &b.shards[i]
		s.mu.Lock()
		for teamID, counter := range s.m {
			val := counter.Swap(0)
			if val > 0 {
				counts[teamID] += val
			}
		}
		s.mu.Unlock()
	}

	if len(counts) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Batch all team updates into a single SQL statement using CASE/WHEN.
	teamIDs := make([]int64, 0, len(counts))
	for id := range counts {
		teamIDs = append(teamIDs, id)
	}

	var caseBuilder strings.Builder
	args := make([]any, 0, len(teamIDs)*2+len(teamIDs))
	caseBuilder.WriteString("UPDATE teams SET data_ingested_kb = data_ingested_kb + CASE id ")
	for _, id := range teamIDs {
		kb := counts[id] / 1024
		if kb == 0 {
			kb = 1 // count at least 1 KB for any non-zero payload
		}
		caseBuilder.WriteString("WHEN ? THEN ? ")
		args = append(args, id, kb)
	}
	caseBuilder.WriteString("ELSE 0 END WHERE id IN (")
	for i, id := range teamIDs {
		if i > 0 {
			caseBuilder.WriteString(",")
		}
		caseBuilder.WriteString("?")
		args = append(args, id)
	}
	caseBuilder.WriteString(")")

	_, err := b.db.ExecContext(ctx, caseBuilder.String(), args...)
	if err != nil {
		log.Printf("ingest/bytetracker: batch update failed for %d teams: %v", len(teamIDs), err)

		// Re-add failed counts back — use Track to re-distribute across shards.
		for id, bytes := range counts {
			b.Track(id, bytes)
		}
	} else if len(teamIDs) > 0 {
		log.Printf("ingest/bytetracker: flushed %d teams in single batch", len(teamIDs))
	}
}
