package ingest

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"
)

// ByteTracker aggregates ingested byte counts per team in memory and flushes them
// to the MySQL database on a regular interval (e.g. hourly) to reduce DB load.
type ByteTracker struct {
	db             *sql.DB
	flushInterval  time.Duration
	mu             sync.Mutex
	ingestedCounts map[int64]int64
	doneCh         chan struct{}
}

// NewByteTracker makes a new data volume aggregator.
func NewByteTracker(db *sql.DB, flushInterval time.Duration) *ByteTracker {
	return &ByteTracker{
		db:             db,
		flushInterval:  flushInterval,
		ingestedCounts: make(map[int64]int64),
		doneCh:         make(chan struct{}),
	}
}

// Track atomically increments the ingested byte count for a given team.
func (b *ByteTracker) Track(teamID int64, bytes int64) {
	if bytes <= 0 || teamID <= 0 {
		return
	}

	b.mu.Lock()
	b.ingestedCounts[teamID] += bytes
	b.mu.Unlock()
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
	b.mu.Lock()
	if len(b.ingestedCounts) == 0 {
		b.mu.Unlock()
		return
	}

	// Copy and clear the map
	counts := make(map[int64]int64, len(b.ingestedCounts))
	for k, v := range b.ingestedCounts {
		counts[k] = v
	}
	// Clear the map for the next interval
	b.ingestedCounts = make(map[int64]int64)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Batch all team updates into a single SQL statement using CASE/WHEN.
	// This reduces N round-trips to 1 for N teams.
	teamIDs := make([]int64, 0, len(counts))
	for id := range counts {
		teamIDs = append(teamIDs, id)
	}

	// Build: UPDATE teams SET data_ingested_kb = data_ingested_kb + CASE id WHEN ? THEN ? ... END WHERE id IN (?,...)
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

		// Re-add all failed counts back to the tracker
		b.mu.Lock()
		for id, bytes := range counts {
			b.ingestedCounts[id] += bytes
		}
		b.mu.Unlock()
	} else if len(teamIDs) > 0 {
		log.Printf("ingest/bytetracker: flushed %d teams in single batch", len(teamIDs))
	}

}
