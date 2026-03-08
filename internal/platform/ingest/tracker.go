package ingest

import (
	"context"
	"database/sql"
	"log"
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

	// Update the database.
	// We run multiple queries, one per team. They can be inside a transaction if desired,
	// but individual updates are sufficient.
	for teamID, bytes := range counts {
		kb := bytes / 1024
		if kb == 0 {
			kb = 1 // count at least 1 KB for any non-zero payload
		}
		_, err := b.db.ExecContext(ctx, "UPDATE teams SET data_ingested_kb = data_ingested_kb + ? WHERE id = ?", kb, teamID)
		if err != nil {
			log.Printf("ingest/bytetracker: failed to update bytes for team %d: %v", teamID, err)
			
			// Re-add failed counts back to the tracker (in original bytes so rounding is consistent on retry)
			b.mu.Lock()
			b.ingestedCounts[teamID] += bytes
			b.mu.Unlock()
		}
	}
}
