package tracker

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ByteTracker aggregates ingested byte counts per team using in-memory
// atomic counters and periodically flushes the totals into MySQL.
type ByteTracker struct {
	db            *sql.DB
	flushInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// counts maps teamID (int64) to per-team byte counter (*int64).
	// We use a sync.Map for thread-safe access to per-team counters.
	counts sync.Map
}

// NewByteTracker creates a tracker backed by in-memory atomic counters.
func NewByteTracker(db *sql.DB, flushInterval time.Duration) *ByteTracker {
	bt := &ByteTracker{
		db:            db,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}
	bt.wg.Add(1)
	go bt.flushLoop()
	return bt
}

// Stop flushes remaining counts and shuts down the background loop.
func (b *ByteTracker) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}

// Track increments the ingested byte count for a team using in-memory atomic counters.
func (b *ByteTracker) Track(teamID int64, bytes int64) {
	if bytes <= 0 || teamID <= 0 {
		return
	}

	actual, _ := b.counts.LoadOrStore(teamID, new(int64))
	atomic.AddInt64(actual.(*int64), bytes)
}

// flushLoop periodically aggregates the in-memory totals and flushes to MySQL.
func (b *ByteTracker) flushLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()
		case <-b.stopCh:
			b.flush() // final flush on shutdown
			return
		}
	}
}

// flush snapshots the current in-memory totals, resets them, and batch-updates MySQL.
func (b *ByteTracker) flush() {
	// Collect and reset counters
	snapshot := make(map[int64]int64)
	b.counts.Range(func(key, value any) bool {
		teamID := key.(int64)
		counter := value.(*int64)

		// Atomically swap the counter with 0 to get the delta for this interval.
		delta := atomic.SwapInt64(counter, 0)
		if delta > 0 {
			snapshot[teamID] = delta
		}
		return true
	})

	if len(snapshot) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b.updateMySQL(ctx, snapshot)
}

// updateMySQL batches all team byte counts into a single UPDATE statement.
func (b *ByteTracker) updateMySQL(ctx context.Context, counts map[int64]int64) {
	teamIDs := make([]int64, 0, len(counts))
	for id := range counts {
		teamIDs = append(teamIDs, id)
	}

	var sb strings.Builder
	args := make([]any, 0, len(teamIDs)*3)
	sb.WriteString("UPDATE teams SET data_ingested_kb = data_ingested_kb + CASE id ")
	for _, id := range teamIDs {
		// Convert to KB, minimum 1 to ensure some visibility for small pulses.
		kb := counts[id] / 1024
		if kb == 0 {
			kb = 1
		}
		sb.WriteString("WHEN ? THEN ? ")
		args = append(args, id, kb)
	}
	sb.WriteString("ELSE 0 END WHERE id IN (")
	for i, id := range teamIDs {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("?")
		args = append(args, id)
	}
	sb.WriteString(")")

	if _, err := b.db.ExecContext(ctx, sb.String(), args...); err != nil {
		slog.Error("ingest/bytetracker: MySQL batch update failed",
			slog.Int("teams", len(teamIDs)), slog.Any("error", err))
	} else {
		slog.Debug("ingest/bytetracker: flushed to MySQL", slog.Int("teams", len(teamIDs)))
	}
}
