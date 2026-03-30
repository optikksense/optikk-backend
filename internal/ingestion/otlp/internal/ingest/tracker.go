package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// redisKeyPrefix is the prefix for all usage counter keys in Redis.
	// Key format: usage:bytes:{teamID}:{YYYYMMDDHH}
	redisKeyPrefix = "usage:bytes:"
	// redisKeyTTL is how long hourly counter keys live before auto-expiring.
	redisKeyTTL = 48 * time.Hour
)

// ByteTracker aggregates ingested byte counts per team using Redis atomic
// counters and periodically flushes the totals into MySQL.
type ByteTracker struct {
	rdb           *redis.Client
	db            *sql.DB
	flushInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewByteTracker creates a tracker backed by Redis atomic counters.
// If rdb is nil, tracking is silently disabled (no-op).
func NewByteTracker(db *sql.DB, rdb *redis.Client, flushInterval time.Duration) *ByteTracker {
	bt := &ByteTracker{
		rdb:           rdb,
		db:            db,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}
	if rdb != nil {
		bt.wg.Add(1)
		go bt.flushLoop()
	}
	return bt
}

// Stop flushes remaining counts and shuts down the background loop.
func (b *ByteTracker) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}

// Track increments the ingested byte count for a team.
// Uses Redis INCRBY — atomic, lock-free, crash-safe.
func (b *ByteTracker) Track(teamID int64, bytes int64) {
	if bytes <= 0 || teamID <= 0 || b.rdb == nil {
		return
	}
	key := redisKey(teamID)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	pipe := b.rdb.Pipeline()
	pipe.IncrBy(ctx, key, bytes)
	pipe.Expire(ctx, key, redisKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		logger.L().Warn("ingest/bytetracker: redis INCRBY failed", zap.Int64("team", teamID), zap.Error(err))
	}
}

func redisKey(teamID int64) string {
	hour := time.Now().UTC().Format("2006010215")
	return redisKeyPrefix + strconv.FormatInt(teamID, 10) + ":" + hour
}

// flushLoop periodically scans Redis for usage keys and flushes totals to MySQL.
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

// flush scans all usage:bytes:* keys, aggregates per team, atomically reads
// and deletes each key (GETDEL), then batch-updates MySQL.
func (b *ByteTracker) flush() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Scan for all usage keys.
	var keys []string
	iter := b.rdb.Scan(ctx, 0, redisKeyPrefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		logger.L().Error("ingest/bytetracker: redis SCAN failed", zap.Error(err))
		return
	}
	if len(keys) == 0 {
		return
	}

	// Atomically read + delete each key and aggregate per team.
	counts := make(map[int64]int64)
	for _, key := range keys {
		val, err := b.rdb.GetDel(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			logger.L().Warn("ingest/bytetracker: redis GETDEL failed", zap.String("key", key), zap.Error(err))
			continue
		}
		bytes, _ := strconv.ParseInt(val, 10, 64)
		if bytes <= 0 {
			continue
		}
		teamID := parseTeamIDFromKey(key)
		if teamID <= 0 {
			continue
		}
		counts[teamID] += bytes
	}

	if len(counts) == 0 {
		return
	}

	b.updateMySQL(ctx, counts)
}

// parseTeamIDFromKey extracts the team ID from a key like "usage:bytes:42:2026032812".
func parseTeamIDFromKey(key string) int64 {
	// Strip prefix "usage:bytes:"
	rest := strings.TrimPrefix(key, redisKeyPrefix)
	// rest = "42:2026032812"
	idx := strings.Index(rest, ":")
	if idx < 0 {
		return 0
	}
	id, _ := strconv.ParseInt(rest[:idx], 10, 64)
	return id
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
		logger.L().Error("ingest/bytetracker: MySQL batch update failed",
			zap.Int("teams", len(teamIDs)), zap.Error(err))
		// Re-add failed counts back to Redis so they are retried next flush.
		reCtx, reCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer reCancel()
		for _, id := range teamIDs {
			key := fmt.Sprintf("%s%d:retry", redisKeyPrefix, id)
			b.rdb.IncrBy(reCtx, key, counts[id])
			b.rdb.Expire(reCtx, key, redisKeyTTL)
		}
	} else {
		logger.L().Info("ingest/bytetracker: flushed to MySQL", zap.Int("teams", len(teamIDs)))
	}
}
