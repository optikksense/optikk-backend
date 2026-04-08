package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/livetailredis"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	"github.com/redis/go-redis/v9"
)

// LiveTailEmit sends a named event with JSON-serializable data.
type LiveTailEmit func(event string, data any)

const xreadBlock = 2 * time.Second
const xreadCount = 100

// RunLogsLiveTail subscribes to ingest fan-out in Redis Streams until done is closed (no ClickHouse polling).
func RunLogsLiveTail(rdb *redis.Client, payload json.RawMessage, emit LiveTailEmit, done <-chan struct{}) {
	var p SubscribeLogsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		slog.Warn("live tail [subscribe:logs] bad payload", slog.Any("error", err))
		emit("subscribeError", socketErrorPayload{Message: "invalid payload"})
		return
	}

	if p.TeamID == 0 {
		emit("subscribeError", socketErrorPayload{Message: "teamId is required"})
		return
	}

	if rdb == nil {
		emit("subscribeError", socketErrorPayload{Message: "live tail requires Redis"})
		return
	}

	filters := shared.LogFilters{
		TeamID:            p.TeamID,
		StartMs:           p.StartMs,
		EndMs:             p.EndMs,
		Severities:        p.Severities,
		Services:          p.Services,
		Hosts:             p.Hosts,
		Pods:              p.Pods,
		Containers:        p.Containers,
		Environments:      p.Environments,
		TraceID:           p.TraceID,
		SpanID:            p.SpanID,
		Search:            p.Search,
		SearchMode:        p.SearchMode,
		ExcludeSeverities: p.ExcludeSeverities,
		ExcludeServices:   p.ExcludeServices,
		ExcludeHosts:      p.ExcludeHosts,
		AttributeFilters:  p.AttributeFilters,
	}
	// The UI sends startMs/endMs from the explorer time picker. For live tail, that would drop
	// anything arriving "after" a pinned or stale end bound. Ignore wall-clock window here;
	// MatchesLog + normalizeLogFilterTimeRange(0,0) becomes [now-30d, now] per check.
	filters.StartMs = 0
	filters.EndMs = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-done
		cancel()
	}()

	var mu sync.Mutex
	var lastLagMs int64

	go func() {
		hb := time.NewTicker(sioHeartbeatInterval)
		defer hb.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-hb.C:
				mu.Lock()
				lag := lastLagMs
				mu.Unlock()
				emit("heartbeat", socketHeartbeatPayload{
					LagMs:        lag,
					DroppedCount: 0,
				})
			}
		}
	}()

	emitOne := func(log shared.Log, lagMs int64) {
		mu.Lock()
		lastLagMs = lagMs
		mu.Unlock()
		emit("log", socketLogEventPayload{
			Item:         log,
			LagMs:        lagMs,
			DroppedCount: 0,
		})
	}

	err := runLogsRedisStreamLoop(ctx, rdb, p.TeamID, filters, emitOne)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("live tail [subscribe:logs] redis stream read ended", slog.Any("error", err))
	}
}

func runLogsRedisStreamLoop(ctx context.Context, rdb *redis.Client, teamID int64, filters shared.LogFilters, emit func(shared.Log, int64)) error {
	streamKey := livetailredis.LogsStreamKey(teamID)

	msgs, err := rdb.XRevRangeN(ctx, streamKey, "+", "-", livetailredis.SnapshotCount).Result()
	if err != nil {
		slog.Warn("live tail [subscribe:logs] xrevrange snapshot", slog.Int64("teamId", teamID), slog.Any("error", err))
	}

	var lastID string
	if len(msgs) > 0 {
		for _, m := range msgs {
			raw, ok := streamPayloadString(m)
			if !ok {
				continue
			}
			var msg redisLiveTailMsg
			if err := json.Unmarshal([]byte(raw), &msg); err != nil {
				continue
			}
			if !filters.MatchesLog(msg.Log) {
				continue
			}
			emit(msg.Log, lagMsLiveTail(msg.EmitMs, msg.Timestamp))
		}
		lastID = msgs[0].ID
	} else {
		lastID = "$"
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		res, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Count:   xreadCount,
			Block:   xreadBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return err
			}
			return err
		}
		if len(res) == 0 || len(res[0].Messages) == 0 {
			continue
		}

		for _, m := range res[0].Messages {
			lastID = m.ID
			raw, ok := streamPayloadString(m)
			if !ok {
				continue
			}
			var live redisLiveTailMsg
			if err := json.Unmarshal([]byte(raw), &live); err != nil {
				continue
			}
			if !filters.MatchesLog(live.Log) {
				continue
			}
			emit(live.Log, lagMsLiveTail(live.EmitMs, live.Timestamp))
		}
	}
}

func streamPayloadString(m redis.XMessage) (string, bool) {
	v, ok := m.Values[livetailredis.StreamPayloadField]
	if !ok {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return fmt.Sprint(s), true
	}
}

// redisLiveTailMsg is the JSON stored in Redis (shared.Log + optional emit_ms from ingest).
type redisLiveTailMsg struct {
	shared.Log
	EmitMs int64 `json:"emit_ms"`
}

// lagMsLiveTail prefers emit_ms (time from ingest publish to now) so the UI shows pipeline delay,
// not “how old the log event is” (which includes OTLP batching and can look like tens of seconds).
func lagMsLiveTail(emitMs int64, timestampNs uint64) int64 {
	now := time.Now().UnixMilli()
	if emitMs > 0 {
		lag := now - emitMs
		if lag < 0 {
			return 0
		}
		return lag
	}
	return lagMsForTimestamp(timestampNs)
}

func lagMsForTimestamp(timestampNs uint64) int64 {
	nowMs := time.Now().UnixMilli()
	tsMs := int64(timestampNs / 1_000_000) //nolint:gosec // G115
	lag := nowMs - tsMs
	if lag < 0 {
		return 0
	}
	return lag
}
