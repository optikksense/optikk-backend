package livetail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/livetailredis"
	"github.com/redis/go-redis/v9"
)

// LiveTailEmit sends a named event with JSON-serializable data.
type LiveTailEmit func(event string, data any)

const (
	spansXReadBlock = 2 * time.Second
	spansXReadCount = 100
)

// RunSpansLiveTail polls ClickHouse for new spans until done is closed (legacy).
func RunSpansLiveTail(service *Service, payload json.RawMessage, emit LiveTailEmit, done <-chan struct{}) {
	var p SubscribeSpansPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		slog.Warn("live tail [subscribe:spans] bad payload", slog.Any("error", err))
		emit("subscribeError", socketErrorPayload{Message: "invalid payload"})
		return
	}

	if p.TeamID == 0 {
		emit("subscribeError", socketErrorPayload{Message: "teamId is required"})
		return
	}

	filters := LiveTailFilters{
		Services:   p.Services,
		Status:     p.Status,
		SpanKind:   p.SpanKind,
		SearchText: p.SearchText,
		Operation:  p.Operation,
		HTTPMethod: p.HTTPMethod,
	}

	since := time.Now().Add(-sioPollInterval)
	deadline := time.Now().Add(sioMaxSessionTime)
	ticker := time.NewTicker(sioPollInterval)
	heartbeat := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer heartbeat.Stop()
	lastLagMs := int64(0)
	lastDroppedCount := int64(0)

	for {
		select {
		case <-done:
			return
		case <-heartbeat.C:
			emit("heartbeat", socketHeartbeatPayload{
				LagMs:        lastLagMs,
				DroppedCount: lastDroppedCount,
			})
		case <-ticker.C:
			if time.Now().After(deadline) {
				emit("done", socketDonePayload{Reason: "session_timeout"})
				return
			}

			result, err := service.Poll(p.TeamID, since, filters)
			if err != nil {
				slog.Warn("live tail [subscribe:spans] poll error", slog.Any("error", err))
				continue
			}

			lastDroppedCount = result.DroppedCount
			for _, s := range result.Spans {
				since = s.Timestamp
				lagMs := time.Since(s.Timestamp).Milliseconds()
				if lagMs < 0 {
					lagMs = 0
				}
				emit("span", socketSpanEventPayload{
					Item:         s,
					LagMs:        lagMs,
					DroppedCount: lastDroppedCount,
				})
				lastLagMs = lagMs
			}
		}
	}
}

// RunSpansLiveTailRedis reads span tail fan-out from Redis Streams (no ClickHouse polling).
func RunSpansLiveTailRedis(rdb *redis.Client, payload json.RawMessage, emit LiveTailEmit, done <-chan struct{}) {
	var p SubscribeSpansPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		slog.Warn("live tail [subscribe:spans] bad payload", slog.Any("error", err))
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

	filters := LiveTailFilters{
		Services:   p.Services,
		Status:     p.Status,
		SpanKind:   p.SpanKind,
		SearchText: p.SearchText,
		Operation:  p.Operation,
		HTTPMethod: p.HTTPMethod,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-done
		cancel()
	}()

	var mu sync.Mutex
	var lastLagMs int64

	go func() {
		hb := time.NewTicker(15 * time.Second)
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

	emitOne := func(s LiveSpan, lagMs int64) {
		mu.Lock()
		lastLagMs = lagMs
		mu.Unlock()
		emit("span", socketSpanEventPayload{
			Item:         s,
			LagMs:        lagMs,
			DroppedCount: 0,
		})
	}

	err := runSpansRedisStreamLoop(ctx, rdb, p.TeamID, filters, emitOne)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("live tail [subscribe:spans] redis stream read ended", slog.Any("error", err))
	}
}

func runSpansRedisStreamLoop(ctx context.Context, rdb *redis.Client, teamID int64, filters LiveTailFilters, emit func(LiveSpan, int64)) error {
	streamKey := livetailredis.SpansStreamKey(teamID)

	msgs, err := rdb.XRevRangeN(ctx, streamKey, "+", "-", livetailredis.SnapshotCount).Result()
	if err != nil {
		slog.Warn("live tail [subscribe:spans] xrevrange snapshot", slog.Int64("teamId", teamID), slog.Any("error", err))
	}

	var lastID string
	if len(msgs) > 0 {
		for _, m := range msgs {
			raw, ok := spansStreamPayloadString(m)
			if !ok {
				continue
			}
			var msg redisSpanTailMsg
			if err := json.Unmarshal([]byte(raw), &msg); err != nil {
				continue
			}
			if !filters.MatchesSpan(msg.LiveSpan) {
				continue
			}
			emit(msg.LiveSpan, lagMsSpanTail(msg.EmitMs, msg.Timestamp))
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
			Count:   spansXReadCount,
			Block:   spansXReadBlock,
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
			raw, ok := spansStreamPayloadString(m)
			if !ok {
				continue
			}
			var live redisSpanTailMsg
			if err := json.Unmarshal([]byte(raw), &live); err != nil {
				continue
			}
			if !filters.MatchesSpan(live.LiveSpan) {
				continue
			}
			emit(live.LiveSpan, lagMsSpanTail(live.EmitMs, live.Timestamp))
		}
	}
}

func spansStreamPayloadString(m redis.XMessage) (string, bool) {
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

func lagMsSpanTail(emitMs int64, ts time.Time) int64 {
	now := time.Now().UnixMilli()
	if emitMs > 0 {
		lag := now - emitMs
		if lag < 0 {
			return 0
		}
		return lag
	}
	lag := time.Since(ts).Milliseconds()
	if lag < 0 {
		return 0
	}
	return lag
}
