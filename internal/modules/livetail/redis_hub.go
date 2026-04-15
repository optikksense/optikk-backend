package livetail

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"
)

// redisStreamKeyFmt is the Redis Stream key pattern per team.
const redisStreamKeyFmt = "optikk:livetail:%d"

// RedisHub implements Hub using Redis Streams. Each team has a dedicated
// stream; Publish() does XADD, Subscribe() spawns a goroutine that does
// blocking XREAD until the provided channel is unsubscribed.
//
// This enables cross-pod live tail fan-out: any API pod can publish to the
// stream and any client connected to any pod will receive events.
type RedisHub struct {
	client    *goredis.Client
	maxLen    int64 // MAXLEN ~ for stream trimming
	connCount atomic.Int64
}

// NewRedisHub creates a RedisHub writing to "optikk:livetail:{teamID}" streams.
// maxLen controls approximate stream length per team (XADD MAXLEN ~).
func NewRedisHub(client *goredis.Client, maxLen int64) *RedisHub {
	if maxLen <= 0 {
		maxLen = 10_000
	}
	return &RedisHub{client: client, maxLen: maxLen}
}

func (h *RedisHub) streamKey(teamID int64) string {
	return fmt.Sprintf(redisStreamKeyFmt, teamID)
}

// Publish serialises event as JSON and appends it to the team's Redis Stream.
// Non-blocking — drops silently if Redis is unavailable.
func (h *RedisHub) Publish(teamID int64, event any) {
	payload, err := json.Marshal(event)
	if err != nil {
		slog.Debug("livetail: marshal failed", slog.Any("error", err))
		return
	}
	ctx := context.Background()
	if err := h.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: h.streamKey(teamID),
		MaxLen: h.maxLen,
		Approx: true,
		Values: map[string]any{"data": string(payload)},
	}).Err(); err != nil {
		slog.Debug("livetail: xadd failed", slog.Any("error", err))
	}
}

// Subscribe starts a goroutine that reads new events from the team's Redis
// Stream and sends them to ch. A nil filter accepts all events.
// Returns false if the global connection limit (100) is exceeded.
func (h *RedisHub) Subscribe(teamID int64, ch chan any, filter FilterFunc) bool {
	if h.connCount.Load() >= int64(maxGlobalConnections) {
		return false
	}
	h.connCount.Add(1)

	go func() {
		defer func() {
			h.connCount.Add(-1)
		}()

		key := h.streamKey(teamID)
		lastID := "$" // only new events from subscription time

		for {
			// XREAD BLOCK 2000ms — blocks until a new message or timeout.
			results, err := h.client.XRead(context.Background(), &goredis.XReadArgs{
				Streams: []string{key, lastID},
				Count:   100,
				Block:   2000, // ms
			}).Result()
			if err != nil {
				if err == goredis.Nil {
					// Timeout with no new messages — check if ch is still alive.
					select {
					case _, ok := <-ch:
						if !ok {
							return // channel closed — unsubscribe
						}
					default:
					}
					continue
				}
				// Redis error — log and bail.
				slog.Debug("livetail: xread error", slog.Any("error", err))
				return
			}

			for _, stream := range results {
				for _, msg := range stream.Messages {
					lastID = msg.ID
					raw, ok := msg.Values["data"].(string)
					if !ok {
						continue
					}
					var event any
					if err := json.Unmarshal([]byte(raw), &event); err != nil {
						continue
					}
					if filter != nil && !filter(event) {
						continue
					}
					select {
					case ch <- event:
					default:
						// Slow consumer — skip to prevent blocking the reader loop.
					}
				}
			}
		}
	}()
	return true
}

// Unsubscribe closes the channel to signal the reader goroutine to stop.
func (h *RedisHub) Unsubscribe(teamID int64, ch chan any) {
	// Closing ch signals the goroutine spawned in Subscribe to return.
	// The goroutine detects this via the channel-closed check in the loop.
	close(ch)
}

// Ensure RedisHub implements the Hub interface at compile time.
var _ Hub = (*RedisHub)(nil)
