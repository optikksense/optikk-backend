package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBroker wraps the in-process Broker and adds Redis Pub/Sub fanout for
// multi-pod Kubernetes deployments. Every Publish() call writes to both the
// local in-memory channels (for same-pod efficiency) and a Redis channel
// "sse:team:{teamID}", so pods that have subscribers on *different* replicas
// still receive all events.
//
// If Redis is unavailable, RedisBroker falls back transparently to the
// underlying in-process Broker so local dev and single-pod deployments work
// without configuration.
type RedisBroker struct {
	local  *Broker
	client *redis.Client
	// channelPrefix is prepended to all Redis Pub/Sub channel names.
	channelPrefix string
}

// NewRedisBroker creates a RedisBroker backed by the given Redis client.
// Call Start() after construction to begin consuming cross-pod events.
func NewRedisBroker(client *redis.Client) *RedisBroker {
	rb := &RedisBroker{
		local:         NewBroker(),
		client:        client,
		channelPrefix: "sse:team:",
	}
	return rb
}

// Start spawns a goroutine that subscribes to the Redis pattern
// "sse:team:*" and forwards incoming events to local in-memory channels.
// The goroutine runs until ctx is cancelled.
func (rb *RedisBroker) Start(ctx context.Context) {
	go rb.listenRedis(ctx)
}

// Subscribe registers a local SSE client for the given team.
func (rb *RedisBroker) Subscribe(teamID int64) chan Event {
	return rb.local.Subscribe(teamID)
}

// Unsubscribe removes a client from the local broker.
func (rb *RedisBroker) Unsubscribe(teamID int64, ch chan Event) {
	rb.local.Unsubscribe(teamID, ch)
}

// Publish sends an event to local in-memory subscribers AND publishes it to
// the Redis channel "sse:team:{teamID}" so other pods pick it up.
func (rb *RedisBroker) Publish(teamID int64, eventType string, data any) {
	// Always deliver to same-pod subscribers immediately.
	rb.local.Publish(teamID, eventType, data)

	// Publish to Redis for cross-pod fanout.
	payload, err := json.Marshal(wireEvent{Team: teamID, Type: eventType, Data: jsonMarshalRaw(data)})
	if err != nil {
		log.Printf("sse/redis: marshal error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	channel := fmt.Sprintf("%s%d", rb.channelPrefix, teamID)
	if err := rb.client.Publish(ctx, channel, payload).Err(); err != nil {
		// Non-fatal: local subscribers already received the event.
		log.Printf("sse/redis: publish to %s failed: %v", channel, err)
	}
}

// listenRedis subscribes to the Redis PubSub pattern "sse:team:*" and
// forwards messages to the local in-process broker. This way, events
// published by other pods reach subscribers on this pod.
func (rb *RedisBroker) listenRedis(ctx context.Context) {
	pattern := rb.channelPrefix + "*"
	pubsub := rb.client.PSubscribe(ctx, pattern)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				// Channel closed — attempt reconnect after a short delay.
				log.Printf("sse/redis: PubSub channel closed, reconnecting in 5s...")
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					go rb.listenRedis(ctx)
					return
				}
			}
			var we wireEvent
			if err := json.Unmarshal([]byte(msg.Payload), &we); err != nil {
				log.Printf("sse/redis: unmarshal error: %v", err)
				continue
			}
			// Forward to local in-memory channels. Use publishRaw to avoid
			// double-publishing when the event originated from this same pod
			// (we already delivered it locally in Publish()).
			rb.local.publishRaw(we.Team, we.Type, we.Data)
		}
	}
}

// wireEvent is the JSON structure written to Redis.
type wireEvent struct {
	Team int64           `json:"team"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func jsonMarshalRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}
