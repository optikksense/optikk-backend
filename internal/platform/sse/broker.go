package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// Event is the payload sent over an SSE connection.
type Event struct {
	// Event type name (appears as the "event:" field in SSE).
	Type string `json:"type"`
	// Data is the JSON-encoded payload.
	Data json.RawMessage `json:"data"`
}

// Broker manages per-team SSE client connections.
// It is safe for concurrent use.
type Broker struct {
	mu sync.RWMutex
	// subscribers maps teamID -> set of channels.
	subscribers map[int64]map[chan Event]struct{}
}

// NewBroker creates a ready-to-use SSE broker.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[int64]map[chan Event]struct{}),
	}
}

// Subscribe registers a new SSE client for the given team.
// It returns a channel that will receive events and an unsubscribe function
// that MUST be called when the client disconnects.
func (b *Broker) Subscribe(teamID int64) (ch chan Event, unsubscribe func()) {
	ch = make(chan Event, 64) // buffered to avoid blocking publishers

	b.mu.Lock()
	if b.subscribers[teamID] == nil {
		b.subscribers[teamID] = make(map[chan Event]struct{})
	}
	b.subscribers[teamID][ch] = struct{}{}
	b.mu.Unlock()

	unsubscribe = func() {
		b.mu.Lock()
		delete(b.subscribers[teamID], ch)
		if len(b.subscribers[teamID]) == 0 {
			delete(b.subscribers, teamID)
		}
		b.mu.Unlock()
		// Drain any pending events so senders don't block.
		for range ch {
		}
	}

	return ch, unsubscribe
}

// Publish sends an event to all SSE subscribers for the given team.
// It is non-blocking: if a subscriber's channel is full the event is dropped
// for that subscriber (slow consumer protection).
func (b *Broker) Publish(teamID int64, eventType string, data any) {
	payload, err := json.Marshal(data)
	if err != nil {
		log.Printf("sse: failed to marshal event data: %v", err)
		return
	}

	evt := Event{
		Type: eventType,
		Data: payload,
	}

	b.mu.RLock()
	subs := b.subscribers[teamID]
	b.mu.RUnlock()

	for ch := range subs {
		select {
		case ch <- evt:
		default:
			// Channel full — drop event for this slow subscriber.
		}
	}
}

// format renders an Event as an SSE-formatted byte slice:
//
//	event: <type>\ndata: <json>\n\n
func (e Event) Format() []byte {
	return []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", e.Type, string(e.Data)))
}
