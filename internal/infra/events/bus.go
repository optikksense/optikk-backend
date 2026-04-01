// Package events provides a simple in-process event bus for cross-module
// communication. Designed for future use cases like alerting (threshold breach
// → notification → incident) without introducing direct module coupling.
//
// Usage:
//
//	bus := events.NewBus()
//	bus.Subscribe("alert.threshold_breached", func(ctx context.Context, payload any) {
//	    // handle event
//	})
//	bus.Publish(ctx, "alert.threshold_breached", thresholdEvent)
package events

import (
	"context"
	"log/slog"
	"sync"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
)

// Handler processes an event. Handlers are called synchronously in the order
// they were subscribed. If a handler needs to do slow work, it should spawn
// a goroutine internally.
type Handler func(ctx context.Context, payload any)

// Bus is a simple topic-based event bus. It is safe for concurrent use.
type Bus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

// NewBus creates a new event bus.
func NewBus() *Bus {
	return &Bus{handlers: make(map[string][]Handler)}
}

// Subscribe registers a handler for a topic. Returns an unsubscribe function.
func (b *Bus) Subscribe(topic string, handler Handler) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[topic] = append(b.handlers[topic], handler)

	// Return unsubscribe func (marks handler as nil; cleaned on next publish).
	idx := len(b.handlers[topic]) - 1
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if idx < len(b.handlers[topic]) {
			b.handlers[topic][idx] = nil
		}
	}
}

// Publish sends an event to all subscribers of the topic. Handlers are called
// synchronously. Panics in handlers are recovered and logged.
func (b *Bus) Publish(ctx context.Context, topic string, payload any) {
	b.mu.RLock()
	handlers := make([]Handler, len(b.handlers[topic]))
	copy(handlers, b.handlers[topic])
	b.mu.RUnlock()

	for _, h := range handlers {
		if h == nil {
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.L().Error("event handler panicked",
						slog.String("topic", topic),
						slog.Any("panic", r))
				}
			}()
			h(ctx, payload)
		}()
	}
}
