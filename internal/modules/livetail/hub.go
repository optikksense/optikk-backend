package livetail

import (
	"container/ring"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
)

const maxGlobalConnections = 100

// Hub manages active WebSocket subscriptions and maintains a ring buffer
// of recent telemetry events for each team to display on initial load.
type LocalHub struct {
	mu          sync.RWMutex
	subscribers map[int64]map[chan any]FilterFunc
	snapshots   map[int64]*ring.Ring
	connCount   atomic.Int64
}

type eventWrapper struct {
	ts      time.Time
	payload any
}

func NewHub() *LocalHub {
	return &LocalHub{
		subscribers: make(map[int64]map[chan any]FilterFunc),
		snapshots:   make(map[int64]*ring.Ring),
	}
}

const snapshotSize = 20
const snapshotTTL = 5 * time.Second

// Subscribe adds a new client channel to the team's broadcast list with an optional filter.
// Returns false if the global connection limit has been reached.
func (h *LocalHub) Subscribe(teamID int64, ch chan any, filter FilterFunc) bool {
	if h.connCount.Load() >= int64(maxGlobalConnections) {
		return false
	}
	h.mu.Lock()
	if _, ok := h.subscribers[teamID]; !ok {
		h.subscribers[teamID] = make(map[chan any]FilterFunc)
	}
	h.subscribers[teamID][ch] = filter
	h.mu.Unlock()
	h.connCount.Add(1)
	metrics.WSConnectionsActive.Set(float64(h.connCount.Load()))

	// Push snapshot (last 20 events) that are younger than 5 seconds
	go func() {
		h.mu.RLock()
		defer h.mu.RUnlock()
		snap, ok := h.snapshots[teamID]
		if !ok {
			return
		}
		now := time.Now()
		snap.Do(func(p any) {
			if p != nil {
				w, ok := p.(*eventWrapper)
				if !ok {
					return
				}
				// 5-second TTL check
				if now.Sub(w.ts) > snapshotTTL {
					return
				}
				select {
				case ch <- w.payload:
				default:
				}
			}
		})
	}()
	return true
}

func (h *LocalHub) Unsubscribe(teamID int64, ch chan any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connCount.Add(-1)
	metrics.WSConnectionsActive.Set(float64(h.connCount.Load()))

	if subs, ok := h.subscribers[teamID]; ok {
		delete(subs, ch)
		if len(subs) == 0 {
			delete(h.subscribers, teamID)
		}
	}
}

func (h *LocalHub) Publish(teamID int64, event any) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Wrap event with arrival time for TTL check
	w := &eventWrapper{
		ts:      time.Now(),
		payload: event,
	}

	// Update snapshot (max 20 rows)
	if _, ok := h.snapshots[teamID]; !ok {
		h.snapshots[teamID] = ring.New(snapshotSize)
	}
	h.snapshots[teamID].Value = w
	h.snapshots[teamID] = h.snapshots[teamID].Next()

	// Broadcast to subscribers
	if subs, ok := h.subscribers[teamID]; ok {
		for ch, filter := range subs {
			if filter == nil || filter(event) {
				select {
				case ch <- event:
				default:
					// Slow consumer, skip to prevent blocking
				}
			}
		}
	}
}
