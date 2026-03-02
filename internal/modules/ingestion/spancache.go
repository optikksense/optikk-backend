package telemetry

import (
	"sync"
	"time"
)

// SpanCache is a thread-safe, in-memory cache mapping spanID to serviceName
// with a configurable TTL. It enables cross-batch parent span resolution:
// when a child span arrives in a different OTLP request than its parent,
// the cache provides the parent's service name for topology/service-map views.
type SpanCache struct {
	mu      sync.RWMutex
	entries map[string]spanCacheEntry
	ttl     time.Duration
	stopCh  chan struct{}
}

type spanCacheEntry struct {
	serviceName string
	expiresAt   time.Time
}

const (
	defaultSpanCacheTTL   = 5 * time.Minute
	spanCachePurgeInterval = 1 * time.Minute
)

// NewSpanCache creates and starts a SpanCache with the default 5-minute TTL.
// A background goroutine purges expired entries every minute.
// Call Stop() to release resources when the cache is no longer needed.
func NewSpanCache() *SpanCache {
	return NewSpanCacheWithTTL(defaultSpanCacheTTL)
}

// NewSpanCacheWithTTL creates a SpanCache with a custom TTL.
func NewSpanCacheWithTTL(ttl time.Duration) *SpanCache {
	sc := &SpanCache{
		entries: make(map[string]spanCacheEntry),
		ttl:     ttl,
		stopCh:  make(chan struct{}),
	}
	go sc.purgeLoop()
	return sc
}

// Put stores a spanID-to-serviceName mapping with the configured TTL.
func (sc *SpanCache) Put(spanID, serviceName string) {
	sc.mu.Lock()
	sc.entries[spanID] = spanCacheEntry{
		serviceName: serviceName,
		expiresAt:   time.Now().Add(sc.ttl),
	}
	sc.mu.Unlock()
}

// Get returns the service name for the given spanID if it exists and has not
// expired. The second return value indicates whether a valid entry was found.
func (sc *SpanCache) Get(spanID string) (string, bool) {
	sc.mu.RLock()
	entry, ok := sc.entries[spanID]
	sc.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return "", false
	}
	return entry.serviceName, true
}

// Stop terminates the background purge goroutine.
func (sc *SpanCache) Stop() {
	close(sc.stopCh)
}

// purgeLoop periodically removes expired entries from the cache.
func (sc *SpanCache) purgeLoop() {
	ticker := time.NewTicker(spanCachePurgeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sc.stopCh:
			return
		case <-ticker.C:
			sc.purge()
		}
	}
}

func (sc *SpanCache) purge() {
	now := time.Now()
	sc.mu.Lock()
	for id, entry := range sc.entries {
		if now.After(entry.expiresAt) {
			delete(sc.entries, id)
		}
	}
	sc.mu.Unlock()
}
