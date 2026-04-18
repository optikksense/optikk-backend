package server

import (
	"context"
	"sync"
	"time"
)

const healthCacheTTL = 5 * time.Second

type healthResult struct {
	ready     bool
	mysqlErr  string
	chErr     string
	redisErr  string
	expiresAt time.Time
}

type healthCache struct {
	mu      sync.Mutex
	current *healthResult
	inFlight bool
	cond    *sync.Cond
}

func newHealthCache() *healthCache {
	hc := &healthCache{}
	hc.cond = sync.NewCond(&hc.mu)
	return hc
}

// get returns a cached or freshly-computed health snapshot. Concurrent callers
// with an expired cache share a single refresh: the first takes the slot and
// others wait on the condvar rather than piling probe pings onto the backends.
func (h *healthCache) get(ctx context.Context, probe func(ctx context.Context) *healthResult) *healthResult {
	h.mu.Lock()
	if h.current != nil && time.Now().Before(h.current.expiresAt) {
		res := h.current
		h.mu.Unlock()
		return res
	}
	for h.inFlight {
		h.cond.Wait()
		if h.current != nil && time.Now().Before(h.current.expiresAt) {
			res := h.current
			h.mu.Unlock()
			return res
		}
	}
	h.inFlight = true
	h.mu.Unlock()

	res := probe(ctx)
	res.expiresAt = time.Now().Add(healthCacheTTL)

	h.mu.Lock()
	h.current = res
	h.inFlight = false
	h.cond.Broadcast()
	h.mu.Unlock()
	return res
}
