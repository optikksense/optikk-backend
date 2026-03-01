package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
)

// RateLimiter implements a per-key token bucket rate limiter.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    int           // tokens added per interval
	burst   int           // maximum tokens (burst capacity)
	window  time.Duration // refill interval
}

type bucket struct {
	tokens    int
	lastFill  time.Time
}

// NewRateLimiter creates a rate limiter allowing `rate` requests per `window`
// with a burst capacity of `burst`.
func NewRateLimiter(rate, burst int, window time.Duration) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
		window:  window,
	}
	// Periodically clean up stale entries.
	go rl.cleanup()
	return rl
}

func (rl *RateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst, lastFill: time.Now()}
		rl.buckets[key] = b
	}

	// Refill tokens based on elapsed time.
	elapsed := time.Since(b.lastFill)
	refill := int(elapsed / rl.window) * rl.rate
	if refill > 0 {
		b.tokens += refill
		if b.tokens > rl.burst {
			b.tokens = rl.burst
		}
		b.lastFill = time.Now()
	}

	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-10 * time.Minute)
		for key, b := range rl.buckets {
			if b.lastFill.Before(cutoff) {
				delete(rl.buckets, key)
			}
		}
		rl.mu.Unlock()
	}
}

// RateLimitMiddleware creates Gin middleware that rate-limits by the requester's
// API key or IP address. Returns 429 when the limit is exceeded.
func RateLimitMiddleware(rl *RateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Prefer API key for rate limiting (OTLP endpoints); fall back to client IP.
		key := c.GetHeader("X-API-Key")
		if key == "" {
			if auth := c.GetHeader("Authorization"); len(auth) > 7 {
				key = auth[7:]
			}
		}
		if key == "" {
			key = c.ClientIP()
		}

		if !rl.allow(key) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, types.Failure(
				"RATE_LIMITED", "Too many requests, please try again later", c.Request.URL.Path,
			))
			return
		}
		c.Next()
	}
}
