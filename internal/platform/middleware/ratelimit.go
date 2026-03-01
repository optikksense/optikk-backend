package middleware

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/redis/go-redis/v9"
)

// ── In-process rate limiter (local dev / Redis fallback) ─────────────────────

// RateLimiter implements a per-key token bucket rate limiter backed by an
// in-process map. Safe for single-pod use only; under multi-pod deployment use
// RedisRateLimiter so all pods share the same counters.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    int           // tokens added per interval
	burst   int           // maximum tokens (burst capacity)
	window  time.Duration // refill interval
}

type bucket struct {
	tokens   int
	lastFill time.Time
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
	refill := int(elapsed/rl.window) * rl.rate
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

// ── Redis-backed rate limiter (production, multi-pod safe) ───────────────────

// RedisRateLimiter implements a per-key fixed-window rate limiter using Redis
// INCR + EXPIRE. All pods share the same counters so the configured limit is
// enforced globally, not per-replica.
type RedisRateLimiter struct {
	client   *redis.Client
	limit    int           // max requests per window
	window   time.Duration // window duration
	fallback *RateLimiter  // used if Redis is unavailable
}

// NewRedisRateLimiter creates a distributed rate limiter using an existing
// Redis client connection. Falls back to the provided in-process limiter when
// Redis commands fail (fail-open strategy to preserve availability).
func NewRedisRateLimiter(client *redis.Client, limit int, window time.Duration) *RedisRateLimiter {
	return &RedisRateLimiter{
		client:   client,
		limit:    limit,
		window:   window,
		fallback: NewRateLimiter(limit, limit*2, window),
	}
}

// allow returns true if the key is within the rate limit for the current window.
func (r *RedisRateLimiter) allow(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	redisKey := fmt.Sprintf("rl:%s", key)
	count, err := r.client.Incr(ctx, redisKey).Result()
	if err != nil {
		// Redis unavailable — fall back to in-process limiter.
		return r.fallback.allow(key)
	}

	// Set TTL only on the first request in the window.
	if count == 1 {
		r.client.Expire(ctx, redisKey, r.window)
	}

	return count <= int64(r.limit)
}

// ── Gin middleware ────────────────────────────────────────────────────────────

// rateLimitKey extracts the rate-limit key from the request.
// Priority: API key header → JWT (identifies tenant) → client IP.
func rateLimitKey(c *gin.Context) string {
	if key := c.GetHeader("X-API-Key"); key != "" {
		return "apikey:" + key
	}
	if auth := c.GetHeader("Authorization"); len(auth) > 7 {
		// Rate-limit by the raw token so each tenant has their own bucket.
		return "jwt:" + auth[7:min(len(auth), 40)]
	}
	return "ip:" + c.ClientIP()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RateLimitMiddleware creates Gin middleware that rate-limits using an in-process
// limiter (single-pod / dev). For multi-pod deployments use RedisRateLimitMiddleware.
func RateLimitMiddleware(rl *RateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !rl.allow(rateLimitKey(c)) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, types.Failure(
				"RATE_LIMITED", "Too many requests, please try again later", c.Request.URL.Path,
			))
			return
		}
		c.Next()
	}
}

// RedisRateLimitMiddleware creates Gin middleware using the distributed Redis
// rate limiter. This is the recommended middleware for multi-pod Kubernetes
// deployments — all replicas share the same counters.
func RedisRateLimitMiddleware(rl *RedisRateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !rl.allow(rateLimitKey(c)) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, types.Failure(
				"RATE_LIMITED", "Too many requests, please try again later", c.Request.URL.Path,
			))
			return
		}
		c.Next()
	}
}
