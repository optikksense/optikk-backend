package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	limiterlib "github.com/ulule/limiter/v3"
	memorylimiter "github.com/ulule/limiter/v3/drivers/store/memory"
	redislimiter "github.com/ulule/limiter/v3/drivers/store/redis"

	types "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
)

type RateLimiter struct {
	limiter *limiterlib.Limiter
}

type RedisRateLimiter struct {
	limiter  *limiterlib.Limiter
	fallback *RateLimiter
}

func NewRateLimiter(rate, burst int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		limiter: newLimiter(memorylimiter.NewStore(), rateLimit(rate, burst), window),
	}
}

func NewRedisRateLimiter(client *redis.Client, limit int, window time.Duration) *RedisRateLimiter {
	fallback := NewRateLimiter(limit, limit, window)
	if client == nil {
		return &RedisRateLimiter{fallback: fallback}
	}

	store, err := redislimiter.NewStore(client)
	if err != nil {
		return &RedisRateLimiter{fallback: fallback}
	}

	return &RedisRateLimiter{
		limiter:  newLimiter(store, int64(limit), window),
		fallback: fallback,
	}
}

func (rl *RateLimiter) allow(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	limitCtx, err := rl.limiter.Get(ctx, key)
	if err != nil {
		return true
	}
	return !limitCtx.Reached
}

func (r *RedisRateLimiter) allow(key string) bool {
	if r.limiter == nil {
		return r.fallback.allow(key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	limitCtx, err := r.limiter.Get(ctx, key)
	if err != nil {
		return r.fallback.allow(key)
	}
	return !limitCtx.Reached
}

func newLimiter(store limiterlib.Store, limit int64, window time.Duration) *limiterlib.Limiter {
	return limiterlib.New(store, limiterlib.Rate{
		Period: window,
		Limit:  limit,
	})
}

func rateLimit(rate, burst int) int64 {
	limit := rate
	if burst > limit {
		limit = burst
	}
	if limit <= 0 {
		limit = 1
	}
	return int64(limit)
}

// rateLimitKey extracts the rate-limit key from the request.
// Priority: API key header -> authenticated tenant -> client IP.
func rateLimitKey(c *gin.Context) string {
	if key := c.GetHeader("X-API-Key"); key != "" {
		return "apikey:" + key
	}
	if tenant := GetTenant(c); tenant.UserID > 0 {
		return fmt.Sprintf("tenant:%d:%d", tenant.UserID, tenant.TeamID)
	}
	return "ip:" + c.ClientIP()
}

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
