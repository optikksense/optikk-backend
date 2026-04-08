package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	limiterlib "github.com/ulule/limiter/v3"
	memorylimiter "github.com/ulule/limiter/v3/drivers/store/memory"

	types "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
)

type RateLimiter struct {
	limiter *limiterlib.Limiter
}

func NewRateLimiter(rate, burst int, window time.Duration) *RateLimiter {
	limit := rate
	if burst > limit {
		limit = burst
	}
	if limit <= 0 {
		limit = 1
	}

	store := memorylimiter.NewStore()
	limiter := limiterlib.New(store, limiterlib.Rate{
		Period: window,
		Limit:  int64(limit),
	})

	return &RateLimiter{
		limiter: limiter,
	}
}

func (rl *RateLimiter) Allow(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	limitCtx, err := rl.limiter.Get(ctx, key)
	if err != nil {
		return true
	}
	return !limitCtx.Reached
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

type keyLimiter interface {
	Allow(key string) bool
}

func RateLimitMiddleware(rl keyLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !rl.Allow(rateLimitKey(c)) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, types.Failure(
				"RATE_LIMITED", "Too many requests, please try again later", c.Request.URL.Path,
			))
			return
		}
		c.Next()
	}
}
