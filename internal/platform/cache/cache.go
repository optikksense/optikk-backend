package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// QueryCache provides Redis-backed caching for expensive query results.
// When Redis is nil (disabled), all operations are no-ops — callers always
// fall through to the underlying query.
type QueryCache struct {
	client *redis.Client
}

// New creates a QueryCache. Pass nil to disable caching entirely.
func New(client *redis.Client) *QueryCache {
	return &QueryCache{client: client}
}

// Get retrieves a cached value. Returns ("", false) on miss or when disabled.
func (c *QueryCache) Get(ctx context.Context, key string) (string, bool) {
	if c.client == nil {
		return "", false
	}
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return "", false
	}
	return val, true
}

// Set stores a value with the given TTL. No-op when disabled.
func (c *QueryCache) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	if c.client == nil {
		return
	}
	_ = c.client.Set(ctx, key, value, ttl).Err()
}

// Enabled returns true if Redis caching is active.
func (c *QueryCache) Enabled() bool {
	return c.client != nil
}
