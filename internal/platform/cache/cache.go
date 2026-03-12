package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type QueryCache struct {
	client *redis.Client
}

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

func (c *QueryCache) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	if c.client == nil {
		return
	}
	_ = c.client.Set(ctx, key, value, ttl).Err()
}

func (c *QueryCache) Enabled() bool {
	return c.client != nil
}

func (c *QueryCache) Ping(ctx context.Context) error {
	if c.client == nil {
		return nil
	}
	return c.client.Ping(ctx).Err()
}
