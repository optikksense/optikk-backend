package cache

import (
	"context"
	"time"

	rediscache "github.com/go-redis/cache/v9"
	"github.com/redis/go-redis/v9"
)

type QueryCache struct {
	client *redis.Client
	cache  *rediscache.Cache
}

func New(client *redis.Client) *QueryCache {
	if client == nil {
		return &QueryCache{}
	}

	return &QueryCache{
		client: client,
		cache: rediscache.New(&rediscache.Options{
			Redis:      client,
			LocalCache: rediscache.NewTinyLFU(1024, time.Minute),
		}),
	}
}

// Get retrieves a cached value. Returns ("", false) on miss or when disabled.
func (c *QueryCache) Get(ctx context.Context, key string) (string, bool) {
	if !c.Enabled() {
		return "", false
	}

	var value string
	if err := c.cache.Get(ctx, key, &value); err != nil {
		return "", false
	}
	return value, true
}

func (c *QueryCache) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	if !c.Enabled() {
		return
	}
	_ = c.cache.Set(&rediscache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
}

func (c *QueryCache) Enabled() bool {
	return c.client != nil && c.cache != nil
}

func (c *QueryCache) Ping(ctx context.Context) error {
	if c.client == nil {
		return nil
	}
	return c.client.Ping(ctx).Err()
}
