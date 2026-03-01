package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenBlacklist maintains a set of revoked JWT token hashes in Redis.
// Expired entries are automatically purged by Redis TTL.
type TokenBlacklist struct {
	client *redis.Client
}

// NewTokenBlacklist creates a new Redis-backed Token Blacklist.
func NewTokenBlacklist(redisHost, redisPort string) *TokenBlacklist {
	addr := fmt.Sprintf("%s:%s", redisHost, redisPort)
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set by default
		DB:       0,  // use default DB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("WARN: Redis not connected for TokenBlacklist at %s: %v", addr, err)
	} else {
		log.Printf("Redis connected for TokenBlacklist at %s", addr)
	}

	return &TokenBlacklist{
		client: client,
	}
}

// Revoke adds a token to the blacklist. The token is stored as a SHA-256 hash
// for memory efficiency.
func (b *TokenBlacklist) Revoke(tokenString string, expiresAt time.Time) {
	hash := hashToken(tokenString)
	ctx := context.Background()

	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return // Already expired
	}

	err := b.client.Set(ctx, "blacklist:"+hash, "revoked", ttl).Err()
	if err != nil {
		log.Printf("ERROR: Failed to blacklist token: %v", err)
	} else {
		log.Printf("token blacklisted (hash=%s…, expires=%s)", hash[:8], expiresAt.Format(time.RFC3339))
	}
}

// IsRevoked checks whether a token has been revoked.
func (b *TokenBlacklist) IsRevoked(tokenString string) bool {
	hash := hashToken(tokenString)
	ctx := context.Background()

	err := b.client.Get(ctx, "blacklist:"+hash).Err()
	if err == redis.Nil {
		return false // Not found = not revoked
	} else if err != nil {
		log.Printf("ERROR: Redis get error on IsRevoked: %v", err)
		return false // Fail open for safety or default to strict? usually fail open for cache
	}

	return true
}

// Stop is a no-op for the Redis implementation, provided for interface compatibility.
func (b *TokenBlacklist) Stop() {
	if err := b.client.Close(); err != nil {
		log.Printf("ERROR closing redis client: %v", err)
	}
}

func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
