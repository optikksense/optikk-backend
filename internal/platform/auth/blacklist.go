package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"sync"
	"time"
)

// TokenBlacklist maintains an in-memory set of revoked JWT token hashes.
// Expired entries are automatically purged by a background goroutine.
type TokenBlacklist struct {
	mu      sync.RWMutex
	entries map[string]time.Time // SHA-256 hash -> expiry time
	stop    chan struct{}
}

// NewTokenBlacklist creates a new blacklist and starts a background cleanup
// goroutine that removes expired entries every minute.
func NewTokenBlacklist() *TokenBlacklist {
	b := &TokenBlacklist{
		entries: make(map[string]time.Time),
		stop:    make(chan struct{}),
	}
	go b.cleanupLoop()
	return b
}

// Revoke adds a token to the blacklist. The token is stored as a SHA-256 hash
// for memory efficiency. expiresAt should match the token's expiry time so the
// entry can be cleaned up once the token would have expired naturally.
func (b *TokenBlacklist) Revoke(tokenString string, expiresAt time.Time) {
	hash := hashToken(tokenString)
	b.mu.Lock()
	b.entries[hash] = expiresAt
	b.mu.Unlock()
	log.Printf("token blacklisted (hash=%s…, expires=%s)", hash[:8], expiresAt.Format(time.RFC3339))
}

// IsRevoked checks whether a token has been revoked.
func (b *TokenBlacklist) IsRevoked(tokenString string) bool {
	hash := hashToken(tokenString)
	b.mu.RLock()
	_, revoked := b.entries[hash]
	b.mu.RUnlock()
	return revoked
}

// Stop terminates the background cleanup goroutine. Call this during
// application shutdown.
func (b *TokenBlacklist) Stop() {
	close(b.stop)
}

// cleanupLoop runs every minute and removes entries whose expiry time has
// passed — those tokens would be rejected by the JWT parser anyway.
func (b *TokenBlacklist) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.purgeExpired()
		case <-b.stop:
			return
		}
	}
}

func (b *TokenBlacklist) purgeExpired() {
	now := time.Now()
	b.mu.Lock()
	removed := 0
	for hash, exp := range b.entries {
		if now.After(exp) {
			delete(b.entries, hash)
			removed++
		}
	}
	b.mu.Unlock()
	if removed > 0 {
		log.Printf("token blacklist: purged %d expired entries", removed)
	}
}

func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
