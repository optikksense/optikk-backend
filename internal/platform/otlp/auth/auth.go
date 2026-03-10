package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrMissingAPIKey = errors.New("missing API key")
	ErrInvalidAPIKey = errors.New("invalid API key")
	ErrResolveFailed = errors.New("failed to resolve team")
)

const (
	cacheTTL      = 5 * time.Minute
	cleanupPeriod = 10 * time.Minute
)

// Authenticator handles resolving an API key to a Team ID with a lock-free cache.
// Uses sync.Map for contention-free reads on the hot path (cache hit).
type Authenticator struct {
	db    *sql.DB
	cache sync.Map // map[string]*cachedTeam
	size  atomic.Int64
}

type cachedTeam struct {
	teamID    int64
	expiresAt time.Time
}

// NewAuthenticator creates a common auth resolver for both HTTP and gRPC handlers.
func NewAuthenticator(db *sql.DB) *Authenticator {
	a := &Authenticator{db: db}
	go a.cleanupLoop()
	return a
}

// ResolveTeamID returns the numeric team ID for the given API key.
// Hot path (cache hit) is lock-free via sync.Map.Load.
func (a *Authenticator) ResolveTeamID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}

	// Lock-free read path
	if val, ok := a.cache.Load(apiKey); ok {
		ct := val.(*cachedTeam)
		if time.Now().Before(ct.expiresAt) {
			return ct.teamID, nil
		}
		// Expired — fall through to DB
	}

	var teamID int64
	err := a.db.QueryRowContext(ctx,
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID)

	if err == sql.ErrNoRows {
		return 0, ErrInvalidAPIKey
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrResolveFailed, err)
	}

	// Store — sync.Map handles concurrent stores safely
	if _, loaded := a.cache.LoadOrStore(apiKey, &cachedTeam{
		teamID:    teamID,
		expiresAt: time.Now().Add(cacheTTL),
	}); !loaded {
		a.size.Add(1)
	} else {
		// Key existed (expired entry) — overwrite
		a.cache.Store(apiKey, &cachedTeam{
			teamID:    teamID,
			expiresAt: time.Now().Add(cacheTTL),
		})
	}

	return teamID, nil
}

// cleanupLoop periodically evicts expired entries to prevent unbounded growth.
func (a *Authenticator) cleanupLoop() {
	ticker := time.NewTicker(cleanupPeriod)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		a.cache.Range(func(key, val any) bool {
			ct := val.(*cachedTeam)
			if now.After(ct.expiresAt) {
				a.cache.Delete(key)
				a.size.Add(-1)
			}
			return true
		})
	}
}
