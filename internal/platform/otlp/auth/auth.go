package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/observability/observability-backend-go/internal/helpers"
)

var (
	ErrMissingAPIKey = errors.New("missing API key")
	ErrInvalidAPIKey = errors.New("invalid API key")
	ErrResolveFailed = errors.New("failed to resolve team")
)

// Authenticator handles resolving an API key to a Team ID with an in-memory TTL cache.
type Authenticator struct {
	db         *sql.DB
	keyCache   map[string]cachedTeam
	cacheMutex sync.RWMutex
}

type cachedTeam struct {
	teamID    string
	expiresAt time.Time
}

// NewAuthenticator creates a common auth resolver for both HTTP and gRPC handlers.
func NewAuthenticator(db *sql.DB) *Authenticator {
	return &Authenticator{
		db:       db,
		keyCache: make(map[string]cachedTeam),
	}
}

// ResolveTeamID returns the canonical team UUID string for the given API key.
// It uses a 5-minute in-memory cache to prevent database thrashing.
func (a *Authenticator) ResolveTeamID(ctx context.Context, apiKey string) (string, error) {
	if apiKey == "" {
		return "", ErrMissingAPIKey
	}

	a.cacheMutex.RLock()
	cached, found := a.keyCache[apiKey]
	a.cacheMutex.RUnlock()

	// Use cache if entry exists and hasn't expired.
	if found && time.Now().Before(cached.expiresAt) {
		return cached.teamID, nil
	}

	var teamID int64
	err := a.db.QueryRowContext(ctx,
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID)

	if err == sql.ErrNoRows {
		return "", ErrInvalidAPIKey
	}
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrResolveFailed, err)
	}

	resolvedTeamID := helpers.ToTeamUUID(teamID)

	// Cache the valid resolution for 5 minutes.
	a.cacheMutex.Lock()
	a.keyCache[apiKey] = cachedTeam{
		teamID:    resolvedTeamID,
		expiresAt: time.Now().Add(5 * time.Minute),
	}
	a.cacheMutex.Unlock()

	return resolvedTeamID, nil
}
