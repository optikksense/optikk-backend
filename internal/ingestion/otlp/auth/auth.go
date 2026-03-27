package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	ttlcache "github.com/jellydator/ttlcache/v3"
)

var (
	ErrMissingAPIKey = errors.New("missing API key")
	ErrInvalidAPIKey = errors.New("invalid API key")
	ErrResolveFailed = errors.New("failed to resolve team")
)

const (
	cacheTTL = 5 * time.Minute
)

type Authenticator struct {
	db    *sql.DB
	cache *ttlcache.Cache[string, int64]
}

func NewAuthenticator(db *sql.DB) *Authenticator {
	cache := ttlcache.New[string, int64](ttlcache.WithTTL[string, int64](cacheTTL))
	go cache.Start()

	return &Authenticator{
		db:    db,
		cache: cache,
	}
}

func (a *Authenticator) ResolveTeamID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}

	if item := a.cache.Get(apiKey); item != nil {
		return item.Value(), nil
	}

	var teamID int64
	err := a.db.QueryRowContext(ctx,
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID)

	if err == sql.ErrNoRows {
		return 0, ErrInvalidAPIKey
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrResolveFailed, err)
	}

	a.cache.Set(apiKey, teamID, ttlcache.DefaultTTL)

	return teamID, nil
}
