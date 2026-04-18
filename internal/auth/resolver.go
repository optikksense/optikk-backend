package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrMissingAPIKey = errors.New("missing API key")
	ErrInvalidAPIKey = errors.New("invalid API key")
	ErrResolveFailed = errors.New("failed to resolve team")
)

const (
	cacheTTL        = 5 * time.Minute
	redisKeyPrefix  = "optikk:otlp:team_by_api_key:"
	invalidSentinel = "0"
)

// TeamResolver turns an OTLP API key into the owning team id.
type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

// Authenticator is the single implementation of TeamResolver used by the
// gRPC ingest interceptor. MySQL is the source of truth; Redis caches hits
// and invalid-key sentinels for cacheTTL to spare the DB.
type Authenticator struct {
	db    *sql.DB
	redis *goredis.Client
}

func NewAuthenticator(db *sql.DB, redis *goredis.Client) *Authenticator {
	return &Authenticator{db: db, redis: redis}
}

func (a *Authenticator) ResolveTeamID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}
	if id, ok := a.lookupCache(ctx, apiKey); ok {
		return id, nil
	}
	id, err := a.lookupDB(ctx, apiKey)
	if err != nil {
		if errors.Is(err, ErrInvalidAPIKey) {
			a.cacheSet(ctx, apiKey, invalidSentinel)
		}
		return 0, err
	}
	a.cacheSet(ctx, apiKey, strconv.FormatInt(id, 10))
	return id, nil
}

// lookupCache returns (teamID, true) on cache hit; (0, false) on miss or error.
// Invalid-key sentinel is surfaced via error in the caller below.
func (a *Authenticator) lookupCache(ctx context.Context, apiKey string) (int64, bool) {
	if a.redis == nil {
		return 0, false
	}
	s, err := a.redis.Get(ctx, apiKeyCacheKey(apiKey)).Result()
	if err != nil {
		return 0, false
	}
	if s == invalidSentinel {
		return 0, false
	}
	id, perr := strconv.ParseInt(s, 10, 64)
	if perr != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func (a *Authenticator) lookupDB(ctx context.Context, apiKey string) (int64, error) {
	var teamID int64
	err := a.db.QueryRowContext(ctx,
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrInvalidAPIKey
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrResolveFailed, err)
	}
	return teamID, nil
}

func (a *Authenticator) cacheSet(ctx context.Context, apiKey, value string) {
	if a.redis == nil {
		return
	}
	_ = a.redis.Set(ctx, apiKeyCacheKey(apiKey), value, cacheTTL).Err() //nolint:errcheck
}

func apiKeyCacheKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return redisKeyPrefix + hex.EncodeToString(h[:])
}
