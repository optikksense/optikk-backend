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

type Authenticator struct {
	db    *sql.DB
	redis *goredis.Client
}

// NewAuthenticator resolves OTLP API keys to team IDs. When redis is non-nil, successful lookups are cached in Redis with a short TTL.
func NewAuthenticator(db *sql.DB, redis *goredis.Client) *Authenticator {
	return &Authenticator{
		db:    db,
		redis: redis,
	}
}

func apiKeyCacheKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return redisKeyPrefix + hex.EncodeToString(h[:])
}

func (a *Authenticator) ResolveTeamID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}

	if a.redis != nil {
		cacheKey := apiKeyCacheKey(apiKey)
		s, err := a.redis.Get(ctx, cacheKey).Result()
		if err == nil {
			if s == invalidSentinel {
				return 0, ErrInvalidAPIKey
			}
			id, perr := strconv.ParseInt(s, 10, 64)
			if perr == nil && id > 0 {
				return id, nil
			}
		} else if err != goredis.Nil {
			// Redis error: fall through to MySQL without failing the request.
		}
	}

	var teamID int64
	err := a.db.QueryRowContext(ctx,
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID)

	if err == sql.ErrNoRows {
		if a.redis != nil {
			_ = a.redis.Set(ctx, apiKeyCacheKey(apiKey), invalidSentinel, cacheTTL).Err() //nolint:errcheck
		}
		return 0, ErrInvalidAPIKey
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrResolveFailed, err)
	}

	if a.redis != nil {
		_ = a.redis.Set(ctx, apiKeyCacheKey(apiKey), strconv.FormatInt(teamID, 10), cacheTTL).Err() //nolint:errcheck
	}

	return teamID, nil
}
