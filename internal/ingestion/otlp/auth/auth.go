package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// ResolveFromContext extracts the x-api-key from gRPC metadata and resolves it
// to a team ID via the given resolver.
func ResolveFromContext(ctx context.Context, resolver ingestion.TeamResolver) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		slog.Warn("OTLP request missing metadata")
		return 0, status.Error(codes.Unauthenticated, "missing metadata")
	}

	keys := md.Get("x-api-key")
	if len(keys) == 0 {
		slog.Warn("OTLP request missing x-api-key header")
		return 0, status.Error(codes.Unauthenticated, "missing x-api-key metadata header")
	}

	apiKey := keys[0]
	maskedKey := apiKey
	if len(apiKey) > 8 {
		maskedKey = apiKey[:4] + "..." + apiKey[len(apiKey)-4:]
	}

	teamID, err := resolver.ResolveTeamID(ctx, apiKey)
	if err != nil {
		slog.Error("OTLP auth failed", slog.String("apiKey", maskedKey), slog.Any("error", err))
		if errors.Is(err, ErrMissingAPIKey) || errors.Is(err, ErrInvalidAPIKey) {
			return 0, status.Error(codes.Unauthenticated, err.Error())
		}
		return 0, status.Error(codes.Internal, err.Error())
	}

	slog.Debug("OTLP request authenticated", slog.String("apiKey", maskedKey), slog.Int64("teamID", teamID))
	return teamID, nil
}

// TrackPayloadSize records the serialized size of a protobuf message for usage tracking.
func TrackPayloadSize(tracker ingestion.SizeTracker, teamID int64, msg proto.Message) {
	if tracker == nil || teamID <= 0 || msg == nil {
		return
	}
	size := proto.Size(msg)
	if size <= 0 {
		return
	}
	tracker.Track(teamID, int64(size))
}
