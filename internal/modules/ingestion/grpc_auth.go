package telemetry

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	util "github.com/observability/observability-backend-go/internal/helpers"
)

// teamUUIDKey is the context key used to pass the resolved team UUID from the
// auth interceptor to the gRPC handler.
type teamUUIDKey struct{}

// TeamUUIDFromContext extracts the team UUID stored by the auth interceptor.
func TeamUUIDFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(teamUUIDKey{}).(string)
	return v, ok && v != ""
}

// GRPCAuthInterceptor returns a unary server interceptor that validates API
// keys from gRPC metadata (headers). It mirrors the HTTP handler's auth logic,
// extracting the key from "authorization" (Bearer) or "x-api-key" metadata.
func GRPCAuthInterceptor(db *sql.DB) grpc.UnaryServerInterceptor {
	cached := &cachedAPIKeyResolver{db: db}

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Skip auth for health checks and reflection.
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") ||
			strings.HasPrefix(info.FullMethod, "/grpc.reflection.v1alpha.") ||
			strings.HasPrefix(info.FullMethod, "/grpc.reflection.v1.") {
			return handler(ctx, req)
		}

		apiKey := extractAPIKeyFromMetadata(ctx)
		if apiKey == "" {
			return nil, status.Error(codes.Unauthenticated, "missing api_key: set 'authorization: Bearer <key>' or 'x-api-key: <key>' in gRPC metadata")
		}

		teamUUID, ok := resolveAPIKeyDirect(cached, apiKey)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid api_key")
		}

		ctx = context.WithValue(ctx, teamUUIDKey{}, teamUUID)
		return handler(ctx, req)
	}
}

// extractAPIKeyFromMetadata extracts the API key from gRPC metadata.
func extractAPIKeyFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Println("DEBUG grpc-auth: no incoming metadata on context")
		return ""
	}

	// Dump every metadata key so we can diagnose what the client sends.
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	log.Printf("DEBUG grpc-auth: metadata keys=%v", keys)

	// 1. Check "authorization" header (Bearer token).
	// gRPC metadata keys are always lowercased.
	if vals := md.Get("authorization"); len(vals) > 0 {
		for _, auth := range vals {
			if strings.HasPrefix(auth, "Bearer ") {
				if key := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")); key != "" {
					return key
				}
			}
		}
	}

	// 2. Check "x-api-key" header.
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		for _, key := range vals {
			if k := strings.TrimSpace(key); k != "" {
				return k
			}
		}
	}

	return ""
}

// resolveAPIKeyDirect resolves an API key without gin.Context, for gRPC use.
func resolveAPIKeyDirect(r *cachedAPIKeyResolver, apiKey string) (string, bool) {
	if apiKey == "" {
		return "", false
	}

	// Fast path: cache hit.
	if v, ok := r.cache.Load(apiKey); ok {
		entry := v.(cacheEntry)
		if time.Now().Before(entry.expiresAt) {
			return entry.teamUUID, true
		}
		r.cache.Delete(apiKey)
	}

	// Slow path: MySQL lookup.
	var teamID int64
	if err := r.db.QueryRow(
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID); err != nil {
		return "", false
	}

	teamUUID := util.ToTeamUUID(teamID)
	r.cache.Store(apiKey, cacheEntry{teamUUID: teamUUID, expiresAt: time.Now().Add(apiKeyCacheTTL)})
	return teamUUID, true
}
