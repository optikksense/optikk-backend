package otlp

import (
	"context"
	"errors"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func ResolveTeamID(ctx context.Context, resolver ingestion.TeamResolver) (int64, error) {
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
	// Truncate API key for safety in logs
	maskedKey := apiKey
	if len(apiKey) > 8 {
		maskedKey = apiKey[:4] + "..." + apiKey[len(apiKey)-4:]
	}

	teamID, err := resolver.ResolveTeamID(ctx, apiKey)
	if err != nil {
		slog.Error("OTLP auth failed", slog.String("apiKey", maskedKey), slog.Any("error", err))
		if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
			return 0, status.Error(codes.Unauthenticated, err.Error())
		}
		return 0, status.Error(codes.Internal, err.Error())
	}

	slog.Debug("OTLP request authenticated", slog.String("apiKey", maskedKey), slog.Int64("teamID", teamID))
	return teamID, nil
}

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
