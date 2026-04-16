package ingestion

import (
	"context"
)

type TelemetryBatch[T any] struct {
	TeamID int64
	Rows   []T
}

type Handlers[T any] struct {
	OnPersistence func(ctx context.Context, rows []T) error
	OnStreaming   func(ctx context.Context, batch TelemetryBatch[T])
}


type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

type SizeTracker interface {
	Track(teamID int64, bytes int64)
}
