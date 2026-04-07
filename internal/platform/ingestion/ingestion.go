package ingestion

import (
	"context"
)

type TelemetryBatch[T any] struct {
	TeamID int64
	Rows   []T
}

type Dispatcher[T any] interface {
	Dispatch(batch TelemetryBatch[T])
	Persistence() <-chan TelemetryBatch[T]
	Streaming() <-chan TelemetryBatch[T]
	Close()
}

type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

type Limiter interface {
	Allow(teamID int64, n int64) bool
}

type SizeTracker interface {
	Track(teamID int64, bytes int64)
}
