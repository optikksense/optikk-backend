package ingestion

import (
	"context"
)

type TelemetryBatch[T any] struct {
	TeamID int64
	Rows   []T
}

type Dispatcher[T any] interface {
	Dispatch(batch TelemetryBatch[T]) error
	Persistence() <-chan TelemetryBatch[T]
	Streaming() <-chan TelemetryBatch[T]
	Close()
}

type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

type SizeTracker interface {
	Track(teamID int64, bytes int64)
}
