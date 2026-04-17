package ingestion

import (
	"context"
)

type TelemetryBatch[T any] struct {
	TeamID int64
	Rows   []T
}

// AckableBatch carries a TelemetryBatch together with:
//   - Ack: invoked by the worker after the CH flush resolves. Ack(nil) commits
//     the source Kafka offset; Ack(err) first produces the original record to
//     the per-signal DLQ topic and then commits the offset. The offset must
//     not advance until Ack is called.
//   - DedupToken: stable hash of the source record used as
//     `insert_deduplication_token` on the ClickHouse batch insert so that
//     redelivered batches (same Kafka record replayed after a crash) collapse
//     into a single physical write.
type AckableBatch[T any] struct {
	Batch      TelemetryBatch[T]
	Ack        func(err error)
	DedupToken string
}

type Dispatcher[T any] interface {
	Dispatch(batch TelemetryBatch[T]) error
	Persistence() <-chan AckableBatch[T]
	Streaming() <-chan TelemetryBatch[T]
	Close()
}

type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

type SizeTracker interface {
	Track(teamID int64, bytes int64)
}
