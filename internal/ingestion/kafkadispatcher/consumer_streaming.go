package kafkadispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/twmb/franz-go/pkg/kgo"
)

// consumeStreamLoop targets instantaneous low-latency WebSocket live-tail publishing
func (d *KafkaDispatcher[T]) consumeStreamLoop(ctx context.Context) {
	for {
		fetches := d.consumerStream.PollFetches(ctx)
		if d.handleStreamFetchErrors(fetches) {
			return
		}

		d.processStreamRows(ctx, fetches)

		if err := d.consumerStream.CommitUncommittedOffsets(ctx); err != nil {
			slog.Warn("kafka: stream commit offsets failed", slog.String("signal", d.signal), slog.Any("error", err))
		}
	}
}

func (d *KafkaDispatcher[T]) handleStreamFetchErrors(fetches kgo.Fetches) bool {
	if err := fetches.Err0(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
			return true
		}
	}
	return false
}

func (d *KafkaDispatcher[T]) processStreamRows(ctx context.Context, fetches kgo.Fetches) {
	iter := fetches.RecordIter()
	for !iter.Done() {
		r := iter.Next()

		var batch ingestion.TelemetryBatch[T]
		if err := json.Unmarshal(r.Value, &batch); err != nil {
			slog.Error("kafka: stream unmarshal failed", slog.String("signal", d.signal), slog.Any("error", err))
			continue
		}

		d.handlers.OnStreaming(ctx, batch)
	}
}
