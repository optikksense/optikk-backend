package kafkadispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/twmb/franz-go/pkg/kgo"
)

// consumePersistLoop targets chunky, high-throughput ClickHouse ingestion
func (d *KafkaDispatcher[T]) consumePersistLoop(ctx context.Context) {
	for {
		fetches := d.consumerPersist.PollFetches(ctx)
		if d.handleFetchErrors(fetches) {
			return
		}

		batchRows := d.extractBatchRows(fetches)
		d.flushAndCommitPersist(ctx, batchRows)
	}
}

func (d *KafkaDispatcher[T]) handleFetchErrors(fetches kgo.Fetches) bool {
	if err := fetches.Err0(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
			return true
		}
	}

	fetches.EachError(func(topic string, p int32, err error) {
		if err != nil {
			slog.Error("kafka: persist fetch partition error",
				slog.String("signal", d.signal),
				slog.String("topic", topic),
				slog.Int("partition", int(p)),
				slog.Any("error", err))
		}
	})
	
	return false
}

func (d *KafkaDispatcher[T]) extractBatchRows(fetches kgo.Fetches) []T {
	iter := fetches.RecordIter()
	var batchRows []T
	for !iter.Done() {
		r := iter.Next()

		var batch ingestion.TelemetryBatch[T]
		if err := json.Unmarshal(r.Value, &batch); err != nil {
			slog.Error("kafka: persist unmarshal failed", slog.String("signal", d.signal), slog.Any("error", err))
			continue
		}
		
		batchRows = append(batchRows, batch.Rows...)
	}
	return batchRows
}

func (d *KafkaDispatcher[T]) flushAndCommitPersist(ctx context.Context, batchRows []T) {
	if len(batchRows) > 0 {
		if err := d.handlers.OnPersistence(ctx, batchRows); err != nil {
			slog.Error("kafka: persistence flush failed", slog.String("signal", d.signal), slog.Any("error", err))
		}
	}

	if err := d.consumerPersist.CommitUncommittedOffsets(ctx); err != nil {
		slog.Warn("kafka: persist commit offsets failed", slog.String("signal", d.signal), slog.Any("error", err))
	}
}
