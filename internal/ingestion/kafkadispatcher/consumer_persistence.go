package kafkadispatcher

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// consumePersistLoop targets chunky, high-throughput ClickHouse ingestion.
func (d *KafkaDispatcher[T]) consumePersistLoop(ctx context.Context) {
	for {
		fetches := d.consumerPersist.PollFetches(ctx)
		if d.handleFetchErrors(fetches) {
			return
		}

		rows := d.extractProtoRows(fetches)
		d.flushAndCommitPersist(ctx, rows)
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

func (d *KafkaDispatcher[T]) extractProtoRows(fetches kgo.Fetches) []T {
	iter := fetches.RecordIter()
	var rows []T
	for !iter.Done() {
		r := iter.Next()
		msg := d.newMsg()
		if err := proto.Unmarshal(r.Value, msg); err != nil {
			slog.Error("kafka: persist proto unmarshal failed",
				slog.String("signal", d.signal), slog.Any("error", err))
			continue
		}
		rows = append(rows, msg)
	}
	return rows
}

func (d *KafkaDispatcher[T]) flushAndCommitPersist(ctx context.Context, rows []T) {
	if len(rows) > 0 {
		if err := d.handlers.OnPersistence(ctx, rows); err != nil {
			slog.Error("kafka: persistence flush failed", slog.String("signal", d.signal), slog.Any("error", err))
		}
	}
	if err := d.consumerPersist.CommitUncommittedOffsets(ctx); err != nil {
		slog.Warn("kafka: persist commit offsets failed", slog.String("signal", d.signal), slog.Any("error", err))
	}
}
