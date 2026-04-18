package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is the subset of *kgo.Record ingest consumers touch. Keeping the
// dependency surface narrow lets tests substitute fake records without
// pulling in the Kafka client.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	raw       *kgo.Record
}

// Consumer wraps a single-topic, single-group franz-go client. It is owned by
// exactly one goroutine (the per-signal consumer.Run loop). Polling is
// long-polling: PollBatch blocks up to the configured FetchMaxWait and returns
// whatever records are available, so the caller does not need its own timer.
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(client *kgo.Client) *Consumer {
	return &Consumer{client: client}
}

// PollBatch fetches up to the next available batch of records. The returned
// slice is owned by the caller — records can be committed individually via
// Commit or in bulk.
func (c *Consumer) PollBatch(ctx context.Context) ([]Record, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("kafka: nil consumer")
	}
	fetches := c.client.PollFetches(ctx)
	if err := fetches.Err0(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
			return nil, err
		}
	}
	fetches.EachError(func(topic string, p int32, err error) {
		slog.Warn("kafka: partition fetch error",
			slog.String("topic", topic),
			slog.Int("partition", int(p)),
			slog.Any("error", err))
	})
	var out []Record
	iter := fetches.RecordIter()
	for !iter.Done() {
		r := iter.Next()
		out = append(out, Record{
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Key:       r.Key,
			Value:     r.Value,
			raw:       r,
		})
	}
	return out, nil
}

// Commit marks the given records as processed for this consumer's group.
// At-least-once semantics: callers MUST only commit after the downstream
// side-effect (CH insert, hub broadcast) has succeeded for the batch.
func (c *Consumer) Commit(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	raws := make([]*kgo.Record, 0, len(records))
	for _, r := range records {
		if r.raw != nil {
			raws = append(raws, r.raw)
		}
	}
	return c.client.CommitRecords(ctx, raws...)
}

// Close releases the consumer's underlying connection. Safe to call more than once.
func (c *Consumer) Close() {
	if c == nil || c.client == nil {
		return
	}
	c.client.Close()
}
