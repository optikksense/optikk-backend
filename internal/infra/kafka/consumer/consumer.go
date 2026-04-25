// Package consumer wraps a single-topic, single-group franz-go client for
// the ingest dispatcher pipeline. PollBatch + Commit are retained for any
// legacy caller that hasn't migrated to the generic ingest subpkg yet.
package consumer

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

// Raw exposes the underlying *kgo.Record for consumers that need to pass it
// back to Kafka-native APIs (e.g. the new ingest dispatcher). Tests can leave
// this nil; callers must handle nil gracefully.
func (r Record) Raw() *kgo.Record { return r.raw }

// Consumer wraps a single-topic, single-group franz-go client. The new ingest
// pipeline uses PollFetches directly through Client() for per-partition fanout;
// PollBatch + Commit are preserved for the metrics pipeline which has not yet
// been converted to the generic ingest subpkg.
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(client *kgo.Client) *Consumer {
	return &Consumer{client: client}
}

// Client returns the underlying franz-go client so the generic ingest
// dispatcher can call PollFetches / PauseFetchPartitions / CommitRecords
// directly without going through the narrow Record wrapper.
func (c *Consumer) Client() *kgo.Client {
	if c == nil {
		return nil
	}
	return c.client
}

// PollBatch fetches up to the next available batch of records. Retained for
// the metrics persistence consumer which has not yet been migrated to the
// generic ingest subpkg.
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
		slog.WarnContext(ctx, "kafka: partition fetch error",
			slog.String("topic", topic),
			slog.Int("partition", int(p)),
			slog.Any("error", err))
	})
	var out []Record
	iter := fetches.RecordIter()
	for !iter.Done() {
		r := iter.Next()
		out = append(out, Record{
			Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
			Key: r.Key, Value: r.Value, raw: r,
		})
	}
	return out, nil
}

// Commit marks the given records as processed for this consumer's group.
// At-least-once semantics: callers MUST only commit after the downstream
// side-effect (CH insert) has succeeded for the batch.
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

// PauseFetchPartitions is a passthrough used by the ingest dispatcher to stop
// fetching from a saturated partition while its worker drains.
func (c *Consumer) PauseFetchPartitions(parts map[string][]int32) map[string][]int32 {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.PauseFetchPartitions(parts)
}

// ResumeFetchPartitions reverses a prior Pause call once the worker has caught
// up. Zero-arg safe.
func (c *Consumer) ResumeFetchPartitions(parts map[string][]int32) {
	if c == nil || c.client == nil {
		return
	}
	c.client.ResumeFetchPartitions(parts)
}

// Close releases the consumer's underlying connection. Safe to call more than once.
func (c *Consumer) Close() {
	if c == nil || c.client == nil {
		return
	}
	c.client.Close()
}
