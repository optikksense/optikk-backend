package ingestion

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaDispatcher implements Dispatcher[T] using Kafka for durable buffering toward
// persistence workers. Streaming() is fed from the same consume loop (non-blocking send).
//
// Delivery semantics (Phase 1 change):
//   - Each consumed record is wrapped in an AckableBatch with a closure that owns
//     the source record. The closure commits the offset only after the worker
//     invokes Ack.
//   - Ack(nil) → commit source offset.
//   - Ack(err) → produce original record bytes to <topic>.dlq, then commit.
//
// This removes the prior at-most-once behavior where offsets advanced before
// the CH flush completed, which silently dropped data on crash or flush failure.
type KafkaDispatcher[T any] struct {
	signal    string
	topic     string
	dlqTopic  string
	producer  *kgo.Client
	consumer  *kgo.Client

	persistence chan AckableBatch[T]
	streaming   chan TelemetryBatch[T]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewKafkaDispatcher starts a consumer group client and a producer client for one signal topic.
func NewKafkaDispatcher[T any](brokers []string, groupBase, topic, signal string) (*KafkaDispatcher[T], error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers for %s", signal)
	}
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil, fmt.Errorf("kafka: empty topic for %s", signal)
	}
	groupBase = strings.TrimSpace(groupBase)
	if groupBase == "" {
		return nil, fmt.Errorf("kafka: empty consumer group base")
	}
	groupID := groupBase + "-" + signal

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka: producer client %s: %w", signal, err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("kafka: consumer client %s: %w", signal, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &KafkaDispatcher[T]{
		signal:      signal,
		topic:       topic,
		dlqTopic:    kafkainfra.DLQTopicFor(topic),
		producer:    producer,
		consumer:    consumer,
		persistence: make(chan AckableBatch[T], 512),
		streaming:   make(chan TelemetryBatch[T], 512),
		cancel:      cancel,
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.consumeLoop(ctx)
	}()

	return d, nil
}

func (d *KafkaDispatcher[T]) Dispatch(batch TelemetryBatch[T]) error {
	if len(batch.Rows) == 0 {
		return nil
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		slog.Error("kafka: marshal batch failed", slog.String("signal", d.signal), slog.Any("error", err))
		return fmt.Errorf("kafka marshal %s: %w", d.signal, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := d.producer.ProduceSync(ctx, &kgo.Record{
		Topic: d.topic,
		Value: payload,
	})
	if err := res.FirstErr(); err != nil {
		slog.Error("kafka: publish failed", slog.String("signal", d.signal), slog.Any("error", err))
		return fmt.Errorf("kafka publish %s: %w", d.signal, err)
	}
	return nil
}

func (d *KafkaDispatcher[T]) Persistence() <-chan AckableBatch[T] { return d.persistence }

func (d *KafkaDispatcher[T]) Streaming() <-chan TelemetryBatch[T] { return d.streaming }

func (d *KafkaDispatcher[T]) Close() {
	d.cancel()
	d.wg.Wait()
	d.consumer.Close()
	d.producer.Close()
	close(d.persistence)
	close(d.streaming)
}

func (d *KafkaDispatcher[T]) consumeLoop(ctx context.Context) {
	for {
		fetches := d.consumer.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
				return
			}
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if err != nil {
				slog.Error("kafka: partition fetch error",
					slog.String("signal", d.signal),
					slog.String("topic", topic),
					slog.Int("partition", int(p)),
					slog.Any("error", err))
			}
		})

		iter := fetches.RecordIter()
		for !iter.Done() {
			r := iter.Next()

			var batch TelemetryBatch[T]
			if err := json.Unmarshal(r.Value, &batch); err != nil {
				slog.Error("kafka: unmarshal batch failed", slog.String("signal", d.signal), slog.Any("error", err))
				// Unparseable record: drop to DLQ and commit so it does not block the partition.
				d.produceToDLQ(ctx, r, "unmarshal_failed")
				if err := d.consumer.CommitRecords(ctx, r); err != nil {
					slog.Warn("kafka: commit after bad message", slog.String("signal", d.signal), slog.Any("error", err))
				}
				continue
			}

			record := r // capture for the ack closure
			token := dedupToken(record.Value)
			ack := d.makeAck(ctx, record)

			select {
			case d.persistence <- AckableBatch[T]{Batch: batch, Ack: ack, DedupToken: token}:
			case <-ctx.Done():
				return
			}

			select {
			case d.streaming <- batch:
			default:
			}
		}
	}
}

// makeAck returns a one-shot ack closure. It is safe to call exactly once per
// record; subsequent calls are no-ops (a log line is emitted).
func (d *KafkaDispatcher[T]) makeAck(ctx context.Context, record *kgo.Record) func(error) {
	var once sync.Once
	return func(flushErr error) {
		once.Do(func() {
			if flushErr != nil {
				d.produceToDLQ(ctx, record, flushErr.Error())
			}
			if err := d.consumer.CommitRecords(ctx, record); err != nil {
				slog.Warn("kafka: commit failed",
					slog.String("signal", d.signal),
					slog.Any("error", err))
			}
		})
	}
}

// produceToDLQ publishes the original record bytes to the per-signal DLQ topic.
// Best-effort: a DLQ produce failure is logged but does not block offset commit
// (the alternative is a stuck partition).
func (d *KafkaDispatcher[T]) produceToDLQ(ctx context.Context, record *kgo.Record, reason string) {
	if d.dlqTopic == "" {
		slog.Error("kafka: DLQ unavailable, dropping batch",
			slog.String("signal", d.signal),
			slog.String("reason", reason))
		return
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	headers := append([]kgo.RecordHeader{}, record.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: "dlq_reason", Value: []byte(reason)},
		kgo.RecordHeader{Key: "dlq_source_topic", Value: []byte(record.Topic)},
		kgo.RecordHeader{Key: "dlq_source_partition", Value: []byte(fmt.Sprintf("%d", record.Partition))},
		kgo.RecordHeader{Key: "dlq_source_offset", Value: []byte(fmt.Sprintf("%d", record.Offset))},
	)
	res := d.producer.ProduceSync(pctx, &kgo.Record{
		Topic:   d.dlqTopic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: headers,
	})
	if err := res.FirstErr(); err != nil {
		slog.Error("kafka: DLQ publish failed",
			slog.String("signal", d.signal),
			slog.String("dlq_topic", d.dlqTopic),
			slog.Any("error", err))
		return
	}
	slog.Warn("kafka: batch routed to DLQ",
		slog.String("signal", d.signal),
		slog.String("dlq_topic", d.dlqTopic),
		slog.String("reason", reason))
}

// dedupToken returns a stable hex-encoded SHA-1 of the raw Kafka record bytes.
// Identical source records (e.g. redelivered after a crash before commit)
// produce the same token, which ClickHouse uses to collapse duplicate inserts.
func dedupToken(recordValue []byte) string {
	if len(recordValue) == 0 {
		return ""
	}
	sum := sha1.Sum(recordValue)
	return hex.EncodeToString(sum[:])
}
