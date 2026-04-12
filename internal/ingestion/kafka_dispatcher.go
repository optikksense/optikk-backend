package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaDispatcher implements Dispatcher[T] using Kafka for durable buffering toward
// persistence workers. Streaming() is fed from the same consume loop (non-blocking send).
type KafkaDispatcher[T any] struct {
	signal   string
	topic    string
	producer *kgo.Client
	consumer *kgo.Client

	persistence chan TelemetryBatch[T]
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
		producer:    producer,
		consumer:    consumer,
		persistence: make(chan TelemetryBatch[T], 512),
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

func (d *KafkaDispatcher[T]) Dispatch(batch TelemetryBatch[T]) {
	if len(batch.Rows) == 0 {
		return
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		slog.Error("kafka: marshal batch failed", slog.String("signal", d.signal), slog.Any("error", err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := d.producer.ProduceSync(ctx, &kgo.Record{
		Topic: d.topic,
		Value: payload,
	})
	if err := res.FirstErr(); err != nil {
		slog.Error("kafka: publish failed", slog.String("signal", d.signal), slog.Any("error", err))
	}
}

func (d *KafkaDispatcher[T]) Persistence() <-chan TelemetryBatch[T] { return d.persistence }

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
				if err := d.consumer.CommitRecords(ctx, r); err != nil {
					slog.Warn("kafka: commit after bad message", slog.String("signal", d.signal), slog.Any("error", err))
				}
				continue
			}

			select {
			case d.persistence <- batch:
			case <-ctx.Done():
				return
			}

			select {
			case d.streaming <- batch:
			default:
			}

			if err := d.consumer.CommitRecords(ctx, r); err != nil {
				slog.Warn("kafka: commit failed", slog.String("signal", d.signal), slog.Any("error", err))
			}
		}
	}
}
