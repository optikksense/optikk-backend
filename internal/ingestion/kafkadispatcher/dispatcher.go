package kafkadispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// KafkaDispatcher implements pushing generic telemetry batches using Kafka.
// T must implement proto.Message — the dispatcher serializes natively with proto.Marshal.
// It separates the LiveTail streaming from Persistence using dedicated consumer groups.
type KafkaDispatcher[T proto.Message] struct {
	signal          string
	topic           string
	producer        *kgo.Client
	consumerStream  *kgo.Client
	consumerPersist *kgo.Client

	handlers ingestion.Handlers[T]
	newMsg   func() T

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New initializes a KafkaDispatcher. newMsg is a factory that returns a blank proto message
// for use during Unmarshal in the consumer loops.
func New[T proto.Message](
	brokers []string,
	groupBase, topic, signal string,
	newMsg func() T,
	handlers ingestion.Handlers[T],
) (*KafkaDispatcher[T], error) {
	producer, stream, persist, err := kafka.InitIngestClients(brokers, groupBase, topic, signal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &KafkaDispatcher[T]{
		signal:          signal,
		topic:           topic,
		producer:        producer,
		consumerStream:  stream,
		consumerPersist: persist,
		handlers:        handlers,
		newMsg:          newMsg,
		cancel:          cancel,
	}
	d.startLoops(ctx)
	return d, nil
}

// Dispatch serializes the batch using proto.Marshal and pushes it to the Kafka topic.
func (d *KafkaDispatcher[T]) Dispatch(batch ingestion.TelemetryBatch[T]) error {
	if len(batch.Rows) == 0 {
		return nil
	}
	if len(batch.Rows) != 1 {
		return fmt.Errorf("kafka dispatch %s: expected single proto.Message envelope, got %d", d.signal, len(batch.Rows))
	}

	payload, err := proto.Marshal(batch.Rows[0])
	if err != nil {
		slog.Error("kafka: proto marshal failed", slog.String("signal", d.signal), slog.Any("error", err))
		return fmt.Errorf("kafka marshal %s: %w", d.signal, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	d.producer.Produce(ctx, &kgo.Record{Topic: d.topic, Value: payload}, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error("kafka: async publish failed", slog.String("signal", d.signal), slog.Any("error", err))
		}
	})
	return nil
}

// Close formally drains dependencies and shuts down active loops.
func (d *KafkaDispatcher[T]) Close() {
	d.cancel()
	d.wg.Wait()
	d.consumerStream.Close()
	d.consumerPersist.Close()
	d.producer.Close()
}

func (d *KafkaDispatcher[T]) startLoops(ctx context.Context) {
	if d.handlers.OnStreaming != nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.consumeStreamLoop(ctx)
		}()
	}
	if d.handlers.OnPersistence != nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.consumePersistLoop(ctx)
		}()
	}
}
