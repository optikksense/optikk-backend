package kafkadispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Dispatcher implements pushing telemetry batches to Kafka topics.
type Dispatcher[T proto.Message] struct {
	Signal   string
	Topic    string
	Producer *kgo.Client
}

// New initializes a Dispatcher.
func New[T proto.Message](producer *kgo.Client, topic, signal string) *Dispatcher[T] {
	return &Dispatcher[T]{
		Signal:   signal,
		Topic:    topic,
		Producer: producer,
	}
}

// Dispatch serializes the message and pushes it to the Kafka topic.
func (d *Dispatcher[T]) Dispatch(ctx context.Context, msg T) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		slog.Error("kafka: proto marshal failed", slog.String("signal", d.Signal), slog.Any("error", err))
		return fmt.Errorf("kafka marshal %s: %w", d.Signal, err)
	}

	produceCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	d.Producer.Produce(produceCtx, &kgo.Record{Topic: d.Topic, Value: payload}, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error("kafka: async publish failed", slog.String("signal", d.Signal), slog.Any("error", err))
		}
	})
	return nil
}

// Close closes the producer client.
func (d *Dispatcher[T]) Close() {
	if d.Producer != nil {
		d.Producer.Close()
	}
}
