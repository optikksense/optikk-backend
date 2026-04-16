package kafkadispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaDispatcher implements pushing generic telemetry batches using Kafka.
// It separates the LiveTail streaming from Persistence using dedicated consumer groups.
type KafkaDispatcher[T any] struct {
	signal          string
	topic           string
	producer        *kgo.Client
	consumerStream  *kgo.Client
	consumerPersist *kgo.Client

	handlers ingestion.Handlers[T]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Dispatch marshals the batch and pushes to the Kafka topic durably.
func (d *KafkaDispatcher[T]) Dispatch(batch ingestion.TelemetryBatch[T]) error {
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

	res := d.producer.ProduceSync(ctx, &kgo.Record{Topic: d.topic, Value: payload})
	if err := res.FirstErr(); err != nil {
		slog.Error("kafka: publish failed", slog.String("signal", d.signal), slog.Any("error", err))
		return fmt.Errorf("kafka publish %s: %w", d.signal, err)
	}
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
