package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

const (
	TopicSpans   = "otlp-spans"
	TopicMetrics = "otlp-metrics"
	TopicLogs    = "otlp-logs"
)

// KafkaIngester produces telemetry records as JSON to Kafka topics.
type KafkaIngester struct {
	producer sarama.SyncProducer
}

// NewKafkaIngester creates a Sarama SyncProducer connected to the given brokers.
func NewKafkaIngester(brokers []string) (*KafkaIngester, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.MaxMessageBytes = 50 * 1024 * 1024 // 50MB

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	log.Printf("kafka: producer connected to %v", brokers)
	return &KafkaIngester{producer: producer}, nil
}

func (k *KafkaIngester) IngestSpans(ctx context.Context, spans []model.SpanRecord) error {
	const chunkSize = 500
	for i := 0; i < len(spans); i += chunkSize {
		end := i + chunkSize
		if end > len(spans) {
			end = len(spans)
		}
		if err := k.produce(TopicSpans, spans[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaIngester) IngestMetrics(ctx context.Context, metrics []model.MetricRecord) error {
	const chunkSize = 500
	for i := 0; i < len(metrics); i += chunkSize {
		end := i + chunkSize
		if end > len(metrics) {
			end = len(metrics)
		}
		if err := k.produce(TopicMetrics, metrics[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaIngester) IngestLogs(ctx context.Context, logs []model.LogRecord) error {
	const chunkSize = 500
	for i := 0; i < len(logs); i += chunkSize {
		end := i + chunkSize
		if end > len(logs) {
			end = len(logs)
		}
		if err := k.produce(TopicLogs, logs[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaIngester) Close() error {
	return k.producer.Close()
}

func (k *KafkaIngester) produce(topic string, records any) error {
	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	_, _, err = k.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka send to %s: %w", topic, err)
	}
	return nil
}
