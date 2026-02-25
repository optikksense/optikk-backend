package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/observability/observability-backend-go/internal/telemetry/model"
	"github.com/observability/observability-backend-go/internal/telemetry/store"
)

// KafkaConsumerConfig holds batching parameters for the consumer.
type KafkaConsumerConfig struct {
	Brokers       []string
	BatchSize     int
	FlushInterval time.Duration
}

// KafkaConsumer reads from Kafka topics and batch-inserts into ClickHouse.
type KafkaConsumer struct {
	repo   *store.Repository
	cfg    KafkaConsumerConfig
	client sarama.Consumer
	wg     sync.WaitGroup
}

// NewKafkaConsumer creates a new consumer connected to the given brokers.
func NewKafkaConsumer(repo *store.Repository, cfg KafkaConsumerConfig) (*KafkaConsumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Return.Errors = true

	client, err := sarama.NewConsumer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}

	return &KafkaConsumer{
		repo:   repo,
		cfg:    cfg,
		client: client,
	}, nil
}

// Start launches three consumer goroutines (one per topic).
func (kc *KafkaConsumer) Start(ctx context.Context) {
	kc.wg.Add(3)
	go kc.consumeSpans(ctx)
	go kc.consumeMetrics(ctx)
	go kc.consumeLogs(ctx)

	log.Printf("kafka: consumer started (batch=%d, flush=%s)",
		kc.cfg.BatchSize, kc.cfg.FlushInterval)
}

// Wait blocks until all consumer goroutines have exited.
func (kc *KafkaConsumer) Wait() {
	kc.wg.Wait()
}

// Close closes the underlying Sarama client.
func (kc *KafkaConsumer) Close() error {
	return kc.client.Close()
}

func (kc *KafkaConsumer) consumeSpans(ctx context.Context) {
	defer kc.wg.Done()

	pc, err := kc.client.ConsumePartition(TopicSpans, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("kafka: failed to consume %s: %v", TopicSpans, err)
		return
	}
	defer pc.Close()

	buf := make([]model.SpanRecord, 0, kc.cfg.BatchSize)
	ticker := time.NewTicker(kc.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := kc.repo.InsertSpans(context.Background(), buf); err != nil {
			log.Printf("kafka: failed to flush %d spans: %v", len(buf), err)
		} else {
			log.Printf("kafka: flushed %d spans", len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg := <-pc.Messages():
			var spans []model.SpanRecord
			if err := json.Unmarshal(msg.Value, &spans); err != nil {
				log.Printf("kafka: unmarshal spans: %v", err)
				continue
			}
			buf = append(buf, spans...)
			if len(buf) >= kc.cfg.BatchSize {
				flush()
			}
		case err := <-pc.Errors():
			log.Printf("kafka: consumer error on %s: %v", TopicSpans, err)
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func (kc *KafkaConsumer) consumeMetrics(ctx context.Context) {
	defer kc.wg.Done()

	pc, err := kc.client.ConsumePartition(TopicMetrics, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("kafka: failed to consume %s: %v", TopicMetrics, err)
		return
	}
	defer pc.Close()

	buf := make([]model.MetricRecord, 0, kc.cfg.BatchSize)
	ticker := time.NewTicker(kc.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := kc.repo.InsertMetrics(context.Background(), buf); err != nil {
			log.Printf("kafka: failed to flush %d metrics: %v", len(buf), err)
		} else {
			log.Printf("kafka: flushed %d metrics", len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg := <-pc.Messages():
			var metrics []model.MetricRecord
			if err := json.Unmarshal(msg.Value, &metrics); err != nil {
				log.Printf("kafka: unmarshal metrics: %v", err)
				continue
			}
			buf = append(buf, metrics...)
			if len(buf) >= kc.cfg.BatchSize {
				flush()
			}
		case err := <-pc.Errors():
			log.Printf("kafka: consumer error on %s: %v", TopicMetrics, err)
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func (kc *KafkaConsumer) consumeLogs(ctx context.Context) {
	defer kc.wg.Done()

	pc, err := kc.client.ConsumePartition(TopicLogs, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("kafka: failed to consume %s: %v", TopicLogs, err)
		return
	}
	defer pc.Close()

	buf := make([]model.LogRecord, 0, kc.cfg.BatchSize)
	ticker := time.NewTicker(kc.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := kc.repo.InsertLogs(context.Background(), buf); err != nil {
			log.Printf("kafka: failed to flush %d logs: %v", len(buf), err)
		} else {
			log.Printf("kafka: flushed %d logs", len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg := <-pc.Messages():
			var logs []model.LogRecord
			if err := json.Unmarshal(msg.Value, &logs); err != nil {
				log.Printf("kafka: unmarshal logs: %v", err)
				continue
			}
			buf = append(buf, logs...)
			if len(buf) >= kc.cfg.BatchSize {
				flush()
			}
		case err := <-pc.Errors():
			log.Printf("kafka: consumer error on %s: %v", TopicLogs, err)
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}
