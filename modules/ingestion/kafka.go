package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	TopicSpans   = "otlp-spans"
	TopicMetrics = "otlp-metrics"
	TopicLogs    = "otlp-logs"
)

// ---------------------------------------------------------------------------
// KafkaIngester — producer side
// ---------------------------------------------------------------------------

// KafkaIngester produces telemetry records as JSON to Kafka topics.
// It is the ingestion path when Kafka mode is enabled; records are consumed
// and written to ClickHouse by KafkaConsumer running in a background goroutine.
type KafkaIngester struct {
	producer sarama.SyncProducer
}

func NewKafkaIngester(brokers []string) (*KafkaIngester, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.MaxMessageBytes = 50 * 1024 * 1024

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	log.Printf("kafka: producer connected to %v", brokers)
	return &KafkaIngester{producer: producer}, nil
}

func (k *KafkaIngester) IngestSpans(_ context.Context, spans []SpanRecord) error {
	return k.produceChunked(TopicSpans, spans, 500)
}

func (k *KafkaIngester) IngestMetrics(_ context.Context, metrics []MetricRecord) error {
	return k.produceChunked(TopicMetrics, metrics, 100)
}

func (k *KafkaIngester) IngestLogs(_ context.Context, logs []LogRecord) error {
	return k.produceChunked(TopicLogs, logs, 500)
}

func (k *KafkaIngester) Close() error { return k.producer.Close() }

func (k *KafkaIngester) produceChunked(topic string, records any, chunkSize int) error {
	// Use reflection-free chunking via json.Marshal on the full slice first;
	// chunk boundaries are applied at the message level.
	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}
	// Split into chunks only when the payload is very large (> 10 MB).
	// For typical batch sizes the single message is fine.
	const maxMsgBytes = 10 * 1024 * 1024
	if len(data) > maxMsgBytes {
		return k.produceInChunks(topic, records, chunkSize)
	}
	_, _, err = k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (k *KafkaIngester) produceInChunks(topic string, records any, chunkSize int) error {
	// Type-switch to chunk each signal type. Avoids reflect.
	switch recs := records.(type) {
	case []SpanRecord:
		for i := 0; i < len(recs); i += chunkSize {
			end := i + chunkSize
			if end > len(recs) {
				end = len(recs)
			}
			if err := k.produce(topic, recs[i:end]); err != nil {
				return err
			}
		}
	case []MetricRecord:
		for i := 0; i < len(recs); i += chunkSize {
			end := i + chunkSize
			if end > len(recs) {
				end = len(recs)
			}
			if err := k.produce(topic, recs[i:end]); err != nil {
				return err
			}
		}
	case []LogRecord:
		for i := 0; i < len(recs); i += chunkSize {
			end := i + chunkSize
			if end > len(recs) {
				end = len(recs)
			}
			if err := k.produce(topic, recs[i:end]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (k *KafkaIngester) produce(topic string, records any) error {
	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}
	_, _, err = k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	})
	if err != nil {
		return fmt.Errorf("kafka send to %s: %w", topic, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// KafkaConsumer — consumer side
// ---------------------------------------------------------------------------

// KafkaConsumerConfig holds batching parameters for the consumer.
type KafkaConsumerConfig struct {
	Brokers       []string
	BatchSize     int
	FlushInterval time.Duration
}

// KafkaConsumer reads from Kafka topics and batch-inserts into ClickHouse.
// One goroutine per topic; each goroutine batches messages and flushes on
// BatchSize threshold, FlushInterval timer, or context cancellation.
//
// TODO: Migrate from partition consumer to ConsumerGroup for proper offset
// management and at-least-once delivery guarantees across restarts.
type KafkaConsumer struct {
	repo   *Repository
	cfg    KafkaConsumerConfig
	client sarama.Consumer
	wg     sync.WaitGroup
}

func NewKafkaConsumer(repo *Repository, cfg KafkaConsumerConfig) (*KafkaConsumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Return.Errors = true

	client, err := sarama.NewConsumer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &KafkaConsumer{repo: repo, cfg: cfg, client: client}, nil
}

// Start launches three consumer goroutines (one per topic).
func (kc *KafkaConsumer) Start(ctx context.Context) {
	kc.wg.Add(3)
	go kc.consumeSpans(ctx)
	go kc.consumeMetrics(ctx)
	go kc.consumeLogs(ctx)
	log.Printf("kafka: consumer started (batch=%d, flush=%s)", kc.cfg.BatchSize, kc.cfg.FlushInterval)
}

// Wait blocks until all consumer goroutines have exited.
func (kc *KafkaConsumer) Wait() { kc.wg.Wait() }

// Close closes the underlying Sarama client.
func (kc *KafkaConsumer) Close() error { return kc.client.Close() }

func (kc *KafkaConsumer) consumeSpans(ctx context.Context) {
	defer kc.wg.Done()
	pc, err := kc.client.ConsumePartition(TopicSpans, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("kafka: failed to consume %s: %v", TopicSpans, err)
		return
	}
	defer pc.Close()

	buf := make([]SpanRecord, 0, kc.cfg.BatchSize)
	ticker := time.NewTicker(kc.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := kc.repo.InsertSpans(context.Background(), buf); err != nil {
			log.Printf("kafka: flush %d spans: %v", len(buf), err)
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg := <-pc.Messages():
			var spans []SpanRecord
			if err := json.Unmarshal(msg.Value, &spans); err != nil {
				log.Printf("kafka: unmarshal spans: %v", err)
				continue
			}
			buf = append(buf, spans...)
			if len(buf) >= kc.cfg.BatchSize {
				flush()
			}
		case err := <-pc.Errors():
			log.Printf("kafka: error on %s: %v", TopicSpans, err)
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

	batchSize := kc.cfg.BatchSize
	if batchSize <= 0 || batchSize > 100 {
		batchSize = 100
	}
	buf := make([]MetricRecord, 0, batchSize)
	ticker := time.NewTicker(kc.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := kc.repo.InsertMetrics(context.Background(), buf); err != nil {
			log.Printf("kafka: flush %d metrics: %v", len(buf), err)
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg := <-pc.Messages():
			var metrics []MetricRecord
			if err := json.Unmarshal(msg.Value, &metrics); err != nil {
				log.Printf("kafka: unmarshal metrics: %v", err)
				continue
			}
			buf = append(buf, metrics...)
			if len(buf) >= batchSize {
				flush()
			}
		case err := <-pc.Errors():
			log.Printf("kafka: error on %s: %v", TopicMetrics, err)
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

	buf := make([]LogRecord, 0, kc.cfg.BatchSize)
	ticker := time.NewTicker(kc.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := kc.repo.InsertLogs(context.Background(), buf); err != nil {
			log.Printf("kafka: flush %d logs: %v", len(buf), err)
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg := <-pc.Messages():
			var logs []LogRecord
			if err := json.Unmarshal(msg.Value, &logs); err != nil {
				log.Printf("kafka: unmarshal logs: %v", err)
				continue
			}
			buf = append(buf, logs...)
			if len(buf) >= kc.cfg.BatchSize {
				flush()
			}
		case err := <-pc.Errors():
			log.Printf("kafka: error on %s: %v", TopicLogs, err)
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}
