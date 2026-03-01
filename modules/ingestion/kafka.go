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

func (k *KafkaIngester) IngestSpans(_ context.Context, spans []SpanRecord) (*BatchInsertResult, error) {
	err := k.produceChunked(TopicSpans, spans, 500)
	if err != nil {
		return &BatchInsertResult{TotalCount: len(spans), AcceptedCount: 0}, err
	}
	return &BatchInsertResult{TotalCount: len(spans), AcceptedCount: len(spans)}, nil
}

func (k *KafkaIngester) IngestMetrics(_ context.Context, metrics []MetricRecord) (*BatchInsertResult, error) {
	err := k.produceChunked(TopicMetrics, metrics, 100)
	if err != nil {
		return &BatchInsertResult{TotalCount: len(metrics), AcceptedCount: 0}, err
	}
	return &BatchInsertResult{TotalCount: len(metrics), AcceptedCount: len(metrics)}, nil
}

func (k *KafkaIngester) IngestLogs(_ context.Context, logs []LogRecord) (*BatchInsertResult, error) {
	err := k.produceChunked(TopicLogs, logs, 500)
	if err != nil {
		return &BatchInsertResult{TotalCount: len(logs), AcceptedCount: 0}, err
	}
	return &BatchInsertResult{TotalCount: len(logs), AcceptedCount: len(logs)}, nil
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
// KafkaConsumer — consumer side (ConsumerGroup)
// ---------------------------------------------------------------------------

const consumerGroupID = "optic-telemetry"

// KafkaConsumerConfig holds batching parameters for the consumer.
type KafkaConsumerConfig struct {
	Brokers       []string
	BatchSize     int
	FlushInterval time.Duration
}

// KafkaConsumer reads from Kafka topics using a ConsumerGroup for proper offset
// management and at-least-once delivery guarantees. One handler per topic
// batches messages and flushes on BatchSize threshold, FlushInterval timer,
// or rebalance/shutdown.
type KafkaConsumer struct {
	repo   *Repository
	cfg    KafkaConsumerConfig
	group  sarama.ConsumerGroup
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewKafkaConsumer(repo *Repository, cfg KafkaConsumerConfig) (*KafkaConsumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = true
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second

	group, err := sarama.NewConsumerGroup(cfg.Brokers, consumerGroupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer group: %w", err)
	}
	return &KafkaConsumer{repo: repo, cfg: cfg, group: group}, nil
}

// Start launches the consumer group loop. It consumes from all three topics
// (spans, metrics, logs) using a single consumer group.
func (kc *KafkaConsumer) Start(ctx context.Context) {
	ctx, kc.cancel = context.WithCancel(ctx)
	topics := []string{TopicSpans, TopicMetrics, TopicLogs}

	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()
		for {
			handler := &consumerGroupHandler{repo: kc.repo, cfg: kc.cfg}
			if err := kc.group.Consume(ctx, topics, handler); err != nil {
				log.Printf("kafka: consumer group error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			// Rebalance happened — re-enter consume loop.
			log.Println("kafka: consumer group rebalancing…")
		}
	}()

	// Log consumer errors in background.
	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()
		for {
			select {
			case err, ok := <-kc.group.Errors():
				if !ok {
					return
				}
				log.Printf("kafka: consumer group error: %v", err)
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("kafka: consumer group started (group=%s, batch=%d, flush=%s)",
		consumerGroupID, kc.cfg.BatchSize, kc.cfg.FlushInterval)
}

// Wait blocks until all consumer goroutines have exited.
func (kc *KafkaConsumer) Wait() { kc.wg.Wait() }

// Close cancels the consumer context and closes the group.
func (kc *KafkaConsumer) Close() error {
	if kc.cancel != nil {
		kc.cancel()
	}
	return kc.group.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	repo *Repository
	cfg  KafkaConsumerConfig
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batchSize := h.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	ticker := time.NewTicker(h.cfg.FlushInterval)
	defer ticker.Stop()

	topic := claim.Topic()

	switch topic {
	case TopicSpans:
		return consumeAndFlush(session, claim, ticker, batchSize, h.repo.InsertSpans, "spans")
	case TopicMetrics:
		bs := batchSize
		if bs > 100 {
			bs = 100
		}
		return consumeAndFlush(session, claim, ticker, bs, h.repo.InsertMetrics, "metrics")
	case TopicLogs:
		return consumeAndFlush(session, claim, ticker, batchSize, h.repo.InsertLogs, "logs")
	default:
		log.Printf("kafka: unexpected topic %s", topic)
		return nil
	}
}

// consumeAndFlush is a generic consume loop for any signal type.
func consumeAndFlush[T any](
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	ticker *time.Ticker,
	batchSize int,
	insertFn func(context.Context, []T) (*BatchInsertResult, error),
	signalName string,
) error {
	buf := make([]T, 0, batchSize)
	var lastMsg *sarama.ConsumerMessage

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if _, err := insertFn(context.Background(), buf); err != nil {
			log.Printf("kafka: flush %d %s: %v", len(buf), signalName, err)
			return // Don't mark offset on failure — will retry after rebalance.
		}
		if lastMsg != nil {
			session.MarkMessage(lastMsg, "")
		}
		buf = buf[:0]
	}

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				flush()
				return nil
			}
			var records []T
			if err := json.Unmarshal(msg.Value, &records); err != nil {
				log.Printf("kafka: unmarshal %s: %v", signalName, err)
				session.MarkMessage(msg, "") // Skip bad messages.
				continue
			}
			buf = append(buf, records...)
			lastMsg = msg
			if len(buf) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-session.Context().Done():
			flush()
			return nil
		}
	}
}
