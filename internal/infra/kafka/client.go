package kafka

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds the inputs every kgo.Client in this process needs. The zero
// value is not usable — Brokers must be non-empty. Role-specific options
// (producer batching, consumer group + DisableAutoCommit) are layered on top
// inside NewProducer / NewConsumer.
type Config struct {
	Brokers []string
}

// NewProducerClient returns a franz-go client configured for producing to
// ingest topics. Batching knobs are explicit so the "in batches" contract in
// the plan is not hidden in library defaults.
func NewProducerClient(cfg Config) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(50*time.Millisecond),
		kgo.ProducerBatchMaxBytes(256*1024),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
}

// NewConsumerClient returns a franz-go client joined to groupID and reading
// topic. Offsets are committed manually by the consumer after each batch is
// persisted or broadcast — see Consumer.Commit.
func NewConsumerClient(cfg Config, groupID, topic string) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	if groupID == "" {
		return nil, fmt.Errorf("kafka: empty consumer group id")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka: empty topic")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(2*time.Second),
	)
}
