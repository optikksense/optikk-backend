// Package client constructs franz-go kgo.Client instances tuned for the
// producer and consumer roles used by the ingest pipeline. Role-specific
// wrappers live in sibling producer/ and consumer/ subpackages.
package client

import (
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds the inputs every kgo.Client in this process needs. The zero
// value is not usable — Brokers must be non-empty. Role-specific options
// (producer batching, consumer group + DisableAutoCommit) are layered on top
// inside NewProducerClient / NewConsumerClient.
type Config struct {
	Brokers []string

	// Producer knobs. Zero values fall back to sensible defaults.
	LingerMs      int
	BatchMaxBytes int
	Compression   string // "zstd" | "lz4" | "snappy" | "gzip" | "none"
}

// NewProducerClient returns a franz-go client configured for producing to
// ingest topics. Batching knobs come from Config so the "in batches" contract
// in the plan is not hidden in library defaults.
func NewProducerClient(cfg Config) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(lingerOf(cfg)),
		kgo.ProducerBatchMaxBytes(batchMaxBytesOf(cfg)),
		kgo.MaxBufferedRecords(1 << 18),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(compressionOf(cfg.Compression)),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		observability.Hooks(),
	}
	return kgo.NewClient(opts...)
}

// NewConsumerClient returns a franz-go client joined to groupID and reading
// topic. Offsets are committed manually by the consumer after each batch is
// persisted. Cooperative-sticky balancer lets partition rebalances happen
// without a full stop-the-world pause.
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
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMaxWait(2*time.Second),
		observability.Hooks(),
	)
}

func lingerOf(cfg Config) time.Duration {
	if cfg.LingerMs > 0 {
		return time.Duration(cfg.LingerMs) * time.Millisecond
	}
	return 20 * time.Millisecond
}

func batchMaxBytesOf(cfg Config) int32 {
	if cfg.BatchMaxBytes > 0 {
		return int32(cfg.BatchMaxBytes) //nolint:gosec // bounded by config
	}
	return 1 << 20 // 1 MiB
}

func compressionOf(name string) kgo.CompressionCodec {
	switch name {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd", "":
		return kgo.ZstdCompression()
	default:
		return kgo.ZstdCompression()
	}
}
