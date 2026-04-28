// Package kafka holds the franz-go client construction, the thin Producer +
// Consumer wrappers used by every signal, and the topic-ensure helper called
// at app boot. Everything here is signal-agnostic; per-signal config (topic,
// partitions, consumer group) is declared next to each signal's producer.go /
// consumer.go.
package kafka

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds broker + producer-tuning inputs every kgo client in this
// process needs. Per-role options (consumer group, disable auto-commit, etc.)
// are layered on top inside NewProducerClient / NewConsumerClient.
type Config struct {
	Brokers       []string
	LingerMs      int    // producer linger; default 20ms
	BatchMaxBytes int    // producer batch cap; default 1 MiB
	Compression   string // "zstd" (default) | "lz4" | "snappy" | "gzip" | "none"
}

// NewProducerClient returns a kgo.Client tuned for producing. Producer-side
// batching (linger + batch_max_bytes) is the only batching layer in this
// pipeline — there is no app-side accumulator.
func NewProducerClient(cfg Config) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(linger(cfg)),
		kgo.ProducerBatchMaxBytes(batchMax(cfg)),
		kgo.MaxBufferedRecords(1<<18),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(compression(cfg.Compression)),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		WithHooks(),
	)
}

// NewConsumerClient returns a kgo.Client joined to groupID and reading topic.
// Offsets are committed manually after each polled batch is flushed to CH.
func NewConsumerClient(cfg Config, groupID, topic string) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	if groupID == "" || topic == "" {
		return nil, fmt.Errorf("kafka: group and topic required")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMaxWait(2*time.Second),
		WithHooks(),
	)
}

func linger(cfg Config) time.Duration {
	if cfg.LingerMs > 0 {
		return time.Duration(cfg.LingerMs) * time.Millisecond
	}
	return 20 * time.Millisecond
}

func batchMax(cfg Config) int32 {
	if cfg.BatchMaxBytes > 0 {
		return int32(cfg.BatchMaxBytes) //nolint:gosec // bounded by config
	}
	return 1 << 20
}

func compression(name string) kgo.CompressionCodec {
	switch name {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	default:
		return kgo.ZstdCompression()
	}
}
