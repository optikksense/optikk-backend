package config

import (
	"fmt"
	"strings"
)

// KafkaConfig configures the Kafka-backed OTLP ingest queue (required).
type KafkaConfig struct {
	// BrokerList is a comma-separated list of host:port brokers (env-friendly: OPTIKK_KAFKA_BROKER_LIST).
	// When non-empty after parsing, it takes precedence over Brokers.
	BrokerList string `yaml:"broker_list"`
	// Brokers is the YAML list form of broker addresses.
	Brokers []string `yaml:"brokers"`
	// ConsumerGroup is the base Kafka consumer group id (default optikk-ingest); a suffix per signal is appended.
	ConsumerGroup string `yaml:"consumer_group"`
	// TopicPrefix is the topic name prefix (default optikk.ingest); full topics are {prefix}.logs|spans|metrics.
	TopicPrefix string `yaml:"topic_prefix"`

	// PartitionsPerTopic is the default partition count for ingest topics
	// (default 32). Per-signal overrides below win when non-zero.
	PartitionsPerTopic int `yaml:"partitions_per_topic"`
	LogsPartitions     int `yaml:"logs_partitions"`
	SpansPartitions    int `yaml:"spans_partitions"`
	MetricsPartitions  int `yaml:"metrics_partitions"`
	// ReplicationFactor is the topic replication factor (default 3 in prod,
	// 1 in single-broker dev). Applied uniformly to all ingest + DLQ topics.
	ReplicationFactor int `yaml:"replication_factor"`
	// Compression is the producer batch compression codec
	// ("zstd" | "lz4" | "snappy" | "gzip" | "none"). Default "zstd".
	Compression string `yaml:"compression"`
	// LingerMs is the producer linger in milliseconds (default 20).
	LingerMs int `yaml:"linger_ms"`
	// BatchMaxBytes is the producer batch max bytes (default 1 MiB).
	BatchMaxBytes int `yaml:"batch_max_bytes"`
}

func (c Config) validateKafkaIngestion() error {
	if len(c.KafkaBrokers()) == 0 {
		return fmt.Errorf("kafka: at least one broker required for OTLP ingest (set kafka.broker_list or kafka.brokers, or OPTIKK_KAFKA_BROKER_LIST)")
	}
	return nil
}

// KafkaBrokers returns broker addresses from kafka.broker_list (comma-separated) when set,
// otherwise kafka.brokers from YAML.
func (c Config) KafkaBrokers() []string {
	if s := strings.TrimSpace(c.Kafka.BrokerList); s != "" {
		var out []string
		for _, p := range strings.Split(s, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return c.Kafka.Brokers
}

// KafkaConsumerGroup returns the base consumer group id (default optikk-ingest).
func (c Config) KafkaConsumerGroup() string {
	if s := strings.TrimSpace(c.Kafka.ConsumerGroup); s != "" {
		return s
	}
	return "optikk-ingest"
}

// KafkaTopicPrefix returns the ingest topic prefix (default optikk.ingest).
func (c Config) KafkaTopicPrefix() string {
	if s := strings.TrimSpace(c.Kafka.TopicPrefix); s != "" {
		return s
	}
	return "optikk.ingest"
}

// KafkaPartitions returns the partition count for the given signal. Per-signal
// overrides win over PartitionsPerTopic; both default to 32.
func (c Config) KafkaPartitions(signal string) int {
	per := map[string]int{
		"logs":    c.Kafka.LogsPartitions,
		"spans":   c.Kafka.SpansPartitions,
		"metrics": c.Kafka.MetricsPartitions,
	}
	if n := per[signal]; n > 0 {
		return n
	}
	if n := c.Kafka.PartitionsPerTopic; n > 0 {
		return n
	}
	return 32
}

// KafkaReplicationFactor returns the topic replication factor (default 3).
func (c Config) KafkaReplicationFactor() int {
	if n := c.Kafka.ReplicationFactor; n > 0 {
		return n
	}
	return 3
}

// KafkaCompression returns the producer batch compression codec (default zstd).
func (c Config) KafkaCompression() string {
	if s := strings.TrimSpace(strings.ToLower(c.Kafka.Compression)); s != "" {
		return s
	}
	return "zstd"
}

// KafkaLingerMs returns the producer linger in milliseconds (default 20).
func (c Config) KafkaLingerMs() int {
	if n := c.Kafka.LingerMs; n > 0 {
		return n
	}
	return 20
}

// KafkaBatchMaxBytes returns the producer batch max bytes (default 1 MiB).
func (c Config) KafkaBatchMaxBytes() int {
	if n := c.Kafka.BatchMaxBytes; n > 0 {
		return n
	}
	return 1 << 20
}
