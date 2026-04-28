package config

import (
	"fmt"
	"strings"
)

// KafkaConfig configures the Kafka-backed OTLP ingest queue (required).
// Per-signal topology (partitions/replicas/retention) lives on IngestionConfig;
// this struct holds only broker connectivity + producer transport tuning.
type KafkaConfig struct {
	// BrokerList is a comma-separated list of host:port brokers (env-friendly: OPTIKK_KAFKA_BROKER_LIST).
	// When non-empty after parsing, it takes precedence over Brokers.
	BrokerList string   `yaml:"broker_list"`
	Brokers    []string `yaml:"brokers"`

	// TopicPrefix is the ingest-topic prefix (default "optikk.ingest").
	TopicPrefix string `yaml:"topic_prefix"`
	// DLQPrefix is the DLQ-topic prefix (default "optikk.dlq").
	DLQPrefix string `yaml:"dlq_prefix"`

	// Producer-side batching knobs (kgo).
	Compression   string `yaml:"compression"`     // default "zstd"
	LingerMs      int    `yaml:"linger_ms"`       // default 20
	BatchMaxBytes int    `yaml:"batch_max_bytes"` // default 1 MiB
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

// KafkaTopicPrefix returns the ingest topic prefix (default "optikk.ingest").
func (c Config) KafkaTopicPrefix() string {
	if s := strings.TrimSpace(c.Kafka.TopicPrefix); s != "" {
		return s
	}
	return "optikk.ingest"
}

// KafkaDLQPrefix returns the DLQ topic prefix (default "optikk.dlq").
func (c Config) KafkaDLQPrefix() string {
	if s := strings.TrimSpace(c.Kafka.DLQPrefix); s != "" {
		return s
	}
	return "optikk.dlq"
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
