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
