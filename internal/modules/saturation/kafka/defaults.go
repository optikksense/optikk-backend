package kafka

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

const (
	// DefaultUnknown is used when a dimensional value cannot be extracted.
	DefaultUnknown = "unknown"

	// Output Formatting
	MessagingSystemKafka = "kafka"

	// Column names
	ColMetricName  = "metric_name"
	ColServiceName = "service"
	ColCount       = "hist_count"
	ColAvg         = "value"
	ColMax         = "value"
	ColP95         = "value"
	ColTeamID      = "team_id"
	ColTimestamp   = "timestamp"
	ColValue       = "value"

	// Limits
	MaxTopQueues = 50

	// Semantic conventions missing from standard
	AttrMessagingKafkaTopic       = "messaging.kafka.topic"
	AttrMessagingKafkaDestination = "messaging.kafka.destination"
	AttrMessagingDestination      = "messaging.destination"
	AttrKafkaTopic                = "kafka.topic"
	AttrTopic                     = "topic"
)

var (
	KafkaConsumerLagMetrics = []string{
		"kafka.consumer.lag",
	}

	KafkaConsumerLagMetricsExtended = []string{
		"kafka.consumer.lag",
	}

	KafkaProducerMetrics = []string{
		"kafka.producer.message.count",
	}

	KafkaConsumerMetrics = []string{
		"kafka.consumer.message.count",
	}

	QueueDepthMetrics = []string{
		"queue.depth",
	}

	AllQueueMetrics = []string{
		"kafka.consumer.lag",
		"kafka.producer.message.count",
		"kafka.consumer.message.count",
		"queue.depth",
	}
)

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// helper functions for repository queries
func MetricSetToInClause(metrics []string) string {
	res := ""
	for i, m := range metrics {
		if i > 0 {
			res += ", "
		}
		res += "'" + m + "'"
	}
	return res
}
