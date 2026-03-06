package kafka

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
)

func init() {
	dashboardconfig.RegisterDefaultConfig("queue", defaultQueue)
}

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

const defaultQueue = `page: queue
title: "Message Queue"
icon: "Layers"
subtitle: "Kafka consumer lag, message rates, operation duration, and partition offsets"

dataSources:
  - id: queue-consumer-lag
    endpoint: /v1/saturation/queue/consumer-lag
  - id: queue-topic-lag
    endpoint: /v1/saturation/queue/topic-lag
  - id: queue-top-queues
    endpoint: /v1/saturation/queue/top-queues
  - id: queue-lag-detail
    endpoint: /v1/saturation/queue/consumer-lag-detail
  - id: queue-message-rates
    endpoint: /v1/saturation/queue/message-rates
  - id: queue-operation-duration
    endpoint: /v1/saturation/queue/operation-duration
  - id: queue-offset-commit-rate
    endpoint: /v1/saturation/queue/offset-commit-rate

statCards:
  - title: "Operation p95 Duration"
    dataSource: queue-operation-duration
    valueField: p95
    formatter: ms
    icon: Clock
  - title: "Operation p99 Duration"
    dataSource: queue-operation-duration
    valueField: p99
    formatter: ms
    icon: Clock
  - title: "Published/sec"
    dataSource: queue-message-rates
    valueField: published_per_sec
    formatter: number
    icon: ArrowUp
  - title: "Consumed/sec"
    dataSource: queue-message-rates
    valueField: consumed_per_sec
    formatter: number
    icon: ArrowDown

charts:
  - id: queue-consumer-lag
    title: "Consumer Lag by Queue"
    type: area
    titleIcon: Layers
    layout:
      col: 12
    dataSource: queue-consumer-lag
    groupByKey: queue
    valueKey: avg_consumer_lag
    height: 260
  - id: queue-topic-lag
    title: "Queue Depth by Topic"
    type: area
    titleIcon: BarChart
    layout:
      col: 12
    dataSource: queue-topic-lag
    groupByKey: queue
    valueKey: avg_queue_depth
    height: 260
  - id: queue-offset-commit-rate
    title: "Consumer Offset by Topic"
    type: area
    titleIcon: Activity
    layout:
      col: 12
    dataSource: queue-offset-commit-rate
    groupByKey: topic
    valueKey: value
    height: 260
  - id: queue-lag-detail
    title: "Consumer Lag per Partition"
    type: table
    titleIcon: Table
    layout:
      col: 12
    dataSource: queue-lag-detail
    height: 320
  - id: queue-top-queues
    title: "Top Queues"
    type: table
    titleIcon: List
    layout:
      col: 24
    dataSource: queue-top-queues
    height: 320
`

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
