package kafka

import (
	"fmt"
	"strings"

	database "github.com/observability/observability-backend-go/internal/database"
)

const (
	DefaultUnknown       = "unknown"
	MessagingSystemKafka = "kafka"

	// Column names in the metrics table.
	ColMetricName  = "metric_name"
	ColServiceName = "service"
	ColCount       = "hist_count"
	ColTeamID      = "team_id"
	ColTimestamp   = "timestamp"
	ColValue       = "value"

	MaxTopQueues = 50
)

var (
	ProducerMetrics = []string{
		MetricPublishMessages,
		MetricClientSentMessages,
		"kafka.producer.message.count",
		"messaging.client.published.messages",
	}

	ConsumerMetrics = []string{
		MetricReceiveMessages,
		MetricClientReceivedMessages,
		"kafka.consumer.message.count",
		"messaging.client.consumed.messages",
	}

	ProcessMetrics = []string{
		MetricProcessMessages,
	}

	ConsumerLagMetrics = []string{
		MetricKafkaConsumerLag,
		MetricKafkaConsumerLagSum,
		"kafka.consumer.lag",
		"queue.depth",
	}

	RebalanceMetrics = []string{
		MetricRebalanceCount,
		MetricJoinCount,
		MetricSyncCount,
		MetricHeartbeatCount,
		MetricFailedHeartbeatCount,
		MetricAssignedPartitions,
	}

	DurationMetrics = []string{
		MetricPublishDuration,
		MetricReceiveDuration,
		MetricProcessDuration,
		MetricClientOperationDuration,
	}

	AllKafkaMetrics = flatten(
		ProducerMetrics,
		ConsumerMetrics,
		ProcessMetrics,
		ConsumerLagMetrics,
		RebalanceMetrics,
		DurationMetrics,
		[]string{MetricClientConnections},
	)
)

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func MetricSetToInClause(metrics []string) string {
	res := ""
	for i, metric := range metrics {
		if i > 0 {
			res += ", "
		}
		res += "'" + metric + "'"
	}
	return res
}

func flatten(slices ...[]string) []string {
	var out []string
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

func attrStringAny(attrNames ...string) string {
	if len(attrNames) == 0 {
		return "''"
	}

	parts := make([]string, 0, len(attrNames))
	for _, attrName := range attrNames {
		parts = append(parts, fmt.Sprintf("nullIf(attributes.'%s'::String, '')", attrName))
	}
	return fmt.Sprintf("coalesce(%s, '')", strings.Join(parts, ", "))
}

func topicExpr() string {
	return attrStringAny(AttrMessagingDestinationName, "messaging.destination")
}

func consumerGroupExpr() string {
	return attrStringAny(AttrMessagingConsumerGroupName, "messaging.kafka.consumer.group")
}

func operationExpr() string {
	return attrStringAny(AttrMessagingOperationName, "messaging.operation")
}

func publishDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN ('publish', 'produce', 'send')))",
		ColMetricName, MetricPublishDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

func receiveDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN ('receive', 'consume')))",
		ColMetricName, MetricReceiveDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

func processDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) = 'process'))",
		ColMetricName, MetricProcessDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

// KafkaFilters holds optional filter query params for detail pages.
type KafkaFilters struct {
	Topic string
	Group string
}

// kafkaFilterClauses builds optional WHERE clauses from KafkaFilters.
func kafkaFilterClauses(f KafkaFilters) (string, []any) {
	var sb strings.Builder
	var args []any
	if f.Topic != "" {
		sb.WriteString(fmt.Sprintf(" AND %s = ?", topicExpr()))
		args = append(args, f.Topic)
	}
	if f.Group != "" {
		sb.WriteString(fmt.Sprintf(" AND %s = ?", consumerGroupExpr()))
		args = append(args, f.Group)
	}
	return sb.String(), args
}
