package kafka

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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
	ProducerMetrics    = producerMetricAliases
	ConsumerMetrics    = consumerMetricAliases
	ProcessMetrics     = processMetricAliases
	ConsumerLagMetrics = consumerLagMetricAliases
	RebalanceMetrics   = rebalanceMetricAliases
	DurationMetrics    = durationMetricAliases

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
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func MetricSetToInClause(metrics []string) string {
	var b strings.Builder
	for i, metric := range metrics {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('\'')
		b.WriteString(metric)
		b.WriteByte('\'')
	}
	return b.String()
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
	return attrStringAny(topicAttributeAliases...)
}

func consumerGroupExpr() string {
	return attrStringAny(consumerGroupAttributeAliases...)
}

func operationExpr() string {
	return attrStringAny(operationAttributeAliases...)
}

func clientIDExpr() string {
	return attrStringAny("client-id")
}

func messagingSystemExpr() string {
	return attrStringAny(AttrMessagingSystem)
}

func topicPartitionExpr() string {
	return attrStringAny(partitionAttributeAliases...)
}

func nodeIDExpr() string {
	return attrStringAny("node-id")
}

func publishDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN (%s)))",
		ColMetricName, MetricPublishDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
		MetricSetToInClause(publishOperationAliases),
	)
}

func receiveDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN (%s)))",
		ColMetricName, MetricReceiveDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
		MetricSetToInClause(receiveOperationAliases),
	)
}

func processDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN (%s)))",
		ColMetricName, MetricProcessDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
		MetricSetToInClause(processOperationAliases),
	)
}

// KafkaFilters holds optional filter query params for detail pages.
type KafkaFilters struct {
	Topic string
	Group string
}

// kafkaFilterClauses builds optional WHERE clauses from KafkaFilters.
func kafkaFilterClauses(f KafkaFilters) (frag string, args []any) {
	var sb strings.Builder

	fmt.Fprintf(&sb, " AND lower(%s) = '%s'", messagingSystemExpr(), MessagingSystemKafka)
	if f.Topic != "" {
		fmt.Fprintf(&sb, " AND %s = @topicFilter", topicExpr())
		args = append(args, clickhouse.Named("topicFilter", f.Topic))
	}
	if f.Group != "" {
		fmt.Fprintf(&sb, " AND %s = @groupFilter", consumerGroupExpr())
		args = append(args, clickhouse.Named("groupFilter", f.Group))
	}
	return sb.String(), args
}

func kafkaInventoryFilterClauses(f KafkaFilters) (frag string, args []any) {
	var sb strings.Builder

	if f.Topic != "" {
		fmt.Fprintf(&sb, " AND %s = @topicFilter", topicExpr())
		args = append(args, clickhouse.Named("topicFilter", f.Topic))
	}
	if f.Group != "" {
		fmt.Fprintf(&sb, " AND %s = @groupFilter", consumerGroupExpr())
		args = append(args, clickhouse.Named("groupFilter", f.Group))
	}
	return sb.String(), args
}
