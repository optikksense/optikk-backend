package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
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
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN @publishOps))",
		ColMetricName, MetricPublishDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

func receiveDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN @receiveOps))",
		ColMetricName, MetricReceiveDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

func processDurationCondition() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN @processOps))",
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

func (r *ClickHouseRepository) baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", teamID),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("publishOps", publishOperationAliases),
		clickhouse.Named("receiveOps", receiveOperationAliases),
		clickhouse.Named("processOps", processOperationAliases),
		clickhouse.Named("producerMetrics", ProducerMetrics),
		clickhouse.Named("consumerMetrics", ConsumerMetrics),
		clickhouse.Named("lagMetrics", ConsumerLagMetrics),
		clickhouse.Named("rebalanceMetrics", RebalanceMetrics),
		clickhouse.Named("processMetrics", ProcessMetrics),
	}
}

// rollupBaseParams binds the shared named params every rollup-backed latency
// query needs. Scope is narrower than baseParams — rollup queries don't need
// rate/lag/rebalance metric lists (those stay on raw metrics_*).
func (r *ClickHouseRepository) rollupBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("publishDuration", MetricPublishDuration),
		clickhouse.Named("receiveDuration", MetricReceiveDuration),
		clickhouse.Named("processDuration", MetricProcessDuration),
		clickhouse.Named("opDuration", MetricClientOperationDuration),
		clickhouse.Named("publishOps", publishOperationAliases),
		clickhouse.Named("receiveOps", receiveOperationAliases),
		clickhouse.Named("processOps", processOperationAliases),
	}
}

// rollupTopicGroupFilter emits WHERE clauses against the rollup's canonical
// dims (no attribute coalescing — the rollup MV already stored the canonical
// attribute).
func rollupTopicGroupFilter(f KafkaFilters) string {
	var sb strings.Builder
	sb.WriteString(" AND lower(messaging_system) = 'kafka'")
	if f.Topic != "" {
		sb.WriteString(" AND messaging_destination = @topicFilter")
	}
	if f.Group != "" {
		sb.WriteString(" AND consumer_group = @groupFilter")
	}
	return sb.String()
}

func rollupTopicGroupArgs(f KafkaFilters) []any {
	var args []any
	if f.Topic != "" {
		args = append(args, clickhouse.Named("topicFilter", f.Topic))
	}
	if f.Group != "" {
		args = append(args, clickhouse.Named("groupFilter", f.Group))
	}
	return args
}
