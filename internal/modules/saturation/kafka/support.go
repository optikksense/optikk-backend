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

// The attribute alias fallback that coalesce(nullIf(...), ...) once provided
// is removed here — sketches and ingest-side fallback already canonicalise
// aliases (internal/ingestion/metrics/row.go), so query-side expressions can
// stick to the canonical OpenTelemetry key and stay free of coalesce / nullIf
// / if. attrString lives in otel_conventions.go.

func topicExpr() string {
	return attrString(AttrMessagingDestinationName)
}

func consumerGroupExpr() string {
	return attrString(AttrMessagingConsumerGroupName)
}

func operationExpr() string {
	return attrString(AttrMessagingOperationName)
}

func clientIDExpr() string {
	return attrString("messaging.client.id")
}

func messagingSystemExpr() string {
	return attrString(AttrMessagingSystem)
}

func topicPartitionExpr() string {
	return attrString(AttrMessagingKafkaDestinationPartition)
}

func nodeIDExpr() string {
	return attrString("node-id")
}

// publishDurationPredicate is a plain WHERE predicate matching publish-side
// histogram rows: dedicated publish.duration OR generic client.operation.duration
// with operation_name in the publish aliases. lower() is a simple scalar
// function — not in the forbidden aggregate list — so we keep the case-insensitive
// match for operation names.
func publishDurationPredicate() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN @publishOps))",
		ColMetricName, MetricPublishDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

func receiveDurationPredicate() string {
	return fmt.Sprintf("(%s = '%s' OR (%s = '%s' AND lower(%s) IN @receiveOps))",
		ColMetricName, MetricReceiveDuration,
		ColMetricName, MetricClientOperationDuration,
		operationExpr(),
	)
}

func processDurationPredicate() string {
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

	fmt.Fprintf(&sb, " AND %s = '%s'", messagingSystemExpr(), MessagingSystemKafka)
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
