package kafka

import dbutil "github.com/observability/observability-backend-go/internal/database"

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
	}

	ConsumerMetrics = []string{
		MetricReceiveMessages,
		MetricClientReceivedMessages,
	}

	ProcessMetrics = []string{
		MetricProcessMessages,
	}

	ConsumerLagMetrics = []string{
		MetricKafkaConsumerLag,
		MetricKafkaConsumerLagSum,
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
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
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
