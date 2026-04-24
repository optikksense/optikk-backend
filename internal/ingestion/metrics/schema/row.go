// Package schema holds the metrics signal's Kafka wire format and ClickHouse
// column mapping. Row is the protobuf message produced on the ingest topic
// and consumed by the dispatcher; CHTable/Columns/ChValues drive the batch
// insert into observability.metrics.
//
//go:generate protoc --go_out=. --go_opt=paths=source_relative metric_row.proto
package schema

import "time"

// CHTable is the ClickHouse destination table for the metric signal.
const CHTable = "observability.metrics"

// Columns is the insert column order for CHTable. Mirrors Row's proto fields
// one-for-one so ChValues can emit positional values without a lookup.
var Columns = []string{
	"team_id", "env", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "resource_fingerprint", "timestamp", "value",
	"hist_sum", "hist_count", "hist_buckets", "hist_counts", "attributes",
}

// Metric names whose samples feed sketch aggregation.
const (
	MetricNameDbOpDuration = "db.client.operation.duration"
	MetricNameKafkaPublish = "messaging.kafka.publish.latency"
	MetricNameKafkaConsume = "messaging.kafka.consume.latency"
	MetricNameKafkaProduce = "messaging.kafka.producer.latency"
)

// DbOpDim composes the dimension tuple used by DbOpLatency sketches:
// db_system | operation | collection | namespace. Empty segments are preserved.
func DbOpDim(r *Row) string {
	a := r.GetAttributes()
	return firstNonEmpty(a, "db.system", "db.system.name") + "|" +
		firstNonEmpty(a, "db.operation.name", "db.operation") + "|" +
		firstNonEmpty(a, "db.collection.name", "db.sql.table", "db.mongodb.collection") + "|" +
		firstNonEmpty(a, "db.namespace", "db.name")
}

// KafkaTopicDim composes the dimension tuple used by KafkaTopicLatency
// sketches: topic | client_id.
func KafkaTopicDim(r *Row) string {
	a := r.GetAttributes()
	return firstNonEmpty(a, "messaging.destination.name", "messaging.kafka.topic") + "|" +
		firstNonEmpty(a, "messaging.client.id", "messaging.kafka.client_id")
}

func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

// ChValues returns positional values aligned with Columns for CH batch insert.
func ChValues(r *Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetEnv(),
		r.GetMetricName(),
		r.GetMetricType(),
		r.GetTemporality(),
		r.GetIsMonotonic(),
		r.GetUnit(),
		r.GetDescription(),
		r.GetResourceFingerprint(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetValue(),
		r.GetHistSum(),
		r.GetHistCount(),
		r.GetHistBuckets(),
		r.GetHistCounts(),
		r.GetAttributes(),
	}
}
