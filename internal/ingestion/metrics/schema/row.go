// Package schema holds the metrics signal's Kafka wire format and ClickHouse column mapping.
//
//go:generate protoc --go_out=. --go_opt=paths=source_relative metric_row.proto
package schema

import "time"

const CHTable = "observability.metrics"

var Columns = []string{
	"team_id", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "fingerprint", "timestamp",
	"ts_bucket_hour",
	"value", "hist_sum", "hist_count", "hist_buckets", "hist_counts",
	"service", "host", "environment", "k8s_namespace", "http_method", "http_status_code",
	"resource", "attributes",
}

const (
	MetricNameDbOpDuration = "db.client.operation.duration"
	MetricNameKafkaPublish = "messaging.kafka.publish.latency"
	MetricNameKafkaConsume = "messaging.kafka.consume.latency"
	MetricNameKafkaProduce = "messaging.kafka.producer.latency"
)

// DbOpDim composes the dimension tuple "db_system|operation|collection|namespace" for DbOpLatency sketches.
func DbOpDim(r *Row) string {
	a := r.GetAttributes()
	return firstNonEmpty(a, "db.system", "db.system.name") + "|" +
		firstNonEmpty(a, "db.operation.name", "db.operation") + "|" +
		firstNonEmpty(a, "db.collection.name", "db.sql.table", "db.mongodb.collection") + "|" +
		firstNonEmpty(a, "db.namespace", "db.name")
}

// KafkaTopicDim composes the "topic|client_id" tuple for KafkaTopicLatency sketches.
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

// ChValues returns positional values aligned with Columns for the CH batch insert.
func ChValues(r *Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetMetricName(),
		r.GetMetricType(),
		r.GetTemporality(),
		r.GetIsMonotonic(),
		r.GetUnit(),
		r.GetDescription(),
		r.GetFingerprint(),
		time.Unix(0, r.GetTimestampNs()),
		time.Unix(r.GetTsBucketHourSeconds(), 0).UTC(),
		r.GetValue(),
		r.GetHistSum(),
		r.GetHistCount(),
		r.GetHistBuckets(),
		r.GetHistCounts(),
		r.GetService(),
		r.GetHost(),
		r.GetEnvironment(),
		r.GetK8SNamespace(),
		r.GetHttpMethod(),
		uint16(r.GetHttpStatusCode()), //nolint:gosec // status code 0..599
		r.GetResource(),
		r.GetAttributes(),
	}
}
