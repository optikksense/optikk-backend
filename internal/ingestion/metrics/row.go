// Package metrics is the metric-ingest module. Files are flat: producer.go,
// consumer.go, handler.go, mapper.go (+ mapper_points.go), and
// row.go live side-by-side so the full pipeline for one signal is obvious.
//
// Regenerate row.pb.go after editing row.proto:
//
//	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative row.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative metric_row.proto
package metrics

import "time"

// CHTable is the ClickHouse destination table for the metric signal.
const CHTable = "observability.metrics"

// Columns is the insert column order for CHTable. Mirrors Row's proto fields
// one-for-one so chValues can emit positional values without a lookup.
var Columns = []string{
	"team_id", "env", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "resource_fingerprint", "timestamp", "value",
	"hist_sum", "hist_count", "hist_buckets", "hist_counts", "attributes",
}

// Metric names whose samples feed sketch aggregation.
const (
	MetricNameDbOpDuration  = "db.client.operation.duration"
	MetricNameKafkaPublish  = "messaging.kafka.publish.latency"
	MetricNameKafkaConsume  = "messaging.kafka.consume.latency"
	MetricNameKafkaProduce  = "messaging.kafka.producer.latency"
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

// chValues returns positional values aligned with Columns for CH batch insert.
func chValues(r *Row) []any {
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
