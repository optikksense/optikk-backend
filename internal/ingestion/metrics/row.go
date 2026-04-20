// Package metrics is the metric-ingest module. Files are flat: producer.go,
// consumer.go, livetail.go, handler.go, mapper.go (+ mapper_points.go), and
// row.go live side-by-side so the full pipeline for one signal is obvious.
//
// Regenerate row.pb.go after editing row.proto:
//
//	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative row.proto
package metrics

import (
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

// CHTable is the ClickHouse destination table for the metric signal.
const CHTable = "observability.metrics"

// Columns is the insert column order for CHTable. Mirrors Row's proto fields
// one-for-one so chValues can emit positional values without a lookup.
var Columns = []string{
	"team_id", "env", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "resource_fingerprint", "timestamp", "value",
	"hist_sum", "hist_count", "hist_buckets", "hist_counts", "attributes",
}

// Metric names whose samples feed sketch aggregation. Keep the literals
// aligned with the OTel semantic conventions — these exact strings drive the
// observe() switch in consumer.go.
const (
	MetricNameDbOpDuration = "db.client.operation.duration"
	MetricNameKafkaPublish = "messaging.kafka.publish.latency"
	MetricNameKafkaConsume = "messaging.kafka.consume.latency"
	MetricNameKafkaProduce = "messaging.kafka.producer.latency"
	MetricNameHttpServer   = "http.server.request.duration"
	MetricNameHttpClient   = "http.client.request.duration"
)

// DbOpDim / KafkaTopicDim / HttpServerDim / HttpClientDim / JvmMetricDim /
// DbQueryDim all delegate to sketch.Dim* so ingest and query paths share one
// source of truth for dim construction.

func DbOpDim(r *Row) string {
	a := r.GetAttributes()
	return sketch.DimDbOp(
		firstNonEmpty(a, "db.system", "db.system.name"),
		firstNonEmpty(a, "db.operation.name", "db.operation"),
		firstNonEmpty(a, "db.collection.name", "db.sql.table", "db.mongodb.collection"),
		firstNonEmpty(a, "db.namespace", "db.name"),
	)
}

func KafkaTopicDim(r *Row) string {
	a := r.GetAttributes()
	return sketch.DimKafkaTopic(
		firstNonEmpty(a, "messaging.destination.name", "messaging.kafka.topic"),
		firstNonEmpty(a, "messaging.client.id", "messaging.kafka.client_id"),
	)
}

func HttpServerDim(r *Row) string {
	a := r.GetAttributes()
	return sketch.DimHttpServer(
		firstNonEmpty(a, "otel.scope.name", "scope.name"),
		firstNonEmpty(a, "http.route", "url.path"),
		firstNonEmpty(a, "http.request.method", "http.method"),
		firstNonEmpty(a, "http.response.status_code", "http.status_code"),
		firstNonEmpty(a, "server.address", "net.host.name"),
	)
}

func HttpClientDim(r *Row) string {
	a := r.GetAttributes()
	return sketch.DimHttpClient(
		firstNonEmpty(a, "otel.scope.name", "scope.name"),
		firstNonEmpty(a, "server.address", "net.peer.name"),
		firstNonEmpty(a, "http.request.method", "http.method"),
		firstNonEmpty(a, "http.response.status_code", "http.status_code"),
	)
}

func JvmMetricDim(r *Row) string {
	a := r.GetAttributes()
	return sketch.DimJvmMetric(
		r.GetMetricName(),
		firstNonEmpty(a, "service.name"),
		firstNonEmpty(a, "k8s.pod.name"),
	)
}

// DbQueryDim is used only when db.query.text.fingerprint is present;
// slowqueries-style views rely on that high-card dim.
func DbQueryDim(r *Row) string {
	a := r.GetAttributes()
	return sketch.DimDbQuery(
		firstNonEmpty(a, "db.system", "db.system.name"),
		firstNonEmpty(a, "db.query.text.fingerprint"),
	)
}

// IsJvmMetric is the predicate for the JvmMetricLatency sketch kind.
func IsJvmMetric(metricName string) bool {
	return strings.HasPrefix(metricName, "jvm.")
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
