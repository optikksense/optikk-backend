package kafka

import "time"

// HTTP response DTOs (kept as the API contract — handlers + tests rely on these
// JSON shapes).

// TopicRatePoint — produce or consume rate per topic per time bucket.
type TopicRatePoint struct {
	Timestamp  string  `json:"timestamp"`
	Topic      string  `json:"topic"`
	RatePerSec float64 `json:"rate_per_sec"`
}

// TopicLatencyPoint — histogram percentiles per topic per time bucket.
type TopicLatencyPoint struct {
	Timestamp string  `json:"timestamp"`
	Topic     string  `json:"topic"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
}

// GroupRatePoint — consume or process rate per consumer group per time bucket.
type GroupRatePoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	RatePerSec    float64 `json:"rate_per_sec"`
}

// GroupLatencyPoint — histogram percentiles per consumer group per time bucket.
type GroupLatencyPoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
}

// LagPoint — consumer lag per group+topic per time bucket.
type LagPoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	Topic         string  `json:"topic"`
	Lag           float64 `json:"lag"`
}

// PartitionLag — current lag snapshot per topic/partition/group (table view).
type PartitionLag struct {
	Topic         string `json:"topic"`
	Partition     int64  `json:"partition"`
	ConsumerGroup string `json:"consumer_group"`
	Lag           int64  `json:"lag"`
}

// RebalancePoint — rebalance and heartbeat signals per consumer group per time bucket.
type RebalancePoint struct {
	Timestamp           string  `json:"timestamp"`
	ConsumerGroup       string  `json:"consumer_group"`
	RebalanceRate       float64 `json:"rebalance_rate"`
	JoinRate            float64 `json:"join_rate"`
	SyncRate            float64 `json:"sync_rate"`
	HeartbeatRate       float64 `json:"heartbeat_rate"`
	FailedHeartbeatRate float64 `json:"failed_heartbeat_rate"`
	AssignedPartitions  float64 `json:"assigned_partitions"`
}

// E2ELatencyPoint — end-to-end latency p95 for publish/receive/process per topic per bucket.
type E2ELatencyPoint struct {
	Timestamp    string  `json:"timestamp"`
	Topic        string  `json:"topic"`
	PublishP95Ms float64 `json:"publish_p95_ms"`
	ReceiveP95Ms float64 `json:"receive_p95_ms"`
	ProcessP95Ms float64 `json:"process_p95_ms"`
}

// ErrorRatePoint — error rate per error.type per time bucket, optionally per topic/group/operation.
type ErrorRatePoint struct {
	Timestamp     string  `json:"timestamp"`
	Topic         string  `json:"topic,omitempty"`
	ConsumerGroup string  `json:"consumer_group,omitempty"`
	OperationName string  `json:"operation_name,omitempty"`
	ErrorType     string  `json:"error_type"`
	ErrorRate     float64 `json:"error_rate"`
}

// BrokerConnectionPoint — open connections to a broker per time bucket.
type BrokerConnectionPoint struct {
	Timestamp   string  `json:"timestamp"`
	Broker      string  `json:"broker"`
	Connections float64 `json:"connections"`
}

// ClientOpDurationPoint — client operation duration percentiles per operation per bucket.
type ClientOpDurationPoint struct {
	Timestamp     string  `json:"timestamp"`
	OperationName string  `json:"operation_name"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
}

// KafkaSummaryStats — scalar aggregates for stat cards.
type KafkaSummaryStats struct {
	PublishRatePerSec float64 `json:"publish_rate_per_sec"`
	ReceiveRatePerSec float64 `json:"receive_rate_per_sec"`
	MaxLag            float64 `json:"max_lag"`
	PublishP95Ms      float64 `json:"publish_p95_ms"`
	ReceiveP95Ms      float64 `json:"receive_p95_ms"`
}

// Internal repository row types — one struct per SQL shape with named columns.
// Not part of the JSON contract.

// Counter time-series rows (Sum metrics).
type TopicCounterRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	Value     float64   `ch:"value"`
}

type GroupCounterRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	Value         float64   `ch:"value"`
}

// Counter-with-error_type rows.
type TopicErrorCounterRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	ErrorType string    `ch:"error_type"`
	Value     float64   `ch:"value"`
}

type GroupErrorCounterRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	ErrorType     string    `ch:"error_type"`
	Value         float64   `ch:"value"`
}

type OperationErrorCounterRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	OperationName string    `ch:"operation_name"`
	ErrorType     string    `ch:"error_type"`
	Value         float64   `ch:"value"`
}

// Histogram time-series rows. P50/P95/P99 are computed server-side via
// quantilesPrometheusHistogramMerge on metrics_1m.latency_state (seconds-domain
// for OTel kafka duration metrics — service multiplies by 1000 for ms).
type TopicHistogramRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	P50       float64   `ch:"p50"`
	P95       float64   `ch:"p95"`
	P99       float64   `ch:"p99"`
}

type GroupHistogramRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	P50           float64   `ch:"p50"`
	P95           float64   `ch:"p95"`
	P99           float64   `ch:"p99"`
}

type OperationHistogramRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	OperationName string    `ch:"operation_name"`
	P50           float64   `ch:"p50"`
	P95           float64   `ch:"p95"`
	P99           float64   `ch:"p99"`
}

// TopicMetricHistogramRow — used by e2e-latency where metric_name itself is a
// dimension and the service folds the 3 duration metrics per (timestamp, topic).
// Only p95 is consumed downstream.
type TopicMetricHistogramRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Topic      string    `ch:"topic"`
	MetricName string    `ch:"metric_name"`
	P95        float64   `ch:"p95"`
}

// HistogramAggRow — single aggregated histogram across the window.
type HistogramAggRow struct {
	SumHistSum   float64 `ch:"sum_hist_sum"`
	SumHistCount uint64  `ch:"sum_hist_count"`
	P50          float64 `ch:"p50"`
	P95          float64 `ch:"p95"`
	P99          float64 `ch:"p99"`
}

type CounterAggRow struct {
	Sum float64 `ch:"sum_value"`
}

type GaugeMaxRow struct {
	Max float64 `ch:"max_value"`
}

// Gauge time-series rows.
type GroupTopicGaugeRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	Topic         string    `ch:"topic"`
	Value         float64   `ch:"value"`
}

// PartitionLagSnapshotRow — argMax snapshot (no time-series — one row per (topic, partition, group)).
type PartitionLagSnapshotRow struct {
	Topic         string    `ch:"topic"`
	Partition     string    `ch:"partition"`
	ConsumerGroup string    `ch:"consumer_group"`
	Lag           float64   `ch:"lag"`
	Timestamp     time.Time `ch:"timestamp"`
}

type BrokerGaugeRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Broker    string    `ch:"broker"`
	Value     float64   `ch:"value"`
}

// RebalanceMetricRow — projects metric_name so service can switch on it.
type RebalanceMetricRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	MetricName    string    `ch:"metric_name"`
	Value         float64   `ch:"value"`
}

// ConsumerMetricSample — latest value per (consumer_group, node_id, metric_name)
// inside the query window. Used by saturation/database/explorer.
type ConsumerMetricSample struct {
	Timestamp     string  `ch:"timestamp"     json:"timestamp"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	NodeID        string  `ch:"node_id"        json:"node_id"`
	MetricName    string  `ch:"metric_name"    json:"metric_name"`
	Value         float64 `ch:"value"          json:"value"`
}

// TopicMetricSample — latest value per (topic, consumer_group, metric_name)
// inside the query window.
type TopicMetricSample struct {
	Timestamp     string  `ch:"timestamp"     json:"timestamp"`
	Topic         string  `ch:"topic"          json:"topic"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	MetricName    string  `ch:"metric_name"    json:"metric_name"`
	Value         float64 `ch:"value"          json:"value"`
}
