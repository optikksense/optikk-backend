package consumer

import "time"

// HTTP response DTOs.

// TopicRatePoint — consume rate per topic per time bucket.
type TopicRatePoint struct {
	Timestamp  string  `json:"timestamp"`
	Topic      string  `json:"topic"`
	RatePerSec float64 `json:"rate_per_sec"`
}

// TopicLatencyPoint — receive histogram percentiles per topic per bucket.
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

// GroupLatencyPoint — process histogram percentiles per consumer group per bucket.
type GroupLatencyPoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
}

// ErrorRatePoint — consume / process error rate per (consumer_group, error_type) per bucket.
type ErrorRatePoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	ErrorType     string  `json:"error_type"`
	ErrorRate     float64 `json:"error_rate"`
}

// LagPoint — consumer lag per group+topic per time bucket.
type LagPoint struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	Topic         string  `json:"topic"`
	Lag           float64 `json:"lag"`
}

// PartitionLag — current lag snapshot per topic/partition/group (table view).
// Exported for the explorer/ composer.
type PartitionLag struct {
	Topic         string `json:"topic"`
	Partition     int64  `json:"partition"`
	ConsumerGroup string `json:"consumer_group"`
	Lag           int64  `json:"lag"`
}

// RebalancePoint — rebalance and heartbeat signals per consumer group per bucket.
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

// Internal repository row types.

type TopicCounterRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	Value     float64   `ch:"value"`
}

type TopicHistogramRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	P50       float64   `ch:"p50"`
	P95       float64   `ch:"p95"`
	P99       float64   `ch:"p99"`
}

type GroupCounterRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	Value         float64   `ch:"value"`
}

type GroupHistogramRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	P50           float64   `ch:"p50"`
	P95           float64   `ch:"p95"`
	P99           float64   `ch:"p99"`
}

type GroupErrorCounterRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	ErrorType     string    `ch:"error_type"`
	Value         float64   `ch:"value"`
}

type GroupTopicGaugeRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	Topic         string    `ch:"topic"`
	Value         float64   `ch:"value"`
}

// PartitionLagSnapshotRow — argMax snapshot (no time-series — one row per
// (topic, partition, group)).
type PartitionLagSnapshotRow struct {
	Topic         string    `ch:"topic"`
	Partition     string    `ch:"partition"`
	ConsumerGroup string    `ch:"consumer_group"`
	Lag           float64   `ch:"lag"`
	Timestamp     time.Time `ch:"timestamp"`
}

// RebalanceMetricRow — projects metric_name so service can switch on it.
type RebalanceMetricRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	ConsumerGroup string    `ch:"consumer_group"`
	MetricName    string    `ch:"metric_name"`
	Value         float64   `ch:"value"`
}
