package kafka

import "time"

// Public response types (exposed via handler JSON envelopes) use int64 for
// counts and time.Time-like strings for timestamps. Repository-layer DTOs
// below mirror these shapes but carry CH-native types (uint64 counts, raw
// sum/count pairs, empty percentile slots filled by the service from sketch).

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

// ErrorRatePoint — error rate per error.type per time bucket, optionally per topic/group.
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

type ConsumerMetricSample struct {
	Timestamp     string  `json:"timestamp"`
	ConsumerGroup string  `json:"consumer_group"`
	NodeID        string  `json:"node_id"`
	MetricName    string  `json:"metric_name"`
	Value         float64 `json:"value"`
}

type TopicMetricSample struct {
	Timestamp     string  `json:"timestamp"`
	Topic         string  `json:"topic"`
	ConsumerGroup string  `json:"consumer_group"`
	MetricName    string  `json:"metric_name"`
	Value         float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Repository DTOs (CH-native types). Service maps these into the public shapes
// above, computing rates / averages / percentiles in Go.
// ---------------------------------------------------------------------------

// topicValueSumRow carries raw sum(value) per (time_bucket, topic). Service
// divides by bucket width (seconds) to get rate_per_sec.
type topicValueSumRow struct {
	TimeBucket time.Time `ch:"time_bucket"`
	Topic      string    `ch:"topic"`
	ValueSum   float64   `ch:"value_sum"`
}

// groupValueSumRow — same shape for consumer-group grouping.
type groupValueSumRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	ConsumerGroup string    `ch:"consumer_group"`
	ValueSum      float64   `ch:"value_sum"`
}

// topicKeyRow — unique (time_bucket, topic) tuples used to seed the
// time-series shape; percentiles attach from the sketch in the service.
type topicKeyRow struct {
	TimeBucket time.Time `ch:"time_bucket"`
	Topic      string    `ch:"topic"`
}

// groupKeyRow — same for consumer groups.
type groupKeyRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	ConsumerGroup string    `ch:"consumer_group"`
}

// groupTopicLagRow carries raw sum(value)+count() lag per (time_bucket,
// consumer_group, topic). Service computes sum/count for avg.
type groupTopicLagRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	ConsumerGroup string    `ch:"consumer_group"`
	Topic         string    `ch:"topic"`
	ValueSum      float64   `ch:"value_sum"`
	ValueCount    uint64    `ch:"value_count"`
}

// partitionLagRawRow — raw sum+count per (topic, partition, consumer_group).
// Partition is a string attribute; service parses it to int64.
type partitionLagRawRow struct {
	Topic         string  `ch:"topic"`
	Partition     string  `ch:"partition"`
	ConsumerGroup string  `ch:"consumer_group"`
	ValueSum      float64 `ch:"value_sum"`
	ValueCount    uint64  `ch:"value_count"`
}

// rebalanceMetricRow — raw count sum per (time_bucket, consumer_group,
// metric_name). Service pivots metric_name into the six rebalance fields.
type rebalanceMetricRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	ConsumerGroup string    `ch:"consumer_group"`
	MetricName    string    `ch:"metric_name"`
	ValueSum      float64   `ch:"value_sum"`
	ValueCount    uint64    `ch:"value_count"`
}

// errorRateRawRow — raw error value sum by error_type and optional axis
// (topic / consumer_group / operation_name).
type errorRateRawRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	Topic         string    `ch:"topic"`
	ConsumerGroup string    `ch:"consumer_group"`
	OperationName string    `ch:"operation_name"`
	ErrorType     string    `ch:"error_type"`
	ValueSum      float64   `ch:"value_sum"`
}

// brokerConnectionRawRow — raw sum+count per (time_bucket, broker). Service
// computes sum/count for avg(connections).
type brokerConnectionRawRow struct {
	TimeBucket time.Time `ch:"time_bucket"`
	Broker     string    `ch:"broker"`
	ValueSum   float64   `ch:"value_sum"`
	ValueCount uint64    `ch:"value_count"`
}

// operationKeyRow — unique (time_bucket, operation_name) tuples. Percentiles
// for client operation duration currently stay zero (no matching sketch
// dimension); the row shape keeps the charting surface stable.
type operationKeyRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	OperationName string    `ch:"operation_name"`
}

// consumerMetricSampleRow — raw sum+count per (time_bucket, consumer_group,
// node_id, metric_name). Service divides to produce avg(value).
type consumerMetricSampleRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	ConsumerGroup string    `ch:"consumer_group"`
	NodeID        string    `ch:"node_id"`
	MetricName    string    `ch:"metric_name"`
	ValueSum      float64   `ch:"value_sum"`
	ValueCount    uint64    `ch:"value_count"`
}

// topicMetricSampleRow — raw sum+count per (time_bucket, topic,
// consumer_group, metric_name).
type topicMetricSampleRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	Topic         string    `ch:"topic"`
	ConsumerGroup string    `ch:"consumer_group"`
	MetricName    string    `ch:"metric_name"`
	ValueSum      float64   `ch:"value_sum"`
	ValueCount    uint64    `ch:"value_count"`
}

// summaryRateRow — raw sums per metric family for the summary card. Service
// divides by durationSecs for publish/receive rates.
type summaryRateRow struct {
	MetricFamily string  `ch:"metric_family"`
	ValueSum     float64 `ch:"value_sum"`
}

// summaryMaxLagRow — scalar max lag across the window.
type summaryMaxLagRow struct {
	MaxLag float64 `ch:"max_lag"`
}
