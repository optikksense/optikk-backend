package kafka

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
