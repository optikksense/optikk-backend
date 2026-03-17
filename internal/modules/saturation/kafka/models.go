package kafka

// TopicRatePoint — produce or consume rate per topic per time bucket.
type TopicRatePoint struct {
	Timestamp  string  `ch:"time_bucket" json:"timestamp"`
	Topic      string  `ch:"topic"        json:"topic"`
	RatePerSec float64 `ch:"rate_per_sec" json:"rate_per_sec"`
}

// TopicLatencyPoint — histogram percentiles per topic per time bucket.
type TopicLatencyPoint struct {
	Timestamp string  `ch:"time_bucket" json:"timestamp"`
	Topic     string  `ch:"topic"        json:"topic"`
	P50Ms     float64 `ch:"p50"          json:"p50_ms"`
	P95Ms     float64 `ch:"p95"          json:"p95_ms"`
	P99Ms     float64 `ch:"p99"          json:"p99_ms"`
}

// GroupRatePoint — consume or process rate per consumer group per time bucket.
type GroupRatePoint struct {
	Timestamp     string  `ch:"time_bucket"    json:"timestamp"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	RatePerSec    float64 `ch:"rate_per_sec"   json:"rate_per_sec"`
}

// GroupLatencyPoint — histogram percentiles per consumer group per time bucket.
type GroupLatencyPoint struct {
	Timestamp     string  `ch:"time_bucket"    json:"timestamp"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	P50Ms         float64 `ch:"p50"            json:"p50_ms"`
	P95Ms         float64 `ch:"p95"            json:"p95_ms"`
	P99Ms         float64 `ch:"p99"            json:"p99_ms"`
}

// LagPoint — consumer lag per group+topic per time bucket.
type LagPoint struct {
	Timestamp     string  `ch:"time_bucket"    json:"timestamp"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group"`
	Topic         string  `ch:"topic"          json:"topic"`
	Lag           float64 `ch:"lag"            json:"lag"`
}

// PartitionLag — current lag snapshot per topic/partition/group (table view).
type PartitionLag struct {
	Topic         string `ch:"topic"          json:"topic"`
	Partition     int64  `ch:"partition"      json:"partition"`
	ConsumerGroup string `ch:"consumer_group" json:"consumer_group"`
	Lag           int64  `ch:"lag"            json:"lag"`
}

// RebalancePoint — rebalance and heartbeat signals per consumer group per time bucket.
type RebalancePoint struct {
	Timestamp           string  `ch:"time_bucket"          json:"timestamp"`
	ConsumerGroup       string  `ch:"consumer_group"        json:"consumer_group"`
	RebalanceRate       float64 `ch:"rebalance_rate"        json:"rebalance_rate"`
	JoinRate            float64 `ch:"join_rate"             json:"join_rate"`
	SyncRate            float64 `ch:"sync_rate"             json:"sync_rate"`
	HeartbeatRate       float64 `ch:"heartbeat_rate"        json:"heartbeat_rate"`
	FailedHeartbeatRate float64 `ch:"failed_heartbeat_rate" json:"failed_heartbeat_rate"`
	AssignedPartitions  float64 `ch:"assigned_partitions"   json:"assigned_partitions"`
}

// E2ELatencyPoint — end-to-end latency p95 for publish/receive/process per topic per bucket.
type E2ELatencyPoint struct {
	Timestamp    string  `ch:"time_bucket"  json:"timestamp"`
	Topic        string  `ch:"topic"         json:"topic"`
	PublishP95Ms float64 `ch:"publish_p95"   json:"publish_p95_ms"`
	ReceiveP95Ms float64 `ch:"receive_p95"   json:"receive_p95_ms"`
	ProcessP95Ms float64 `ch:"process_p95"   json:"process_p95_ms"`
}

// ErrorRatePoint — error rate per error.type per time bucket, optionally per topic/group.
type ErrorRatePoint struct {
	Timestamp     string  `ch:"time_bucket"    json:"timestamp"`
	Topic         string  `ch:"topic"          json:"topic,omitempty"`
	ConsumerGroup string  `ch:"consumer_group" json:"consumer_group,omitempty"`
	OperationName string  `ch:"operation_name" json:"operation_name,omitempty"`
	ErrorType     string  `ch:"error_type"     json:"error_type"`
	ErrorRate     float64 `ch:"error_rate"     json:"error_rate"`
}

// BrokerConnectionPoint — open connections to a broker per time bucket.
type BrokerConnectionPoint struct {
	Timestamp   string  `ch:"time_bucket"  json:"timestamp"`
	Broker      string  `ch:"broker"        json:"broker"`
	Connections float64 `ch:"connections"   json:"connections"`
}

// ClientOpDurationPoint — client operation duration percentiles per operation per bucket.
type ClientOpDurationPoint struct {
	Timestamp     string  `ch:"time_bucket"    json:"timestamp"`
	OperationName string  `ch:"operation_name" json:"operation_name"`
	P50Ms         float64 `ch:"p50"            json:"p50_ms"`
	P95Ms         float64 `ch:"p95"            json:"p95_ms"`
	P99Ms         float64 `ch:"p99"            json:"p99_ms"`
}

// KafkaSummaryStats — scalar aggregates for stat cards.
type KafkaSummaryStats struct {
	PublishRatePerSec float64 `ch:"publish_rate" json:"publish_rate_per_sec"`
	ReceiveRatePerSec float64 `ch:"receive_rate" json:"receive_rate_per_sec"`
	MaxLag            float64 `ch:"max_lag"      json:"max_lag"`
	PublishP95Ms      float64 `ch:"publish_p95"  json:"publish_p95_ms"`
	ReceiveP95Ms      float64 `ch:"receive_p95"  json:"receive_p95_ms"`
}
