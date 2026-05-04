package client

import "time"

// HTTP response DTOs.

// KafkaSummaryStats — scalar aggregates for stat cards.
type KafkaSummaryStats struct {
	PublishRatePerSec float64 `json:"publish_rate_per_sec"`
	ReceiveRatePerSec float64 `json:"receive_rate_per_sec"`
	MaxLag            float64 `json:"max_lag"`
	PublishP95Ms      float64 `json:"publish_p95_ms"`
	ReceiveP95Ms      float64 `json:"receive_p95_ms"`
}

// E2ELatencyPoint — end-to-end latency p95 for publish/receive/process per
// topic per bucket.
type E2ELatencyPoint struct {
	Timestamp    string  `json:"timestamp"`
	Topic        string  `json:"topic"`
	PublishP95Ms float64 `json:"publish_p95_ms"`
	ReceiveP95Ms float64 `json:"receive_p95_ms"`
	ProcessP95Ms float64 `json:"process_p95_ms"`
}

// BrokerConnectionPoint — open connections to a broker per time bucket.
type BrokerConnectionPoint struct {
	Timestamp   string  `json:"timestamp"`
	Broker      string  `json:"broker"`
	Connections float64 `json:"connections"`
}

// ClientOpDurationPoint — client operation duration percentiles per operation
// per bucket.
type ClientOpDurationPoint struct {
	Timestamp     string  `json:"timestamp"`
	OperationName string  `json:"operation_name"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
}

// ErrorRatePoint — client-op error rate per (operation_name, error_type) per
// bucket. Same JSON shape as the consumer/producer ErrorRatePoint (operation_name
// instead of consumer_group/topic).
type ErrorRatePoint struct {
	Timestamp     string  `json:"timestamp"`
	OperationName string  `json:"operation_name"`
	ErrorType     string  `json:"error_type"`
	ErrorRate     float64 `json:"error_rate"`
}

// Internal repository row types.

type CounterAggRow struct {
	Sum float64 `ch:"sum_value"`
}

type GaugeMaxRow struct {
	Max float64 `ch:"max_value"`
}

// HistogramAggRow — single aggregated histogram across the window.
type HistogramAggRow struct {
	SumHistSum   float64 `ch:"sum_hist_sum"`
	SumHistCount uint64  `ch:"sum_hist_count"`
	P50          float64 `ch:"p50"`
	P95          float64 `ch:"p95"`
	P99          float64 `ch:"p99"`
}

// TopicMetricHistogramRow — used by e2e-latency where metric_name itself is a
// dimension. Service folds the 3 duration metrics per (timestamp, topic).
type TopicMetricHistogramRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Topic      string    `ch:"topic"`
	MetricName string    `ch:"metric_name"`
	P95        float64   `ch:"p95"`
}

type BrokerGaugeRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Broker    string    `ch:"broker"`
	Value     float64   `ch:"value"`
}

type OperationHistogramRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	OperationName string    `ch:"operation_name"`
	P50           float64   `ch:"p50"`
	P95           float64   `ch:"p95"`
	P99           float64   `ch:"p99"`
}

type OperationErrorCounterRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	OperationName string    `ch:"operation_name"`
	ErrorType     string    `ch:"error_type"`
	Value         float64   `ch:"value"`
}
