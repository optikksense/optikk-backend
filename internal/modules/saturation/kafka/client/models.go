package client

import "time"

// HTTP response DTOs.

// E2ELatencyPoint — end-to-end latency p95 for publish/receive/process per
// topic per bucket.
type E2ELatencyPoint struct {
	Timestamp    string  `json:"timestamp"`
	Topic        string  `json:"topic"`
	PublishP95Ms float64 `json:"publish_p95_ms"`
	ReceiveP95Ms float64 `json:"receive_p95_ms"`
	ProcessP95Ms float64 `json:"process_p95_ms"`
}

// Internal repository row types.

// TopicMetricHistogramRow — used by e2e-latency where metric_name itself is a
// dimension. Service folds the 3 duration metrics per (timestamp, topic).
type TopicMetricHistogramRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Topic      string    `ch:"topic"`
	MetricName string    `ch:"metric_name"`
	P95        float64   `ch:"p95"`
}
