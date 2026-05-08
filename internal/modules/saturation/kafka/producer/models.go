package producer

import "time"

// HTTP response DTOs.

// TopicRatePoint — produce rate per topic per time bucket.
type TopicRatePoint struct {
	Timestamp  string  `json:"timestamp"`
	Topic      string  `json:"topic"`
	RatePerSec float64 `json:"rate_per_sec"`
}

// TopicLatencyPoint — publish histogram percentiles per topic per time bucket.
type TopicLatencyPoint struct {
	Timestamp string  `json:"timestamp"`
	Topic     string  `json:"topic"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
}

// ErrorRatePoint — publish error rate per (topic, error_type) per bucket.
type ErrorRatePoint struct {
	Timestamp string  `json:"timestamp"`
	Topic     string  `json:"topic"`
	ErrorType string  `json:"error_type"`
	ErrorRate float64 `json:"error_rate"`
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

type TopicErrorCounterRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Topic     string    `ch:"topic"`
	ErrorType string    `ch:"error_type"`
	Value     float64   `ch:"value"`
}
