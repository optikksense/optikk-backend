package model

// SaturationMetric represents resource saturation summary per service.
type SaturationMetric struct {
	ServiceName         string  `json:"service_name"`
	SpanCount           int64   `json:"span_count"`
	AvgDurationMs       float64 `json:"avg_duration_ms"`
	P95DurationMs       float64 `json:"p95_duration_ms"`
	MaxDurationMs       float64 `json:"max_duration_ms"`
	ErrorCount          int64   `json:"error_count"`
	AvgDbPoolUtil       float64 `json:"avg_db_pool_util"`
	MaxDbPoolUtil       float64 `json:"max_db_pool_util"`
	AvgConsumerLag      float64 `json:"avg_consumer_lag"`
	MaxConsumerLag      float64 `json:"max_consumer_lag"`
	AvgThreadPoolActive float64 `json:"avg_thread_pool_active"`
	MaxThreadPoolSize   float64 `json:"max_thread_pool_size"`
	AvgQueueDepth       float64 `json:"avg_queue_depth"`
	MaxQueueDepth       float64 `json:"max_queue_depth"`
}

// SaturationTimeSeries represents saturation metrics over time per service.
type SaturationTimeSeries struct {
	ServiceName     string  `json:"service_name"`
	Timestamp       string  `json:"timestamp"`
	SpanCount       int64   `json:"span_count"`
	ErrorCount      int64   `json:"error_count"`
	AvgDurationMs   float64 `json:"avg_duration_ms"`
	AvgDbPoolUtil   float64 `json:"avg_db_pool_util"`
	AvgConsumerLag  float64 `json:"avg_consumer_lag"`
	AvgThreadActive float64 `json:"avg_thread_active"`
	AvgQueueDepth   float64 `json:"avg_queue_depth"`
}
