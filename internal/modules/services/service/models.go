package servicepage

import "time"

// SummaryCount is a lightweight count response for services overview cards.
type SummaryCount struct {
	Count int64 `json:"count"`
}

// ServiceMetric represents aggregate metrics for a service.
type ServiceMetric struct {
	ServiceName  string  `json:"service_name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	AvgLatency   float64 `json:"avg_latency"`
	P50Latency   float64 `json:"p50_latency"`
	P95Latency   float64 `json:"p95_latency"`
	P99Latency   float64 `json:"p99_latency"`
}

// EndpointMetric represents aggregate metrics for a single endpoint within a service.
type EndpointMetric struct {
	ServiceName   string  `json:"service_name"`
	OperationName string  `json:"operation_name"`
	HTTPMethod    string  `json:"http_method"`
	RequestCount  int64   `json:"request_count"`
	ErrorCount    int64   `json:"error_count"`
	AvgLatency    float64 `json:"avg_latency"`
	P50Latency    float64 `json:"p50_latency"`
	P95Latency    float64 `json:"p95_latency"`
	P99Latency    float64 `json:"p99_latency"`
}

// TimeSeriesPoint represents a single data point in a service time series.
type TimeSeriesPoint struct {
	Timestamp    time.Time `json:"timestamp"`
	ServiceName  string    `json:"service_name,omitempty"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	AvgLatency   float64   `json:"avg_latency"`
}
