package overview

import "time"

type RequestRatePoint struct {
	Timestamp    time.Time `json:"timestamp"`
	ServiceName  string    `json:"service_name,omitempty"`
	RequestCount int64     `json:"request_count"`
}

type ErrorRatePoint struct {
	Timestamp    time.Time `json:"timestamp"`
	ServiceName  string    `json:"service_name,omitempty"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	ErrorRate    float64   `json:"error_rate"`
}

type P95LatencyPoint struct {
	Timestamp   time.Time `json:"timestamp"`
	ServiceName string    `json:"service_name,omitempty"`
	P95         float64   `json:"p95"`
}

type ServiceMetric struct {
	ServiceName  string  `json:"service_name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	AvgLatency   float64 `json:"avg_latency"`
	P50Latency   float64 `json:"p50_latency"`
	P95Latency   float64 `json:"p95_latency"`
	P99Latency   float64 `json:"p99_latency"`
}

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

type TimeSeriesPoint struct {
	Timestamp     time.Time `json:"timestamp"`
	ServiceName   string    `json:"service_name,omitempty"`
	OperationName string    `json:"operation_name,omitempty"`
	HTTPMethod    string    `json:"http_method,omitempty"`
	RequestCount  int64     `json:"request_count"`
	ErrorCount    int64     `json:"error_count"`
	AvgLatency    float64   `json:"avg_latency"`
	P50           float64   `json:"p50,omitempty"`
	P95           float64   `json:"p95,omitempty"`
	P99           float64   `json:"p99,omitempty"`
}
