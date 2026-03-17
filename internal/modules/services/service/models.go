package servicepage

import "time"

type SummaryCount struct {
	Count int64 `json:"count"`
}

type ServiceMetric struct {
	ServiceName  string  `json:"service_name"  ch:"service_name"`
	RequestCount int64   `json:"request_count" ch:"request_count"`
	ErrorCount   int64   `json:"error_count"   ch:"error_count"`
	AvgLatency   float64 `json:"avg_latency"   ch:"avg_latency"`
	P50Latency   float64 `json:"p50_latency"   ch:"p50_latency"`
	P95Latency   float64 `json:"p95_latency"   ch:"p95_latency"`
	P99Latency   float64 `json:"p99_latency"   ch:"p99_latency"`
}

type EndpointMetric struct {
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	HTTPMethod    string  `json:"http_method"    ch:"http_method"`
	RequestCount  int64   `json:"request_count"  ch:"request_count"`
	ErrorCount    int64   `json:"error_count"    ch:"error_count"`
	AvgLatency    float64 `json:"avg_latency"    ch:"avg_latency"`
	P50Latency    float64 `json:"p50_latency"    ch:"p50_latency"`
	P95Latency    float64 `json:"p95_latency"    ch:"p95_latency"`
	P99Latency    float64 `json:"p99_latency"    ch:"p99_latency"`
}

type TimeSeriesPoint struct {
	Timestamp    time.Time `json:"timestamp"              ch:"timestamp"`
	ServiceName  string    `json:"service_name,omitempty" ch:"service_name"`
	RequestCount int64     `json:"request_count"          ch:"request_count"`
	ErrorCount   int64     `json:"error_count"            ch:"error_count"`
	AvgLatency   float64   `json:"avg_latency"            ch:"avg_latency"`
}
