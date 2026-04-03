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

type SpanAnalysisRow struct {
	SpanKind      string  `json:"span_kind"       ch:"span_kind"`
	OperationName string  `json:"operation_name"  ch:"operation_name"`
	SpanCount     int64   `json:"span_count"      ch:"span_count"`
	TotalDuration float64 `json:"total_duration"  ch:"total_duration"`
	AvgDuration   float64 `json:"avg_duration"    ch:"avg_duration"`
	P95Duration   float64 `json:"p95_duration"    ch:"p95_duration"`
	ErrorCount    int64   `json:"error_count"     ch:"error_count"`
	ErrorRate     float64 `json:"error_rate"      ch:"error_rate"`
}

type ServiceInfraMetrics struct {
	ServiceName      string  `json:"service_name"          ch:"service_name"`
	AvgCpuUtil       float64 `json:"avg_cpu_util"          ch:"avg_cpu_util"`
	AvgMemoryUtil    float64 `json:"avg_memory_util"       ch:"avg_memory_util"`
	AvgDiskUtil      float64 `json:"avg_disk_util"         ch:"avg_disk_util"`
	AvgNetworkUtil   float64 `json:"avg_network_util"      ch:"avg_network_util"`
	AvgConnPoolUtil  float64 `json:"avg_conn_pool_util"    ch:"avg_conn_pool_util"`
	SampleCount      int64   `json:"sample_count"          ch:"sample_count"`
}

type TimeSeriesPoint struct {
	Timestamp    time.Time `json:"timestamp"              ch:"timestamp"`
	ServiceName  string    `json:"service_name,omitempty" ch:"service_name"`
	RequestCount int64     `json:"request_count"          ch:"request_count"`
	ErrorCount   int64     `json:"error_count"            ch:"error_count"`
	AvgLatency   float64   `json:"avg_latency"            ch:"avg_latency"`
}
