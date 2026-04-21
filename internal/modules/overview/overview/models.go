package overview

import "time"

// BatchSummaryResponse bundles the above-the-fold Summary-tab queries (global
// summary + per-service metrics) so the client fetches them in a single request
// and the server fans out concurrently via errgroup. The three timeseries
// (request rate, error rate, p95) stay as separate endpoints so the frontend
// can defer them via IntersectionObserver until the charts scroll into view.
type BatchSummaryResponse struct {
	Summary  GlobalSummary   `json:"summary"`
	Services []ServiceMetric `json:"services"`
}

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

// ChartMetricsPoint is the per-bucket per-service row returned by
// /overview/chart-metrics. Combines the three Summary-tab chart queries
// (request rate, error rate, p95 latency) into one payload so the frontend
// replaces three round-trips and three rollup scans with one.
type ChartMetricsPoint struct {
	Timestamp    time.Time `json:"timestamp"`
	ServiceName  string    `json:"service_name,omitempty"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	ErrorRate    float64   `json:"error_rate"`
	P95          float64   `json:"p95"`
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
	EndpointName  string  `json:"endpoint_name,omitempty"`
	HTTPMethod    string  `json:"http_method"`
	RequestCount  int64   `json:"request_count"`
	ErrorCount    int64   `json:"error_count"`
	AvgLatency    float64 `json:"avg_latency"`
	P50Latency    float64 `json:"p50_latency"`
	P95Latency    float64 `json:"p95_latency"`
	P99Latency    float64 `json:"p99_latency"`
}

type GlobalSummary struct {
	TotalRequests int64   `json:"total_requests"`
	ErrorCount    int64   `json:"error_count"`
	AvgLatency    float64 `json:"avg_latency"`
	P50Latency    float64 `json:"p50_latency"`
	P95Latency    float64 `json:"p95_latency"`
	P99Latency    float64 `json:"p99_latency"`
}
