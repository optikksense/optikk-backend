package servicepage

// serviceHealthRow is the scan target for GetServiceHealth.
// HealthStatus is computed in the service layer from ErrorRate.
type serviceHealthRow struct {
	ServiceName  string  `ch:"service_name"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	ErrorRate    float64 `ch:"error_rate"`
	P95LatencyMs float64 `ch:"p95_latency"`
}

// serviceCountRow is the scan target for scalar COUNT queries.
type serviceCountRow struct {
	Count int64 `ch:"count"`
}

type serviceMetricDTO = ServiceMetric
type endpointMetricDTO = EndpointMetric
type timeSeriesPointDTO = TimeSeriesPoint
