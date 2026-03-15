package servicepage

type ServiceHealth struct {
	ServiceName  string  `json:"service_name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	ErrorRate    float64 `json:"error_rate"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	HealthStatus string  `json:"health_status"`
}
