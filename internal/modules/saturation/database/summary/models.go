package summary

type SummaryStats struct {
	AvgLatencyMs      *float64 `json:"avg_latency_ms"`
	P95LatencyMs      *float64 `json:"p95_latency_ms"`
	P99LatencyMs      *float64 `json:"p99_latency_ms"`
	SpanCount         int64    `json:"span_count"`
	ActiveConnections int64    `json:"active_connections"`
	ErrorRate         *float64 `json:"error_rate"`
	CacheHitRate      *float64 `json:"cache_hit_rate"`
}
