package summary

type SummaryStats struct {
	AvgLatencyMs      *float64 `json:"avg_latency_ms"`
	P95LatencyMs      *float64 `json:"p95_latency_ms"`
	SpanCount         int64    `json:"span_count"`
	ActiveConnections int64    `json:"active_connections"`
	ErrorRate         *float64 `json:"error_rate"`
	CacheHitRate      *float64 `json:"cache_hit_rate"`
}

type summaryMainDTO struct {
	P50        *float64 `ch:"p50"`
	P95        *float64 `ch:"p95"`
	TotalCount int64    `ch:"total_count"`
	ErrorCount int64    `ch:"error_count"`
}

type summaryConnDTO struct {
	UsedCount int64 `ch:"used_count"`
}

type summaryCacheDTO struct {
	SuccessCount int64 `ch:"success_count"`
	TotalCount   int64 `ch:"total_count"`
}
