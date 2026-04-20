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

// summaryMainDTO carries the counters used by the summary panel. Percentiles
// (p50/p95/p99) and avg latency are computed in the service layer — from the
// DbOpLatency sketch for percentiles and from LatencySum/LatencyCount for the
// avg, respectively.
type summaryMainDTO struct {
	LatencySum   float64 `ch:"latency_sum"`
	LatencyCount int64   `ch:"latency_count"`
	TotalCount   int64   `ch:"total_count"`
	ErrorCount   int64   `ch:"error_count"`
}

type summaryConnDTO struct {
	UsedSum   float64 `ch:"used_sum"`
	UsedCount int64   `ch:"used_count"`
}

type summaryCacheDTO struct {
	SuccessCount int64 `ch:"success_count"`
	TotalCount   int64 `ch:"total_count"`
}
