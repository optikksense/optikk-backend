package database

// DatabaseQueryByTable represents the query volume and latency per table.
type DatabaseQueryByTable struct {
	Timestamp    string  `json:"timestamp"`
	System       string  `json:"system"`
	Table        string  `json:"table"`
	QueryCount   int64   `json:"query_count"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
}

// DatabaseAvgLatency represents the DB latency over time.
type DatabaseAvgLatency struct {
	Timestamp    string  `json:"timestamp"`
	System       string  `json:"system"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
}

// DbCacheSummary represents macro-level database metrics (latency & cache hits).
type DbCacheSummary struct {
	AvgLatencyMs        float64           `json:"avg_latency_ms"`
	TotalQueries        int64             `json:"total_queries"`
	AvgCacheHitRatio    float64           `json:"avg_cache_hit_ratio"`
	TotalCacheMisses    int64             `json:"total_cache_misses"`
	LatencyTimeseries   []DbLatencyBucket `json:"latency_timeseries"`
	CacheMissTimeseries []DbCacheBucket   `json:"cache_miss_timeseries"`
	Timestamp           string            `json:"timestamp"`
	AvgQueryLatencyMs   float64           `json:"avg_query_latency_ms"`
	P95QueryLatencyMs   float64           `json:"p95_query_latency_ms"`
	DbSpanCount         int64             `json:"db_span_count"`
	CacheHits           int64             `json:"cache_hits"`
	CacheMisses         int64             `json:"cache_misses"`
	AvgReplicationLagMs float64           `json:"avg_replication_lag_ms"`
}
type DbLatencyBucket struct {
	Timestamp string  `json:"timestamp"`
	LatencyMs float64 `json:"latency_ms"`
}

type DbCacheBucket struct {
	Timestamp   string `json:"timestamp"`
	CacheMisses int64  `json:"cache_misses"`
}

// DbSystemBreakdown represents usage breakdowns by database system (e.g. mysql vs redis).
type DbSystemBreakdown struct {
	Timestamp       string  `json:"timestamp"`
	DbSystem        string  `json:"db_system"`
	QueryCount      int64   `json:"query_count"`
	AvgLatency      float64 `json:"avg_latency"`
	P95QueryLatency float64 `json:"p95_query_latency"`
}

// DbTableMetric aggregates metrics for a specific database table.
type DbTableMetric struct {
	TableName         string  `json:"table_name"`
	SystemName        string  `json:"system_name"`
	DatabaseName      string  `json:"database_name"`
	TotalQueries      int64   `json:"total_queries"`
	AvgQueryLatencyMs float64 `json:"avg_query_latency_ms"`
	MaxQueryLatencyMs float64 `json:"max_query_latency_ms"`
	P95QueryLatencyMs float64 `json:"p95_query_latency_ms"`
	QueryCount        int64   `json:"query_count"`
	TotalErrors       int64   `json:"total_errors"`
	ActiveRequests    int64   `json:"active_requests"`
	CacheHits         int64   `json:"cache_hits"`
	CacheMisses       int64   `json:"cache_misses"`
}

// HistogramSummary holds p50/p95/p99/avg for histogram metrics.
type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

// ConnectionStatValue represents a connection count grouped by state.
type ConnectionStatValue struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

// RedisHitRate holds cache hit/miss counts and the computed hit rate.
type RedisHitRate struct {
	Hits       int64   `json:"hits"`
	Misses     int64   `json:"misses"`
	HitRatePct float64 `json:"hit_rate_pct"`
}

// RedisTimeSeries is a simple scalar timeseries point.
type RedisTimeSeries struct {
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

// RedisDBKeyStat holds per-DB key/expiry counts.
type RedisDBKeyStat struct {
	DB   string `json:"db"`
	Keys int64  `json:"keys"`
}

// RedisReplicationLag holds replication offset stats.
type RedisReplicationLag struct {
	Offset        int64 `json:"offset"`
	BacklogOffset int64 `json:"backlog_offset"`
}
