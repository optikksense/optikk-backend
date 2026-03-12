package database

// ---------------------------------------------------------------------------
// Section 1 — Summary Stats
// ---------------------------------------------------------------------------

// SummaryStats holds the top-level health numbers shown in stat cards.
type SummaryStats struct {
	AvgLatencyMs      *float64 `json:"avg_latency_ms"`
	P95LatencyMs      *float64 `json:"p95_latency_ms"`
	SpanCount         int64    `json:"span_count"`
	ActiveConnections int64    `json:"active_connections"`
	ErrorRate         *float64 `json:"error_rate"` // errors / second
	CacheHitRate      *float64 `json:"cache_hit_rate"` // 0-100 %
}

// ---------------------------------------------------------------------------
// Section 2 — Detected Systems
// ---------------------------------------------------------------------------

// DetectedSystem describes one active database system.
type DetectedSystem struct {
	DBSystem      string  `json:"db_system"`
	SpanCount     int64   `json:"span_count"`
	ErrorCount    int64   `json:"error_count"`
	AvgLatencyMs  float64 `json:"avg_latency_ms"`
	QueryCount    int64   `json:"query_count"`
	ServerAddress string  `json:"server_address"`
	LastSeen      string  `json:"last_seen"`
}

// ---------------------------------------------------------------------------
// Section 3 — Query Latency
// ---------------------------------------------------------------------------

// LatencyTimeSeries is one time-bucket row of latency percentiles.
type LatencyTimeSeries struct {
	TimeBucket string   `json:"time_bucket"`
	GroupBy    string   `json:"group_by"` // value of the grouped attribute
	P50Ms      *float64 `json:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms"`
}

// LatencyHeatmapBucket is one cell in the latency heatmap (time × bucket).
type LatencyHeatmapBucket struct {
	TimeBucket  string  `json:"time_bucket"`
	BucketLabel string  `json:"bucket_label"` // e.g. "10ms", "100ms", "1s"
	Count       int64   `json:"count"`
	Density     float64 `json:"density"` // count / total for that time bucket
}

// ---------------------------------------------------------------------------
// Section 4 — Query Volume
// ---------------------------------------------------------------------------

// OpsTimeSeries is one time-bucket row of ops/sec.
type OpsTimeSeries struct {
	TimeBucket string   `json:"time_bucket"`
	GroupBy    string   `json:"group_by"`
	OpsPerSec  *float64 `json:"ops_per_sec"`
}

// ReadWritePoint is a single time-bucket point for read vs write ratio.
type ReadWritePoint struct {
	TimeBucket   string   `json:"time_bucket"`
	ReadOpsPerSec  *float64 `json:"read_ops_per_sec"`
	WriteOpsPerSec *float64 `json:"write_ops_per_sec"`
}

// ---------------------------------------------------------------------------
// Section 5 — Slow Query Analysis
// ---------------------------------------------------------------------------

// SlowQueryPattern represents an aggregated slow query pattern.
type SlowQueryPattern struct {
	QueryText      string   `json:"query_text"`
	CollectionName string   `json:"collection_name"`
	P50Ms          *float64 `json:"p50_ms"`
	P95Ms          *float64 `json:"p95_ms"`
	P99Ms          *float64 `json:"p99_ms"`
	CallCount      int64    `json:"call_count"`
	ErrorCount     int64    `json:"error_count"`
}

// SlowCollectionRow is per-collection slow query summary.
type SlowCollectionRow struct {
	CollectionName string   `json:"collection_name"`
	P99Ms          *float64 `json:"p99_ms"`
	OpsPerSec      *float64 `json:"ops_per_sec"`
	ErrorRate      *float64 `json:"error_rate"` // 0-100 %
}

// SlowRatePoint is one time-bucket count of slow queries.
type SlowRatePoint struct {
	TimeBucket string   `json:"time_bucket"`
	SlowPerSec *float64 `json:"slow_per_sec"`
}

// P99ByQueryText is a single bar in the top-10 p99 bar chart.
type P99ByQueryText struct {
	QueryText string   `json:"query_text"`
	P99Ms     *float64 `json:"p99_ms"`
}

// ---------------------------------------------------------------------------
// Section 6 — Error Rates
// ---------------------------------------------------------------------------

// ErrorTimeSeries is one time-bucket row of error rate grouped by something.
type ErrorTimeSeries struct {
	TimeBucket   string   `json:"time_bucket"`
	GroupBy      string   `json:"group_by"`
	ErrorsPerSec *float64 `json:"errors_per_sec"`
}

// ErrorRatioPoint is error / total × 100 per time bucket.
type ErrorRatioPoint struct {
	TimeBucket    string   `json:"time_bucket"`
	ErrorRatioPct *float64 `json:"error_ratio_pct"`
}

// ---------------------------------------------------------------------------
// Sections 7 & 8 — Connection Pool
// ---------------------------------------------------------------------------

// ConnectionCountPoint is used/idle connection count per pool per time bucket.
type ConnectionCountPoint struct {
	TimeBucket string   `json:"time_bucket"`
	PoolName   string   `json:"pool_name"`
	State      string   `json:"state"` // "used" | "idle"
	Count      *float64 `json:"count"`
}

// ConnectionUtilPoint is used/max % per pool per time bucket.
type ConnectionUtilPoint struct {
	TimeBucket  string   `json:"time_bucket"`
	PoolName    string   `json:"pool_name"`
	UtilPct     *float64 `json:"util_pct"`
}

// ConnectionLimits holds the configured bounds for a pool.
type ConnectionLimits struct {
	PoolName string   `json:"pool_name"`
	Max      *float64 `json:"max"`
	IdleMax  *float64 `json:"idle_max"`
	IdleMin  *float64 `json:"idle_min"`
}

// PendingRequestsPoint is pending connection request count per pool per bucket.
type PendingRequestsPoint struct {
	TimeBucket string   `json:"time_bucket"`
	PoolName   string   `json:"pool_name"`
	Count      *float64 `json:"count"`
}

// ConnectionTimeoutPoint is timeouts/sec per pool per time bucket.
type ConnectionTimeoutPoint struct {
	TimeBucket  string   `json:"time_bucket"`
	PoolName    string   `json:"pool_name"`
	TimeoutRate *float64 `json:"timeout_rate"`
}

// PoolLatencyPoint is one row of pool latency percentiles.
type PoolLatencyPoint struct {
	TimeBucket string   `json:"time_bucket"`
	PoolName   string   `json:"pool_name"`
	P50Ms      *float64 `json:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms"`
}

// ---------------------------------------------------------------------------
// Sections 9 & 10 — Deep Dives
// ---------------------------------------------------------------------------

// CollectionTopQuery details one query text within a collection.
type CollectionTopQuery struct {
	QueryText  string   `json:"query_text"`
	P99Ms      *float64 `json:"p99_ms"`
	CallCount  int64    `json:"call_count"`
	ErrorCount int64    `json:"error_count"`
}

// SystemNamespace holds distinct namespace info for a system.
type SystemNamespace struct {
	Namespace string `json:"namespace"`
	SpanCount int64  `json:"span_count"`
}

// SystemCollectionRow is per-collection summary within a system.
type SystemCollectionRow struct {
	CollectionName string   `json:"collection_name"`
	P99Ms          *float64 `json:"p99_ms"`
	OpsPerSec      *float64 `json:"ops_per_sec"`
}

// Filters holds optional dimension filters applied by URL query params.
type Filters struct {
	DBSystem   []string
	Collection []string
	Namespace  []string
	Server     []string
}
