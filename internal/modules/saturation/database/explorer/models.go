package explorer

type DatastoreSummaryResponse struct {
	TotalSystems      int     `json:"total_systems"`
	DatabaseSystems   int     `json:"database_systems"`
	RedisSystems      int     `json:"redis_systems"`
	QueryCount        int64   `json:"query_count"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	ErrorRate         float64 `json:"error_rate"`
	ActiveConnections int64   `json:"active_connections"`
}

type DatastoreSystemRow struct {
	System            string  `json:"system"`
	Category          string  `json:"category"`
	QueryCount        int64   `json:"query_count"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	ErrorRate         float64 `json:"error_rate"`
	ActiveConnections int64   `json:"active_connections"`
	ServerHint        string  `json:"server_hint"`
	LastSeen          string  `json:"last_seen"`
}

type DatastoreCollectionSpotlight struct {
	CollectionName string  `json:"collection_name"`
	P99Ms          float64 `json:"p99_ms"`
	OpsPerSec      float64 `json:"ops_per_sec"`
}

type DatastoreSystemOverview struct {
	System            string                         `json:"system"`
	Category          string                         `json:"category"`
	QueryCount        int64                          `json:"query_count"`
	ErrorRate         float64                        `json:"error_rate"`
	AvgLatencyMs      float64                        `json:"avg_latency_ms"`
	P95LatencyMs      float64                        `json:"p95_latency_ms"`
	P99LatencyMs      float64                        `json:"p99_latency_ms"`
	ActiveConnections int64                          `json:"active_connections"`
	CacheHitRate      *float64                       `json:"cache_hit_rate,omitempty"`
	TopServer         string                         `json:"top_server"`
	NamespaceCount    int                            `json:"namespace_count"`
	CollectionCount   int                            `json:"collection_count"`
	ReadOpsPerSec     float64                        `json:"read_ops_per_sec"`
	WriteOpsPerSec    float64                        `json:"write_ops_per_sec"`
	TopCollections    []DatastoreCollectionSpotlight `json:"top_collections"`
}

type DatastoreServerRow struct {
	Server string  `json:"server"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
}

type DatastoreNamespaceRow struct {
	Namespace string `json:"namespace"`
	SpanCount int64  `json:"span_count"`
}

type DatastoreOperationRow struct {
	Operation    string  `json:"operation"`
	OpsPerSec    float64 `json:"ops_per_sec"`
	P50Ms        float64 `json:"p50_ms"`
	P95Ms        float64 `json:"p95_ms"`
	P99Ms        float64 `json:"p99_ms"`
	ErrorsPerSec float64 `json:"errors_per_sec"`
}

type DatastoreErrorRow struct {
	ErrorType    string  `json:"error_type"`
	ErrorsPerSec float64 `json:"errors_per_sec"`
}

type DatastoreConnectionRow struct {
	PoolName        string  `json:"pool_name"`
	UsedConnections float64 `json:"used_connections"`
	UtilPct         float64 `json:"util_pct"`
	PendingRequests float64 `json:"pending_requests"`
	TimeoutRate     float64 `json:"timeout_rate"`
	P95WaitMs       float64 `json:"p95_wait_ms"`
	MaxConnections  float64 `json:"max_connections"`
	IdleMax         float64 `json:"idle_max"`
	IdleMin         float64 `json:"idle_min"`
}

