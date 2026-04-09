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

type KafkaSummaryResponse struct {
	PublishRatePerSec float64 `json:"publish_rate_per_sec"`
	ReceiveRatePerSec float64 `json:"receive_rate_per_sec"`
	MaxLag            float64 `json:"max_lag"`
	PublishP95Ms      float64 `json:"publish_p95_ms"`
	ReceiveP95Ms      float64 `json:"receive_p95_ms"`
	TopicCount        int     `json:"topic_count"`
	GroupCount        int     `json:"group_count"`
}

type KafkaTopicRow struct {
	Topic              string  `json:"topic"`
	ProduceRatePerSec  float64 `json:"produce_rate_per_sec"`
	ConsumeRatePerSec  float64 `json:"consume_rate_per_sec"`
	MaxLag             float64 `json:"max_lag"`
	E2EP95Ms           float64 `json:"e2e_p95_ms"`
	PublishP95Ms       float64 `json:"publish_p95_ms"`
	ReceiveP95Ms       float64 `json:"receive_p95_ms"`
	ErrorRate          float64 `json:"error_rate"`
	ConsumerGroupCount int     `json:"consumer_group_count"`
}

type KafkaGroupRow struct {
	ConsumerGroup      string  `json:"consumer_group"`
	Lag                float64 `json:"lag"`
	ConsumeRatePerSec  float64 `json:"consume_rate_per_sec"`
	ProcessRatePerSec  float64 `json:"process_rate_per_sec"`
	ProcessP95Ms       float64 `json:"process_p95_ms"`
	ErrorRate          float64 `json:"error_rate"`
	RebalanceRate      float64 `json:"rebalance_rate"`
	AssignedPartitions float64 `json:"assigned_partitions"`
	TopicCount         int     `json:"topic_count"`
}

type KafkaTopicTrendPoint struct {
	Timestamp         string  `json:"timestamp"`
	ProduceRatePerSec float64 `json:"produce_rate_per_sec"`
	ConsumeRatePerSec float64 `json:"consume_rate_per_sec"`
	PublishP95Ms      float64 `json:"publish_p95_ms"`
	ReceiveP95Ms      float64 `json:"receive_p95_ms"`
	E2EP95Ms          float64 `json:"e2e_p95_ms"`
	MaxLag            float64 `json:"max_lag"`
	ErrorRate         float64 `json:"error_rate"`
}

type KafkaTopicOverview struct {
	Topic   string                 `json:"topic"`
	Summary KafkaTopicRow          `json:"summary"`
	Trend   []KafkaTopicTrendPoint `json:"trend"`
}

type KafkaGroupTrendPoint struct {
	Timestamp          string  `json:"timestamp"`
	ConsumeRatePerSec  float64 `json:"consume_rate_per_sec"`
	ProcessRatePerSec  float64 `json:"process_rate_per_sec"`
	ProcessP95Ms       float64 `json:"process_p95_ms"`
	MaxLag             float64 `json:"max_lag"`
	ErrorRate          float64 `json:"error_rate"`
	RebalanceRate      float64 `json:"rebalance_rate"`
	AssignedPartitions float64 `json:"assigned_partitions"`
}

type KafkaGroupOverview struct {
	ConsumerGroup string                 `json:"consumer_group"`
	Summary       KafkaGroupRow          `json:"summary"`
	Trend         []KafkaGroupTrendPoint `json:"trend"`
}

type KafkaGroupTopicRow struct {
	Topic             string  `json:"topic"`
	ConsumeRatePerSec float64 `json:"consume_rate_per_sec"`
	MaxLag            float64 `json:"max_lag"`
	ReceiveP95Ms      float64 `json:"receive_p95_ms"`
	ErrorRate         float64 `json:"error_rate"`
}
