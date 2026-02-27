package model

// SaturationMetric represents resource saturation summary per service.
type SaturationMetric struct {
	ServiceName         string  `json:"service_name"`
	SpanCount           int64   `json:"span_count"`
	AvgDurationMs       float64 `json:"avg_duration_ms"`
	P95DurationMs       float64 `json:"p95_duration_ms"`
	MaxDurationMs       float64 `json:"max_duration_ms"`
	ErrorCount          int64   `json:"error_count"`
	AvgDbPoolUtil       float64 `json:"avg_db_pool_util"`
	MaxDbPoolUtil       float64 `json:"max_db_pool_util"`
	AvgConsumerLag      float64 `json:"avg_consumer_lag"`
	MaxConsumerLag      float64 `json:"max_consumer_lag"`
	AvgThreadPoolActive float64 `json:"avg_thread_pool_active"`
	MaxThreadPoolSize   float64 `json:"max_thread_pool_size"`
	AvgQueueDepth       float64 `json:"avg_queue_depth"`
	MaxQueueDepth       float64 `json:"max_queue_depth"`
}

// SaturationTimeSeries represents saturation metrics over time per service.
type SaturationTimeSeries struct {
	ServiceName     string  `json:"service_name"`
	Timestamp       string  `json:"timestamp"`
	SpanCount       int64   `json:"span_count"`
	ErrorCount      int64   `json:"error_count"`
	AvgDurationMs   float64 `json:"avg_duration_ms"`
	AvgDbPoolUtil   float64 `json:"avg_db_pool_util"`
	AvgConsumerLag  float64 `json:"avg_consumer_lag"`
	AvgThreadActive float64 `json:"avg_thread_active"`
	AvgQueueDepth   float64 `json:"avg_queue_depth"`
}

// KafkaQueueLag represents lag per queue/topic.
type KafkaQueueLag struct {
	Queue          string  `json:"queue"`
	Timestamp      string  `json:"timestamp"`
	AvgConsumerLag float64 `json:"avg_consumer_lag"`
	MaxConsumerLag float64 `json:"max_consumer_lag"`
}

// KafkaProductionRate represents publish rate per topic.
type KafkaProductionRate struct {
	Queue          string  `json:"queue"`
	Timestamp      string  `json:"timestamp"`
	AvgPublishRate float64 `json:"avg_publish_rate"`
}

// KafkaConsumptionRate represents receive/fetch rate per topic.
type KafkaConsumptionRate struct {
	Queue          string  `json:"queue"`
	Timestamp      string  `json:"timestamp"`
	AvgReceiveRate float64 `json:"avg_receive_rate"`
}

// DatabaseQueryByTable represents query counts per table.
type DatabaseQueryByTable struct {
	Table      string `json:"table"`
	Timestamp  string `json:"timestamp"`
	QueryCount int64  `json:"query_count"`
}

// DatabaseAvgLatency represents latency metrics for the database over time.
type DatabaseAvgLatency struct {
	Timestamp    string  `json:"timestamp"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
}

// ---- Database Cache Insights ----

type DatabaseCacheResponse struct {
	Summary         DbCacheSummary      `json:"summary"`
	TableMetrics    []DbTableMetric     `json:"tableMetrics"`
	Cache           DbCacheStats        `json:"cache"`
	SlowLogs        DbSlowLogs          `json:"slowLogs"`
	SystemBreakdown []DbSystemBreakdown `json:"systemBreakdown"`
}

type DbSystemBreakdown struct {
	DbSystem        string   `json:"db_system"`
	QueryCount      int64    `json:"query_count"`
	AvgQueryLatency *float64 `json:"avg_query_latency_ms"`
	P95QueryLatency *float64 `json:"p95_query_latency_ms"`
	ErrorCount      int64    `json:"error_count"`
	SpanCount       int64    `json:"span_count"`
}

type DbCacheSummary struct {
	AvgQueryLatencyMs   *float64 `json:"avg_query_latency_ms"`
	P95QueryLatencyMs   *float64 `json:"p95_query_latency_ms"`
	DbSpanCount         int64    `json:"db_span_count"`
	CacheHits           int64    `json:"cache_hits"`
	CacheMisses         int64    `json:"cache_misses"`
	AvgReplicationLagMs *float64 `json:"avg_replication_lag_ms"`
}

type DbTableMetric struct {
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	DbSystem          string   `json:"db_system"`
	AvgQueryLatencyMs *float64 `json:"avg_query_latency_ms"`
	MaxQueryLatencyMs *float64 `json:"max_query_latency_ms"`
	CacheHits         int64    `json:"cache_hits"`
	CacheMisses       int64    `json:"cache_misses"`
	QueryCount        int64    `json:"query_count"`
}

type DbCacheStats struct {
	CacheHits     int64   `json:"cacheHits"`
	CacheMisses   int64   `json:"cacheMisses"`
	CacheHitRatio float64 `json:"cacheHitRatio"`
}

type DbSlowLogs struct {
	Logs    []any `json:"logs"` // Always empty based on current implementation
	HasMore bool  `json:"hasMore"`
	Offset  int   `json:"offset"`
	Limit   int   `json:"limit"`
	Total   int   `json:"total"`
}

// ---- Messaging Queue Insights ----

type MessagingQueueResponse struct {
	Summary    MqSummary    `json:"summary"`
	Timeseries []MqBucket   `json:"timeseries"`
	TopQueues  []MqTopQueue `json:"topQueues"`
}

type MqSummary struct {
	AvgQueueDepth    *float64 `json:"avg_queue_depth"`
	MaxQueueDepth    *float64 `json:"max_queue_depth"`
	AvgConsumerLag   *float64 `json:"avg_consumer_lag"`
	MaxConsumerLag   *float64 `json:"max_consumer_lag"`
	AvgPublishRate   float64  `json:"avg_publish_rate"`
	AvgReceiveRate   float64  `json:"avg_receive_rate"`
	ProcessingErrors float64  `json:"processing_errors"`
}

type MqBucket struct {
	Timestamp       string   `json:"timestamp"`
	ServiceName     string   `json:"service_name"`
	QueueName       string   `json:"queue_name"`
	MessagingSystem string   `json:"messaging_system"`
	AvgQueueDepth   *float64 `json:"avg_queue_depth"`
	AvgConsumerLag  *float64 `json:"avg_consumer_lag"`
	AvgPublishRate  float64  `json:"avg_publish_rate"`
	AvgReceiveRate  float64  `json:"avg_receive_rate"`
}

type MqTopQueue struct {
	QueueName       string   `json:"queue_name"`
	ServiceName     string   `json:"service_name"`
	MessagingSystem string   `json:"messaging_system"`
	AvgQueueDepth   *float64 `json:"avg_queue_depth"`
	MaxConsumerLag  *float64 `json:"max_consumer_lag"`
	AvgPublishRate  float64  `json:"avg_publish_rate"`
	AvgReceiveRate  float64  `json:"avg_receive_rate"`
	SampleCount     int64    `json:"sample_count"`
}
