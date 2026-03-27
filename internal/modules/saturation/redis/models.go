package redis

type CacheHitRate struct {
	HitRatePct float64 `json:"hit_rate_pct"`
	Hits       float64 `json:"hits"`
	Misses     float64 `json:"misses"`
}

type ReplicationLag struct {
	Offset float64 `json:"offset"`
}

type MetricPoint struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Value     *float64 `json:"value"     ch:"value"`
}

type RedisInstanceSummary struct {
	Instance                    string  `json:"instance"                       ch:"instance"`
	AvgConnectedClients         float64 `json:"avg_connected_clients"          ch:"avg_connected_clients"`
	AvgMemoryUsed               float64 `json:"avg_memory_used"                ch:"avg_memory_used"`
	AvgMemoryFragmentationRatio float64 `json:"avg_memory_fragmentation_ratio" ch:"avg_memory_fragmentation_ratio"`
	AvgCommandsProcessed        float64 `json:"avg_commands_processed"         ch:"avg_commands_processed"`
	EvictedKeys                 float64 `json:"evicted_keys"                   ch:"evicted_keys"`
}

type KeyspaceRow struct {
	RedisDB  string  `json:"redis_db"  ch:"redis_db"`
	KeyCount float64 `json:"key_count" ch:"key_count"`
}

type KeyExpiryRow struct {
	RedisDB     string  `json:"redis_db"     ch:"redis_db"`
	ExpiryCount float64 `json:"expiry_count" ch:"expiry_count"`
}
