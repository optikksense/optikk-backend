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
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

type KeyspaceRow struct {
	RedisDB  string  `json:"redis_db"`
	KeyCount float64 `json:"key_count"`
}

type KeyExpiryRow struct {
	RedisDB     string  `json:"redis_db"`
	ExpiryCount float64 `json:"expiry_count"`
}
