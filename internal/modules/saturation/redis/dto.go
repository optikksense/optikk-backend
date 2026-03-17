package redis

type cacheHitRateDTO = CacheHitRate
type replicationLagDTO = ReplicationLag
type metricPointDTO = MetricPoint
type keyspaceRowDTO = KeyspaceRow
type keyExpiryRowDTO = KeyExpiryRow

type cacheHitRateRow struct {
	Hits   float64 `ch:"hits"`
	Misses float64 `ch:"misses"`
}

type replicationLagRow struct {
	ReplicationOffset float64 `ch:"replication_offset"`
	BacklogOffset     float64 `ch:"backlog_offset"`
}
