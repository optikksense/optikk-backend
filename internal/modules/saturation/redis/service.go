package redis

type RedisService struct {
	repo *ClickHouseRepository
}

func NewService(repo *ClickHouseRepository) *RedisService {
	return &RedisService{repo: repo}
}

func (s *RedisService) GetCacheHitRate(teamID int64, startMs, endMs int64, instance string) (CacheHitRate, error) {
	return s.repo.GetCacheHitRate(teamID, startMs, endMs, instance)
}

func (s *RedisService) GetReplicationLag(teamID int64, startMs, endMs int64, instance string) (ReplicationLag, error) {
	return s.repo.GetReplicationLag(teamID, startMs, endMs, instance)
}

func (s *RedisService) GetClients(teamID int64, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisClientsConnected, instance)
}

func (s *RedisService) GetMemory(teamID int64, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisMemoryUsed, instance)
}

func (s *RedisService) GetMemoryFragmentation(teamID int64, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisMemoryFragmentation, instance)
}

func (s *RedisService) GetCommands(teamID int64, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisCommandsProcessed, instance)
}

func (s *RedisService) GetEvictions(teamID int64, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisKeysEvicted, instance)
}

func (s *RedisService) GetKeyspace(teamID int64, startMs, endMs int64, instance string) ([]KeyspaceRow, error) {
	return s.repo.GetKeyspace(teamID, startMs, endMs, instance)
}

func (s *RedisService) GetKeyExpiries(teamID int64, startMs, endMs int64, instance string) ([]KeyExpiryRow, error) {
	return s.repo.GetKeyExpiries(teamID, startMs, endMs, instance)
}
