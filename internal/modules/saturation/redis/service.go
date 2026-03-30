package redis

type Service struct {
	repo *ClickHouseRepository
}

func NewService(repo *ClickHouseRepository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetCacheHitRate(teamID, startMs, endMs int64, instance string) (CacheHitRate, error) {
	return s.repo.GetCacheHitRate(teamID, startMs, endMs, instance)
}

func (s *Service) GetReplicationLag(teamID, startMs, endMs int64, instance string) (ReplicationLag, error) {
	return s.repo.GetReplicationLag(teamID, startMs, endMs, instance)
}

func (s *Service) GetClients(teamID, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisClientsConnected, instance)
}

func (s *Service) GetMemory(teamID, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisMemoryUsed, instance)
}

func (s *Service) GetMemoryFragmentation(teamID, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisMemoryFragmentation, instance)
}

func (s *Service) GetCommands(teamID, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisCommandsProcessed, instance)
}

func (s *Service) GetEvictions(teamID, startMs, endMs int64, instance string) ([]MetricPoint, error) {
	return s.repo.QueryMetricSeries(teamID, startMs, endMs, metricRedisKeysEvicted, instance)
}

func (s *Service) GetInstances(teamID, startMs, endMs int64) ([]RedisInstanceSummary, error) {
	return s.repo.GetInstances(teamID, startMs, endMs)
}

func (s *Service) GetKeyspace(teamID, startMs, endMs int64, instance string) ([]KeyspaceRow, error) {
	return s.repo.GetKeyspace(teamID, startMs, endMs, instance)
}

func (s *Service) GetKeyExpiries(teamID, startMs, endMs int64, instance string) ([]KeyExpiryRow, error) {
	return s.repo.GetKeyExpiries(teamID, startMs, endMs, instance)
}
