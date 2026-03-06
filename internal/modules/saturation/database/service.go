package database

// DatabaseService provides business logic orchestration for database saturation metrics.
type DatabaseService struct {
	repo *ClickHouseRepository
}

// NewService creates a new DatabaseService.
func NewService(repo *ClickHouseRepository) *DatabaseService {
	return &DatabaseService{repo: repo}
}

func (s *DatabaseService) GetDatabaseQueryByTable(teamUUID string, startMs, endMs int64) ([]DatabaseQueryByTable, error) {
	return s.repo.GetDatabaseQueryByTable(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetDatabaseAvgLatency(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseCacheSummary(teamUUID string, startMs, endMs int64) (DbCacheSummary, error) {
	return s.repo.GetDatabaseCacheSummary(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseSystems(teamUUID string, startMs, endMs int64) ([]DbSystemBreakdown, error) {
	return s.repo.GetDatabaseSystems(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseTopTables(teamUUID string, startMs, endMs int64) ([]DbTableMetric, error) {
	return s.repo.GetDatabaseTopTables(teamUUID, startMs, endMs)
}

// db.client.* OTel standard metrics

func (s *DatabaseService) GetConnectionCount(teamUUID string, startMs, endMs int64) ([]ConnectionStatValue, error) {
	return s.repo.GetConnectionCount(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetConnectionWaitTime(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetConnectionWaitTime(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetConnectionPending(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetConnectionPending(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetConnectionTimeouts(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetConnectionTimeouts(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetQueryDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetQueryDuration(teamUUID, startMs, endMs)
}

// Redis metrics

func (s *DatabaseService) GetRedisCacheHitRate(teamUUID string, startMs, endMs int64) (RedisHitRate, error) {
	return s.repo.GetRedisCacheHitRate(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisConnectedClients(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisConnectedClients(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisMemoryUsed(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisMemoryUsed(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisMemoryFragmentation(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisMemoryFragmentation(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisCommandRate(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisCommandRate(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisEvictions(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisEvictions(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisKeyspaceSize(teamUUID string, startMs, endMs int64) ([]RedisDBKeyStat, error) {
	return s.repo.GetRedisKeyspaceSize(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisKeyExpiries(teamUUID string, startMs, endMs int64) ([]RedisDBKeyStat, error) {
	return s.repo.GetRedisKeyExpiries(teamUUID, startMs, endMs)
}

func (s *DatabaseService) GetRedisReplicationLag(teamUUID string, startMs, endMs int64) (RedisReplicationLag, error) {
	return s.repo.GetRedisReplicationLag(teamUUID, startMs, endMs)
}
