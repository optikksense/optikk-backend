package database

// DatabaseService provides business logic orchestration for database saturation metrics.
type DatabaseService struct {
	repo *ClickHouseRepository
}

// NewService creates a new DatabaseService.
func NewService(repo *ClickHouseRepository) *DatabaseService {
	return &DatabaseService{repo: repo}
}

func (s *DatabaseService) GetDatabaseQueryByTable(teamID int64, startMs, endMs int64) ([]DatabaseQueryByTable, error) {
	return s.repo.GetDatabaseQueryByTable(teamID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseAvgLatency(teamID int64, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetDatabaseAvgLatency(teamID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseCacheSummary(teamID int64, startMs, endMs int64) (DbCacheSummary, error) {
	return s.repo.GetDatabaseCacheSummary(teamID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseSystems(teamID int64, startMs, endMs int64) ([]DbSystemBreakdown, error) {
	return s.repo.GetDatabaseSystems(teamID, startMs, endMs)
}

func (s *DatabaseService) GetDatabaseTopTables(teamID int64, startMs, endMs int64) ([]DbTableMetric, error) {
	return s.repo.GetDatabaseTopTables(teamID, startMs, endMs)
}

// db.client.* OTel standard metrics

func (s *DatabaseService) GetConnectionCount(teamID int64, startMs, endMs int64) ([]ConnectionStatValue, error) {
	return s.repo.GetConnectionCount(teamID, startMs, endMs)
}

func (s *DatabaseService) GetConnectionWaitTime(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetConnectionWaitTime(teamID, startMs, endMs)
}

func (s *DatabaseService) GetConnectionPending(teamID int64, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetConnectionPending(teamID, startMs, endMs)
}

func (s *DatabaseService) GetConnectionTimeouts(teamID int64, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetConnectionTimeouts(teamID, startMs, endMs)
}

func (s *DatabaseService) GetQueryDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetQueryDuration(teamID, startMs, endMs)
}

// Redis metrics

func (s *DatabaseService) GetRedisCacheHitRate(teamID int64, startMs, endMs int64) (RedisHitRate, error) {
	return s.repo.GetRedisCacheHitRate(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisConnectedClients(teamID int64, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisConnectedClients(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisMemoryUsed(teamID int64, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisMemoryUsed(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisMemoryFragmentation(teamID int64, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisMemoryFragmentation(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisCommandRate(teamID int64, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisCommandRate(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisEvictions(teamID int64, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return s.repo.GetRedisEvictions(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisKeyspaceSize(teamID int64, startMs, endMs int64) ([]RedisDBKeyStat, error) {
	return s.repo.GetRedisKeyspaceSize(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisKeyExpiries(teamID int64, startMs, endMs int64) ([]RedisDBKeyStat, error) {
	return s.repo.GetRedisKeyExpiries(teamID, startMs, endMs)
}

func (s *DatabaseService) GetRedisReplicationLag(teamID int64, startMs, endMs int64) (RedisReplicationLag, error) {
	return s.repo.GetRedisReplicationLag(teamID, startMs, endMs)
}
