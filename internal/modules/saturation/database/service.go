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
