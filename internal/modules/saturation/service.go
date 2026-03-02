package saturation

import ()

// SaturationService provides business logic orchestration for saturation metrics.
type SaturationService struct {
	repo *ClickHouseRepository
}

// NewService creates a new SaturationService.
func NewService(repo *ClickHouseRepository) *SaturationService {
	return &SaturationService{repo: repo}
}

func (s *SaturationService) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]KafkaQueueLag, error) {
	return s.repo.GetKafkaQueueLag(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetKafkaProductionRate(teamUUID string, startMs, endMs int64) ([]KafkaProductionRate, error) {
	return s.repo.GetKafkaProductionRate(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetKafkaConsumptionRate(teamUUID string, startMs, endMs int64) ([]KafkaConsumptionRate, error) {
	return s.repo.GetKafkaConsumptionRate(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseQueryByTable(teamUUID string, startMs, endMs int64) ([]DatabaseQueryByTable, error) {
	return s.repo.GetDatabaseQueryByTable(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	return s.repo.GetDatabaseAvgLatency(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseCacheSummary(teamUUID string, startMs, endMs int64) (DbCacheSummary, error) {
	return s.repo.GetDatabaseCacheSummary(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseSystems(teamUUID string, startMs, endMs int64) ([]DbSystemBreakdown, error) {
	return s.repo.GetDatabaseSystems(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseTopTables(teamUUID string, startMs, endMs int64) ([]DbTableMetric, error) {
	return s.repo.GetDatabaseTopTables(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetQueueConsumerLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	return s.repo.GetQueueConsumerLag(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetQueueTopicLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	return s.repo.GetQueueTopicLag(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetQueueTopQueues(teamUUID string, startMs, endMs int64) ([]MqTopQueue, error) {
	return s.repo.GetQueueTopQueues(teamUUID, startMs, endMs)
}
