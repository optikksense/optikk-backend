package service

import (
	"github.com/observability/observability-backend-go/internal/modules/saturation/model"
	"github.com/observability/observability-backend-go/internal/modules/saturation/store"
)

// SaturationService provides business logic orchestration for saturation metrics.
type SaturationService struct {
	repo *store.ClickHouseRepository
}

// NewService creates a new SaturationService.
func NewService(repo *store.ClickHouseRepository) *SaturationService {
	return &SaturationService{repo: repo}
}

func (s *SaturationService) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]model.KafkaQueueLag, error) {
	return s.repo.GetKafkaQueueLag(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetKafkaProductionRate(teamUUID string, startMs, endMs int64) ([]model.KafkaProductionRate, error) {
	return s.repo.GetKafkaProductionRate(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetKafkaConsumptionRate(teamUUID string, startMs, endMs int64) ([]model.KafkaConsumptionRate, error) {
	return s.repo.GetKafkaConsumptionRate(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseQueryByTable(teamUUID string, startMs, endMs int64) ([]model.DatabaseQueryByTable, error) {
	return s.repo.GetDatabaseQueryByTable(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]model.DatabaseAvgLatency, error) {
	return s.repo.GetDatabaseAvgLatency(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseCacheSummary(teamUUID string, startMs, endMs int64) (model.DbCacheSummary, error) {
	return s.repo.GetDatabaseCacheSummary(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseSystems(teamUUID string, startMs, endMs int64) ([]model.DbSystemBreakdown, error) {
	return s.repo.GetDatabaseSystems(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetDatabaseTopTables(teamUUID string, startMs, endMs int64) ([]model.DbTableMetric, error) {
	return s.repo.GetDatabaseTopTables(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetQueueConsumerLag(teamUUID string, startMs, endMs int64) ([]model.MqBucket, error) {
	return s.repo.GetQueueConsumerLag(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetQueueTopicLag(teamUUID string, startMs, endMs int64) ([]model.MqBucket, error) {
	return s.repo.GetQueueTopicLag(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetQueueTopQueues(teamUUID string, startMs, endMs int64) ([]model.MqTopQueue, error) {
	return s.repo.GetQueueTopQueues(teamUUID, startMs, endMs)
}
