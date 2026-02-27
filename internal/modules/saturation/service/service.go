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

func (s *SaturationService) GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (*model.DatabaseCacheResponse, error) {
	summary, tableMetrics, systemBreakdown, err := s.repo.GetInsightDatabaseCache(teamUUID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	total := summary.CacheHits + summary.CacheMisses
	hitRatio := 0.0
	if total > 0 {
		hitRatio = float64(summary.CacheHits) * 100.0 / float64(total)
	}

	return &model.DatabaseCacheResponse{
		Summary:      summary,
		TableMetrics: tableMetrics,
		Cache: model.DbCacheStats{
			CacheHits:     summary.CacheHits,
			CacheMisses:   summary.CacheMisses,
			CacheHitRatio: hitRatio,
		},
		SlowLogs: model.DbSlowLogs{
			Logs:    []any{},
			HasMore: false,
			Offset:  0,
			Limit:   50,
			Total:   0,
		},
		SystemBreakdown: systemBreakdown,
	}, nil
}

func (s *SaturationService) GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (*model.MessagingQueueResponse, error) {
	summary, timeseries, topQueues, err := s.repo.GetInsightMessagingQueue(teamUUID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	return &model.MessagingQueueResponse{
		Summary:    summary,
		Timeseries: timeseries,
		TopQueues:  topQueues,
	}, nil
}
