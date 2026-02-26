package service

import (
	"github.com/observability/observability-backend-go/internal/modules/saturation/model"
	"github.com/observability/observability-backend-go/internal/modules/saturation/store"
)

// SaturationService provides business logic orchestration for saturation metrics.
type SaturationService struct {
	repo store.Repository
}

// NewService creates a new SaturationService.
func NewService(repo store.Repository) *SaturationService {
	return &SaturationService{repo: repo}
}

func (s *SaturationService) GetSaturationMetrics(teamUUID string, startMs, endMs int64) ([]model.SaturationMetric, error) {
	return s.repo.GetSaturationMetrics(teamUUID, startMs, endMs)
}

func (s *SaturationService) GetSaturationTimeSeries(teamUUID string, startMs, endMs int64) ([]model.SaturationTimeSeries, error) {
	return s.repo.GetSaturationTimeSeries(teamUUID, startMs, endMs)
}
