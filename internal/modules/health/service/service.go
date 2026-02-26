package service

import (
	"github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/modules/health/model"
	"github.com/observability/observability-backend-go/internal/modules/health/store"
)

// HealthService provides business logic orchestration for health checks.
type HealthService struct {
	repo store.Repository
}

// NewService creates a new HealthService.
func NewService(repo store.Repository) *HealthService {
	return &HealthService{repo: repo}
}

func (s *HealthService) GetHealthChecks(teamID int64) ([]model.HealthCheck, error) {
	return s.repo.GetHealthChecks(teamID)
}

func (s *HealthService) GetHealthCheck(id int64) (model.HealthCheck, error) {
	return s.repo.GetHealthCheck(id)
}

func (s *HealthService) CreateHealthCheck(teamID, orgID int64, req contracts.HealthCheckRequest) (model.HealthCheck, error) {
	return s.repo.CreateHealthCheck(teamID, orgID, req)
}

func (s *HealthService) UpdateHealthCheck(id int64, req contracts.HealthCheckRequest) (model.HealthCheck, error) {
	return s.repo.UpdateHealthCheck(id, req)
}

func (s *HealthService) DeleteHealthCheck(id int64) error {
	return s.repo.DeleteHealthCheck(id)
}

func (s *HealthService) ToggleHealthCheck(id int64) (model.HealthCheck, error) {
	return s.repo.ToggleHealthCheck(id)
}

func (s *HealthService) GetHealthCheckStatus(teamUUID string, startMs, endMs int64) ([]model.HealthCheckStatus, error) {
	return s.repo.GetHealthCheckStatus(teamUUID, startMs, endMs)
}

func (s *HealthService) GetHealthCheckResults(teamUUID string, checkID string, startMs, endMs int64, limit, offset int) ([]model.HealthCheckResult, error) {
	return s.repo.GetHealthCheckResults(teamUUID, checkID, startMs, endMs, limit, offset)
}

func (s *HealthService) GetHealthCheckTrend(teamUUID string, checkID string, startMs, endMs int64) ([]model.HealthCheckTrend, error) {
	return s.repo.GetHealthCheckTrend(teamUUID, checkID, startMs, endMs)
}
