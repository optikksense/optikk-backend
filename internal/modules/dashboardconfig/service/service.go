package service

import (
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig/store"
)

// DashboardConfigService provides business logic orchestration for dashboard configurations.
type DashboardConfigService struct {
	repo store.Repository
}

// NewService creates a new DashboardConfigService.
func NewService(repo store.Repository) *DashboardConfigService {
	return &DashboardConfigService{repo: repo}
}

func (s *DashboardConfigService) EnsureTable() error {
	return s.repo.EnsureTable()
}

func (s *DashboardConfigService) GetConfig(teamID int64, pageID string) (string, error) {
	return s.repo.GetConfig(teamID, pageID)
}

func (s *DashboardConfigService) SaveConfig(teamID int64, pageID, configYaml string) error {
	return s.repo.SaveConfig(teamID, pageID, configYaml)
}
