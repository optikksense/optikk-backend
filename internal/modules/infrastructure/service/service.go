package service

import (
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/model"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/store"
)

// InfrastructureService provides business logic orchestration for infrastructure tracking.
type InfrastructureService struct {
	repo store.Repository
}

// NewService creates a new InfrastructureService.
func NewService(repo store.Repository) *InfrastructureService {
	return &InfrastructureService{repo: repo}
}

func (s *InfrastructureService) GetInfrastructure(teamUUID string, startMs, endMs int64) ([]model.InfrastructureSummary, error) {
	return s.repo.GetInfrastructure(teamUUID, startMs, endMs)
}

func (s *InfrastructureService) GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]model.InfrastructureNode, error) {
	return s.repo.GetInfrastructureNodes(teamUUID, startMs, endMs)
}

func (s *InfrastructureService) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]model.InfrastructureNodeService, error) {
	return s.repo.GetInfrastructureNodeServices(teamUUID, host, startMs, endMs)
}
