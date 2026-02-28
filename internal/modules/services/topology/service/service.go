package service

import (
	"github.com/observability/observability-backend-go/internal/modules/services/topology/model"
	"github.com/observability/observability-backend-go/internal/modules/services/topology/store"
)

// Service encapsulates the business logic for the services topology module.
type Service interface {
	GetTopology(teamUUID string, startMs, endMs int64) (model.TopologyData, error)
}

// TopologyService provides business logic orchestration for services topology.
type TopologyService struct {
	repo store.Repository
}

// NewService creates a new services topology service.
func NewService(repo store.Repository) Service {
	return &TopologyService{repo: repo}
}

func (s *TopologyService) GetTopology(teamUUID string, startMs, endMs int64) (model.TopologyData, error) {
	return s.repo.GetTopology(teamUUID, startMs, endMs)
}
