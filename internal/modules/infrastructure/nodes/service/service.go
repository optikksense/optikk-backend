package service

import (
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes/model"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/nodes/store"
)

// Service encapsulates the business logic for the infrastructure nodes module.
type Service interface {
	GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]model.InfrastructureNode, error)
	GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]model.InfrastructureNodeService, error)
}

// NodeService provides business logic orchestration for infrastructure nodes tracking.
type NodeService struct {
	repo store.Repository
}

// NewService creates a new NodeService.
func NewService(repo store.Repository) Service {
	return &NodeService{repo: repo}
}

func (s *NodeService) GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]model.InfrastructureNode, error) {
	return s.repo.GetInfrastructureNodes(teamUUID, startMs, endMs)
}

func (s *NodeService) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]model.InfrastructureNodeService, error) {
	return s.repo.GetInfrastructureNodeServices(teamUUID, host, startMs, endMs)
}
