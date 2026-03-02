package nodes

import ()

// Service encapsulates the business logic for the infrastructure nodes module.
type Service interface {
	GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

// NodeService provides business logic orchestration for infrastructure nodes tracking.
type NodeService struct {
	repo Repository
}

// NewService creates a new NodeService.
func NewService(repo Repository) Service {
	return &NodeService{repo: repo}
}

func (s *NodeService) GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]InfrastructureNode, error) {
	return s.repo.GetInfrastructureNodes(teamUUID, startMs, endMs)
}

func (s *NodeService) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	return s.repo.GetInfrastructureNodeServices(teamUUID, host, startMs, endMs)
}
