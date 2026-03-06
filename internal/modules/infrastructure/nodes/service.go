package nodes

import ()

// Service encapsulates the business logic for the infrastructure nodes module.
type Service interface {
	GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeSummary(teamUUID string, startMs, endMs int64) (InfrastructureNodeSummary, error)
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

func (s *NodeService) GetInfrastructureNodeSummary(teamUUID string, startMs, endMs int64) (InfrastructureNodeSummary, error) {
	nodes, err := s.repo.GetInfrastructureNodes(teamUUID, startMs, endMs)
	if err != nil {
		return InfrastructureNodeSummary{}, err
	}

	summary := InfrastructureNodeSummary{}
	for _, node := range nodes {
		switch {
		case node.ErrorRate > 10:
			summary.UnhealthyNodes++
		case node.ErrorRate > 2:
			summary.DegradedNodes++
		default:
			summary.HealthyNodes++
		}
		summary.TotalPods += node.PodCount
	}
	return summary, nil
}

func (s *NodeService) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	return s.repo.GetInfrastructureNodeServices(teamUUID, host, startMs, endMs)
}
