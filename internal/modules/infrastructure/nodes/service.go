package nodes

import ()

// Service encapsulates the business logic for the infrastructure nodes module.
type Service interface {
	GetInfrastructureNodes(teamID int64, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeSummary(teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error)
	GetInfrastructureNodeServices(teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

// NodeService provides business logic orchestration for infrastructure nodes tracking.
type NodeService struct {
	repo Repository
}

// NewService creates a new NodeService.
func NewService(repo Repository) Service {
	return &NodeService{repo: repo}
}

func (s *NodeService) GetInfrastructureNodes(teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	return s.repo.GetInfrastructureNodes(teamID, startMs, endMs)
}

func (s *NodeService) GetInfrastructureNodeSummary(teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error) {
	nodes, err := s.repo.GetInfrastructureNodes(teamID, startMs, endMs)
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

func (s *NodeService) GetInfrastructureNodeServices(teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	return s.repo.GetInfrastructureNodeServices(teamID, host, startMs, endMs)
}
