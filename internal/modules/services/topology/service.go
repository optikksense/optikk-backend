package topology

import "context"

type Service interface {
	GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error)
}

type TopologyService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &TopologyService{repo: repo}
}

func (s *TopologyService) GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error) {
	ctx := context.Background()

	nodeRows, err := s.repo.GetTopologyNodes(ctx, teamID, startMs, endMs)
	if err != nil {
		return TopologyData{}, err
	}
	edges, err := s.repo.GetTopologyEdges(ctx, teamID, startMs, endMs)
	if err != nil {
		return TopologyData{}, err
	}

	nodes := make([]TopologyNode, len(nodeRows))
	for i, r := range nodeRows {
		errorRate := 0.0
		if r.RequestCount > 0 {
			errorRate = float64(r.ErrorCount) * 100.0 / float64(r.RequestCount)
		}
		nodes[i] = TopologyNode{
			Name:         r.Name,
			Status:       serviceStatus(errorRate),
			RequestCount: r.RequestCount,
			ErrorRate:    errorRate,
			AvgLatency:   r.AvgLatency,
		}
	}

	return TopologyData{Nodes: nodes, Edges: edges}, nil
}

func serviceStatus(errorRate float64) string {
	if errorRate > UnhealthyMinErrorRate {
		return StatusUnhealthy
	}
	if errorRate > HealthyMaxErrorRate {
		return StatusDegraded
	}
	return StatusHealthy
}
