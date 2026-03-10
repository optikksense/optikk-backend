package topology

import ()

// Service encapsulates the business logic for the services topology module.
type Service interface {
	GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error)
}

// TopologyService provides business logic orchestration for services topology.
type TopologyService struct {
	repo Repository
}

// NewService creates a new services topology service.
func NewService(repo Repository) Service {
	return &TopologyService{repo: repo}
}

func (s *TopologyService) GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error) {
	return s.repo.GetTopology(teamID, startMs, endMs)
}
