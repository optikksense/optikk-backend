package topology

import ()

// Service encapsulates the business logic for the services topology module.
type Service interface {
	GetTopology(teamUUID string, startMs, endMs int64) (TopologyData, error)
}

// TopologyService provides business logic orchestration for services topology.
type TopologyService struct {
	repo Repository
}

// NewService creates a new services topology service.
func NewService(repo Repository) Service {
	return &TopologyService{repo: repo}
}

func (s *TopologyService) GetTopology(teamUUID string, startMs, endMs int64) (TopologyData, error) {
	return s.repo.GetTopology(teamUUID, startMs, endMs)
}
