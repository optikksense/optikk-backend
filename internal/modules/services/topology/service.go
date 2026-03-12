package topology

import ()

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
	return s.repo.GetTopology(teamID, startMs, endMs)
}
