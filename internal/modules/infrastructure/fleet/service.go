package fleet

import "context"

type Service interface {
	GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error)
}

type fleetService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &fleetService{repo: repo}
}

func (s *fleetService) GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error) {
	return s.repo.GetFleetPods(ctx, teamID, startMs, endMs)
}
