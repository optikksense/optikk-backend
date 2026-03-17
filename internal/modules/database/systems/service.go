package systems

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	return s.repo.GetDetectedSystems(ctx, teamID, startMs, endMs)
}
