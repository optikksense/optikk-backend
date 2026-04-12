package livetail

import (
	"context"
	"time"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Poll(ctx context.Context, teamID int64, since time.Time, filters LiveTailFilters) (*PollResult, error) {
	return s.repo.Poll(ctx, teamID, since, filters)
}
