package livetail

import "time"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Poll(teamID int64, since time.Time, filters LiveTailFilters) ([]LiveSpan, error) {
	return s.repo.Poll(teamID, since, filters)
}
