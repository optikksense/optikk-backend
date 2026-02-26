package service

import (
	"github.com/observability/observability-backend-go/internal/modules/explore/model"
	"github.com/observability/observability-backend-go/internal/modules/explore/store"
)

// ExploreService provides business logic orchestration for saved queries.
type ExploreService struct {
	repo store.Repository
}

// NewService creates a new ExploreService.
func NewService(repo store.Repository) *ExploreService {
	return &ExploreService{repo: repo}
}

func (s *ExploreService) ListSavedQueries(teamID int64, queryType string) ([]model.SavedQuery, error) {
	return s.repo.ListSavedQueries(teamID, queryType)
}

func (s *ExploreService) CreateSavedQuery(in model.SavedQueryInput) (model.SavedQuery, error) {
	return s.repo.CreateSavedQuery(in)
}

func (s *ExploreService) UpdateSavedQuery(teamID, id int64, in model.SavedQueryInput) (model.SavedQuery, error) {
	return s.repo.UpdateSavedQuery(teamID, id, in)
}

func (s *ExploreService) DeleteSavedQuery(teamID, id int64) (bool, error) {
	return s.repo.DeleteSavedQuery(teamID, id)
}
