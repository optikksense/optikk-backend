package service

import "github.com/observability/observability-backend-go/internal/modules/explore/model"

// Service encapsulates the business logic for the explore module.
type Service interface {
	ListSavedQueries(teamID int64, queryType string) ([]model.SavedQuery, error)
	CreateSavedQuery(in model.SavedQueryInput) (model.SavedQuery, error)
	UpdateSavedQuery(teamID, id int64, in model.SavedQueryInput) (model.SavedQuery, error)
	DeleteSavedQuery(teamID, id int64) (bool, error)
}
