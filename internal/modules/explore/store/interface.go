package store

import "github.com/observability/observability-backend-go/internal/modules/explore/model"

// Repository encapsulates data access logic for saved queries.
type Repository interface {
	EnsureTable() error
	ListSavedQueries(teamID int64, queryType string) ([]model.SavedQuery, error)
	CreateSavedQuery(in model.SavedQueryInput) (model.SavedQuery, error)
	UpdateSavedQuery(teamID, id int64, in model.SavedQueryInput) (model.SavedQuery, error)
	DeleteSavedQuery(teamID, id int64) (bool, error)
	GetSavedQuery(teamID, id int64) (model.SavedQuery, error)
}
