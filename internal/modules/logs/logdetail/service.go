package logdetail

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// GetByID reads a single log by its deep-link id (a 16-char hex hash stored
// directly as the row's log_id column). Returns nil on not-found.
func (s *Service) GetByID(ctx context.Context, teamID int64, id string) (*models.Log, error) {
	row, err := s.repo.GetByID(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	out := models.MapLog(*row)
	return &out, nil
}
