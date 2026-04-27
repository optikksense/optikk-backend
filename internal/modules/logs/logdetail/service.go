package logdetail

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// GetByID reads a single log by the deep-link id. Returns nil on not-found.
func (s *Service) GetByID(ctx context.Context, teamID int64, id string) (*models.Log, error) {
	traceID, spanID, tsNs, ok := models.ParseLogID(id)
	if !ok {
		return nil, fmt.Errorf("logs.GetByID: malformed id %q", id)
	}
	row, err := s.repo.GetByID(ctx, teamID, traceID, spanID, tsNs)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	out := models.MapLog(*row)
	return &out, nil
}
