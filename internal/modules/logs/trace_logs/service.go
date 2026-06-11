package trace_logs

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// GetByTraceID resolves all logs for a (team_id, trace_id) pair.
func (s *Service) GetByTraceID(ctx context.Context, teamID int64, traceID string, limit int) ([]models.Log, error) {
	bounds, err := s.repo.LookupBounds(ctx, teamID, traceID)
	if err != nil {
		return nil, err
	}
	if bounds.Count == 0 {
		return []models.Log{}, nil
	}
	rows, err := s.repo.FetchByBounds(ctx, teamID, traceID, bounds.MinB, bounds.MaxB, bounds.Fps, limit)
	if err != nil {
		return nil, err
	}
	return models.MapLogs(rows), nil
}
