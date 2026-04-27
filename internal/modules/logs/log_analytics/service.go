package log_analytics //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Analytics dispatches the group-by grid query.
func (s *Service) Analytics(ctx context.Context, req Request, teamID int64) (Response, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return Response{}, fmt.Errorf("logs.Analytics.parse: %w", err)
	}
	rows, stepToken, warns, err := s.repo.Analytics(ctx, filters, req)
	if err != nil {
		return Response{}, fmt.Errorf("logs.Analytics.query: %w", err)
	}
	return Response{
		VizMode:  normalizeViz(req.VizMode),
		Step:     stepToken,
		Rows:     rows,
		Warnings: warns,
	}, nil
}

func normalizeViz(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "topn", "table", "pie":
		return strings.ToLower(v)
	default:
		return "timeseries"
	}
}
