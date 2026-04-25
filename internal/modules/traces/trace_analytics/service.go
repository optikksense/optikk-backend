package trace_analytics //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

type Service interface {
	Analytics(ctx context.Context, req AnalyticsRequest, teamID int64) (AnalyticsResponse, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service { return &service{repo: repo} }

func (s *service) Analytics(ctx context.Context, req AnalyticsRequest, teamID int64) (AnalyticsResponse, error) {
	filters, err := querycompiler.FromStructured(req.Filters, teamID, req.StartTime, req.EndTime)
	if err != nil {
		return AnalyticsResponse{}, fmt.Errorf("trace_analytics.parse: %w", err)
	}
	rows, warns, err := s.repo.Analytics(ctx, req, filters)
	if err != nil {
		return AnalyticsResponse{}, fmt.Errorf("trace_analytics.query: %w", err)
	}
	return AnalyticsResponse{
		VizMode:  normalizeViz(req.VizMode),
		Step:     req.Step,
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
