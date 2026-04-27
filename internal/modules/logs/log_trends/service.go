package log_trends //nolint:revive,stylecheck

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Summary is the in-process include for explorer's /logs/query.
func (s *Service) Summary(ctx context.Context, f querycompiler.Filters) (models.Summary, error) {
	out, err := s.repo.Summary(ctx, f)
	if err != nil {
		return models.Summary{}, fmt.Errorf("logs.Summary: %w", err)
	}
	return out, nil
}

// Trend is the in-process include for explorer's /logs/query.
func (s *Service) Trend(ctx context.Context, f querycompiler.Filters, step string) ([]models.TrendBucket, []string, error) {
	out, warns, err := s.repo.Trend(ctx, f, step)
	if err != nil {
		return nil, nil, fmt.Errorf("logs.Trend: %w", err)
	}
	return out, warns, nil
}

// ComputeResponse drives the public POST /logs/trends endpoint.
func (s *Service) ComputeResponse(ctx context.Context, f querycompiler.Filters, step string) (Response, error) {
	sum, err := s.Summary(ctx, f)
	if err != nil {
		return Response{}, err
	}
	trend, warns, err := s.Trend(ctx, f, step)
	if err != nil {
		return Response{}, err
	}
	return Response{Summary: sum, Trend: trend, Warnings: warns}, nil
}
