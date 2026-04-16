package overview

import (
	"context"

	aishared "github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
)

// Service defines the business logic for the AI overview module.
type Service interface {
	GetOverview(ctx context.Context, teamID, startMs, endMs int64) (aishared.AIOverview, error)
	GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]aishared.AITrendPoint, error)
	GetTopModels(ctx context.Context, teamID, startMs, endMs int64) ([]AIModelBreakdown, error)
	GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64) ([]AIPromptBreakdown, error)
	GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error)
}

type service struct {
	repo Repository
}

// NewService creates a new overview service.
func NewService(repo Repository) Service {
	return &service{repo: repo}
}

func (s *service) GetOverview(ctx context.Context, teamID, startMs, endMs int64) (aishared.AIOverview, error) {
	return s.repo.GetOverview(ctx, teamID, startMs, endMs)
}

func (s *service) GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]aishared.AITrendPoint, error) {
	if step == "" {
		step = "5m"
	}
	return s.repo.GetOverviewTimeseries(ctx, teamID, startMs, endMs, step)
}

func (s *service) GetTopModels(ctx context.Context, teamID, startMs, endMs int64) ([]AIModelBreakdown, error) {
	return s.repo.GetTopModels(ctx, teamID, startMs, endMs, aishared.DefaultBreakdownLimit)
}

func (s *service) GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64) ([]AIPromptBreakdown, error) {
	return s.repo.GetTopPrompts(ctx, teamID, startMs, endMs, aishared.DefaultBreakdownLimit)
}

func (s *service) GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error) {
	return s.repo.GetQualitySummary(ctx, teamID, startMs, endMs)
}
