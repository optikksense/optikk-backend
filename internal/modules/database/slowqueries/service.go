package slowqueries

import (
	"context"

	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error) {
	return s.repo.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, f, limit)
}

func (s *Service) GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error) {
	return s.repo.GetSlowestCollections(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	return s.repo.GetSlowQueryRate(ctx, teamID, startMs, endMs, f, thresholdMs)
}

func (s *Service) GetP99ByQueryText(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]P99ByQueryText, error) {
	return s.repo.GetP99ByQueryText(ctx, teamID, startMs, endMs, f, limit)
}
