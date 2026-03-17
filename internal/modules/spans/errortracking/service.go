package errortracking

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error) {
	return s.repo.GetExceptionRateByType(ctx, teamID, startMs, endMs, serviceName)
}

func (s *Service) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error) {
	return s.repo.GetErrorHotspot(ctx, teamID, startMs, endMs)
}

func (s *Service) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error) {
	return s.repo.GetHTTP5xxByRoute(ctx, teamID, startMs, endMs, serviceName)
}
