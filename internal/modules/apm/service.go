package apm

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRPCDuration(ctx, teamID, startMs, endMs)
}

func (s *Service) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetRPCRequestRate(ctx, teamID, startMs, endMs)
}

func (s *Service) GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetMessagingPublishDuration(ctx, teamID, startMs, endMs)
}

func (s *Service) GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCPU(ctx, teamID, startMs, endMs)
}

func (s *Service) GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (ProcessMemory, error) {
	return s.repo.GetProcessMemory(ctx, teamID, startMs, endMs)
}

func (s *Service) GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetOpenFDs(ctx, teamID, startMs, endMs)
}

func (s *Service) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetUptime(ctx, teamID, startMs, endMs)
}
