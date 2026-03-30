package jvm

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	return s.repo.GetJVMMemory(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetJVMGCDuration(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMGCCollectionBucket, error) {
	return s.repo.GetJVMGCCollections(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMThreadBucket, error) {
	return s.repo.GetJVMThreadCount(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	return s.repo.GetJVMClasses(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	return s.repo.GetJVMCPU(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	return s.repo.GetJVMBuffers(ctx, teamID, startMs, endMs)
}
