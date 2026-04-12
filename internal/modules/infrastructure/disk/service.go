package disk

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskIO(ctx, teamID, startMs, endMs)
}

func (s *Service) GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskOperations(ctx, teamID, startMs, endMs)
}

func (s *Service) GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetDiskIOTime(ctx, teamID, startMs, endMs)
}

func (s *Service) GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]MountpointBucket, error) {
	return s.repo.GetFilesystemUsage(ctx, teamID, startMs, endMs)
}

func (s *Service) GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetFilesystemUtilization(ctx, teamID, startMs, endMs)
}

func (s *Service) GetAvgDisk(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgDisk(ctx, teamID, startMs, endMs)
}

func (s *Service) GetDiskByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return s.repo.GetDiskByService(ctx, teamID, serviceName, startMs, endMs)
}

func (s *Service) GetDiskByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return s.repo.GetDiskByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
