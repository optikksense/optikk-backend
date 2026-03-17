package kubernetes

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetContainerCPU(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetContainerCPU(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetCPUThrottling(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetCPUThrottling(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetContainerMemory(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetContainerMemory(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetOOMKills(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetOOMKills(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetPodRestarts(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PodStat, error) {
	return s.repo.GetPodRestarts(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) (NodeAllocatable, error) {
	return s.repo.GetNodeAllocatable(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PhaseStat, error) {
	return s.repo.GetPodPhases(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ReplicaStat, error) {
	return s.repo.GetReplicaStatus(ctx, teamID, startMs, endMs, node)
}

func (s *Service) GetVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]VolumeStat, error) {
	return s.repo.GetVolumeUsage(ctx, teamID, startMs, endMs, node)
}
