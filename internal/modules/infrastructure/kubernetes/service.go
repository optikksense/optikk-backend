package kubernetes

type Service interface {
	GetContainerCPU(teamID int64, startMs, endMs int64) ([]ContainerBucket, error)
	GetCPUThrottling(teamID int64, startMs, endMs int64) ([]ContainerBucket, error)
	GetContainerMemory(teamID int64, startMs, endMs int64) ([]ContainerBucket, error)
	GetOOMKills(teamID int64, startMs, endMs int64) ([]ContainerBucket, error)
	GetPodRestarts(teamID int64, startMs, endMs int64) ([]PodStat, error)
	GetNodeAllocatable(teamID int64, startMs, endMs int64) (NodeAllocatable, error)
	GetPodPhases(teamID int64, startMs, endMs int64) ([]PhaseStat, error)
	GetReplicaStatus(teamID int64, startMs, endMs int64) ([]ReplicaStat, error)
	GetVolumeUsage(teamID int64, startMs, endMs int64) ([]VolumeStat, error)
}

type KubernetesService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &KubernetesService{repo: repo}
}

func (s *KubernetesService) GetContainerCPU(teamID int64, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetContainerCPU(teamID, startMs, endMs)
}

func (s *KubernetesService) GetCPUThrottling(teamID int64, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetCPUThrottling(teamID, startMs, endMs)
}

func (s *KubernetesService) GetContainerMemory(teamID int64, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetContainerMemory(teamID, startMs, endMs)
}

func (s *KubernetesService) GetOOMKills(teamID int64, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetOOMKills(teamID, startMs, endMs)
}

func (s *KubernetesService) GetPodRestarts(teamID int64, startMs, endMs int64) ([]PodStat, error) {
	return s.repo.GetPodRestarts(teamID, startMs, endMs)
}

func (s *KubernetesService) GetNodeAllocatable(teamID int64, startMs, endMs int64) (NodeAllocatable, error) {
	return s.repo.GetNodeAllocatable(teamID, startMs, endMs)
}

func (s *KubernetesService) GetPodPhases(teamID int64, startMs, endMs int64) ([]PhaseStat, error) {
	return s.repo.GetPodPhases(teamID, startMs, endMs)
}

func (s *KubernetesService) GetReplicaStatus(teamID int64, startMs, endMs int64) ([]ReplicaStat, error) {
	return s.repo.GetReplicaStatus(teamID, startMs, endMs)
}

func (s *KubernetesService) GetVolumeUsage(teamID int64, startMs, endMs int64) ([]VolumeStat, error) {
	return s.repo.GetVolumeUsage(teamID, startMs, endMs)
}
