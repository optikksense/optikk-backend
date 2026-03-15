package kubernetes

type Service interface {
	GetContainerCPU(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetCPUThrottling(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetContainerMemory(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetOOMKills(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetPodRestarts(teamID int64, startMs, endMs int64, node string) ([]PodStat, error)
	GetNodeAllocatable(teamID int64, startMs, endMs int64, node string) (NodeAllocatable, error)
	GetPodPhases(teamID int64, startMs, endMs int64, node string) ([]PhaseStat, error)
	GetReplicaStatus(teamID int64, startMs, endMs int64, node string) ([]ReplicaStat, error)
	GetVolumeUsage(teamID int64, startMs, endMs int64, node string) ([]VolumeStat, error)
}

type KubernetesService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &KubernetesService{repo: repo}
}

func (s *KubernetesService) GetContainerCPU(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetContainerCPU(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetCPUThrottling(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetCPUThrottling(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetContainerMemory(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetContainerMemory(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetOOMKills(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return s.repo.GetOOMKills(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetPodRestarts(teamID int64, startMs, endMs int64, node string) ([]PodStat, error) {
	return s.repo.GetPodRestarts(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetNodeAllocatable(teamID int64, startMs, endMs int64, node string) (NodeAllocatable, error) {
	return s.repo.GetNodeAllocatable(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetPodPhases(teamID int64, startMs, endMs int64, node string) ([]PhaseStat, error) {
	return s.repo.GetPodPhases(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetReplicaStatus(teamID int64, startMs, endMs int64, node string) ([]ReplicaStat, error) {
	return s.repo.GetReplicaStatus(teamID, startMs, endMs, node)
}

func (s *KubernetesService) GetVolumeUsage(teamID int64, startMs, endMs int64, node string) ([]VolumeStat, error) {
	return s.repo.GetVolumeUsage(teamID, startMs, endMs, node)
}
