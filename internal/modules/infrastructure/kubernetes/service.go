package kubernetes

// Service encapsulates the business logic for the Kubernetes module.
type Service interface {
	GetContainerCPU(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetCPUThrottling(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetContainerMemory(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetOOMKills(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetPodRestarts(teamUUID string, startMs, endMs int64) ([]PodStat, error)
	GetNodeAllocatable(teamUUID string, startMs, endMs int64) (NodeAllocatable, error)
	GetPodPhases(teamUUID string, startMs, endMs int64) ([]PhaseStat, error)
	GetReplicaStatus(teamUUID string, startMs, endMs int64) ([]ReplicaStat, error)
	GetVolumeUsage(teamUUID string, startMs, endMs int64) ([]VolumeStat, error)
}

// KubernetesService implements Service.
type KubernetesService struct {
	repo Repository
}

// NewService creates a new KubernetesService.
func NewService(repo Repository) Service {
	return &KubernetesService{repo: repo}
}

func (s *KubernetesService) GetContainerCPU(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetContainerCPU(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetCPUThrottling(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetCPUThrottling(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetContainerMemory(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetContainerMemory(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetOOMKills(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return s.repo.GetOOMKills(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetPodRestarts(teamUUID string, startMs, endMs int64) ([]PodStat, error) {
	return s.repo.GetPodRestarts(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetNodeAllocatable(teamUUID string, startMs, endMs int64) (NodeAllocatable, error) {
	return s.repo.GetNodeAllocatable(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetPodPhases(teamUUID string, startMs, endMs int64) ([]PhaseStat, error) {
	return s.repo.GetPodPhases(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetReplicaStatus(teamUUID string, startMs, endMs int64) ([]ReplicaStat, error) {
	return s.repo.GetReplicaStatus(teamUUID, startMs, endMs)
}

func (s *KubernetesService) GetVolumeUsage(teamUUID string, startMs, endMs int64) ([]VolumeStat, error) {
	return s.repo.GetVolumeUsage(teamUUID, startMs, endMs)
}
