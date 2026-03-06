package resource_utilisation

// Service encapsulates the business logic for the resource utilisation module.
type Service interface {
	GetAvgCPU(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]InstanceResource, error)

	// System infrastructure metrics
	GetCPUTime(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetMemoryUsage(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetSwapUsage(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetDiskIO(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskOperations(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskIOTime(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetFilesystemUsage(teamUUID string, startMs, endMs int64) ([]MountpointBucket, error)
	GetFilesystemUtilization(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetNetworkIO(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkPackets(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkErrors(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkDropped(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetLoadAverage(teamUUID string, startMs, endMs int64) (LoadAverageResult, error)
	GetProcessCount(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkConnections(teamUUID string, startMs, endMs int64) ([]StateBucket, error)

	// JVM runtime metrics
	GetJVMMemory(teamUUID string, startMs, endMs int64) ([]JVMMemoryBucket, error)
	GetJVMGCDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetJVMGCCollections(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetJVMThreadCount(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetJVMClasses(teamUUID string, startMs, endMs int64) (JVMClassStats, error)
	GetJVMCPU(teamUUID string, startMs, endMs int64) (JVMCPUStats, error)
	GetJVMBuffers(teamUUID string, startMs, endMs int64) ([]JVMBufferBucket, error)
}

// ResourceUtilisationService provides business logic orchestration.
type ResourceUtilisationService struct {
	repo Repository
}

// NewService creates a new ResourceUtilisationService.
func NewService(repo Repository) Service {
	return &ResourceUtilisationService{repo: repo}
}

func (s *ResourceUtilisationService) GetAvgCPU(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgCPU(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgMemory(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgMemory(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgNetwork(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgNetwork(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgConnPool(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgConnPool(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetCPUUsagePercentage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetMemoryUsagePercentage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]ServiceResource, error) {
	return s.repo.GetResourceUsageByService(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]InstanceResource, error) {
	return s.repo.GetResourceUsageByInstance(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetCPUTime(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetCPUTime(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetMemoryUsage(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetMemoryUsage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetSwapUsage(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetSwapUsage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetDiskIO(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskIO(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetDiskOperations(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskOperations(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetDiskIOTime(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetDiskIOTime(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetFilesystemUsage(teamUUID string, startMs, endMs int64) ([]MountpointBucket, error) {
	return s.repo.GetFilesystemUsage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetFilesystemUtilization(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetFilesystemUtilization(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkIO(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkIO(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkPackets(teamUUID string, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkPackets(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkErrors(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkErrors(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkDropped(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetNetworkDropped(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetLoadAverage(teamUUID string, startMs, endMs int64) (LoadAverageResult, error) {
	return s.repo.GetLoadAverage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetProcessCount(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCount(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkConnections(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkConnections(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMMemory(teamUUID string, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	return s.repo.GetJVMMemory(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMGCDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetJVMGCDuration(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMGCCollections(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetJVMGCCollections(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMThreadCount(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetJVMThreadCount(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMClasses(teamUUID string, startMs, endMs int64) (JVMClassStats, error) {
	return s.repo.GetJVMClasses(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMCPU(teamUUID string, startMs, endMs int64) (JVMCPUStats, error) {
	return s.repo.GetJVMCPU(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMBuffers(teamUUID string, startMs, endMs int64) ([]JVMBufferBucket, error) {
	return s.repo.GetJVMBuffers(teamUUID, startMs, endMs)
}
