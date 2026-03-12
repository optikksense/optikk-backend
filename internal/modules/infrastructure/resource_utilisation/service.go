package resource_utilisation

type Service interface {
	GetAvgCPU(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]InstanceResource, error)

	// System infrastructure metrics
	GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error)
	GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetNetworkIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkPackets(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkErrors(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkDropped(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error)
	GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkConnections(teamID int64, startMs, endMs int64) ([]StateBucket, error)

	// JVM runtime metrics
	GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error)
	GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error)
	GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error)
	GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error)
}

type ResourceUtilisationService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &ResourceUtilisationService{repo: repo}
}

func (s *ResourceUtilisationService) GetAvgCPU(teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgCPU(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgMemory(teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgMemory(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgNetwork(teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgNetwork(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgConnPool(teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgConnPool(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetCPUUsagePercentage(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetMemoryUsagePercentage(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]ServiceResource, error) {
	return s.repo.GetResourceUsageByService(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]InstanceResource, error) {
	return s.repo.GetResourceUsageByInstance(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetCPUTime(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetMemoryUsage(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetSwapUsage(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskIO(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskOperations(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetDiskIOTime(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error) {
	return s.repo.GetFilesystemUsage(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetFilesystemUtilization(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkIO(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkPackets(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkPackets(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkErrors(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkErrors(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkDropped(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetNetworkDropped(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error) {
	return s.repo.GetLoadAverage(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCount(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetNetworkConnections(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkConnections(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	return s.repo.GetJVMMemory(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetJVMGCDuration(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetJVMGCCollections(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetJVMThreadCount(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	return s.repo.GetJVMClasses(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	return s.repo.GetJVMCPU(teamID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	return s.repo.GetJVMBuffers(teamID, startMs, endMs)
}
