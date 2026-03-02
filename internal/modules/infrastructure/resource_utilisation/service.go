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
