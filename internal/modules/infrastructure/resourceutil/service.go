package resourceutil //nolint:misspell

type Service interface {
	GetAvgCPU(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]InstanceResource, error)
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
