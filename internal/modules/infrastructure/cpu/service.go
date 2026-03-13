package cpu

type Service interface {
	GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error)
	GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
}

type CPUService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &CPUService{repo: repo}
}

func (s *CPUService) GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetCPUTime(teamID, startMs, endMs)
}

func (s *CPUService) GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetCPUUsagePercentage(teamID, startMs, endMs)
}

func (s *CPUService) GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error) {
	return s.repo.GetLoadAverage(teamID, startMs, endMs)
}

func (s *CPUService) GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCount(teamID, startMs, endMs)
}
