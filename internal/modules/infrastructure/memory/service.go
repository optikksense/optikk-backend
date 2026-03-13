package memory

type Service interface {
	GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
}

type MemoryService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &MemoryService{repo: repo}
}

func (s *MemoryService) GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetMemoryUsage(teamID, startMs, endMs)
}

func (s *MemoryService) GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetMemoryUsagePercentage(teamID, startMs, endMs)
}

func (s *MemoryService) GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetSwapUsage(teamID, startMs, endMs)
}
