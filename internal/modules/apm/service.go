package apm

// Service encapsulates the business logic for APM metrics.
type Service interface {
	GetRPCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetRPCRequestRate(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetMessagingPublishDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetProcessCPU(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetProcessMemory(teamID int64, startMs, endMs int64) (ProcessMemory, error)
	GetOpenFDs(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetUptime(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
}

// APMService implements Service.
type APMService struct {
	repo Repository
}

// NewService creates a new APMService.
func NewService(repo Repository) Service {
	return &APMService{repo: repo}
}

func (s *APMService) GetRPCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRPCDuration(teamID, startMs, endMs)
}

func (s *APMService) GetRPCRequestRate(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetRPCRequestRate(teamID, startMs, endMs)
}

func (s *APMService) GetMessagingPublishDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetMessagingPublishDuration(teamID, startMs, endMs)
}

func (s *APMService) GetProcessCPU(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCPU(teamID, startMs, endMs)
}

func (s *APMService) GetProcessMemory(teamID int64, startMs, endMs int64) (ProcessMemory, error) {
	return s.repo.GetProcessMemory(teamID, startMs, endMs)
}

func (s *APMService) GetOpenFDs(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetOpenFDs(teamID, startMs, endMs)
}

func (s *APMService) GetUptime(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetUptime(teamID, startMs, endMs)
}
