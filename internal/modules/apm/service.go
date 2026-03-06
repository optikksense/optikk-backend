package apm

// Service encapsulates the business logic for APM metrics.
type Service interface {
	GetRPCDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetRPCRequestRate(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
	GetMessagingPublishDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetProcessCPU(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetProcessMemory(teamUUID string, startMs, endMs int64) (ProcessMemory, error)
	GetOpenFDs(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
	GetUptime(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
}

// APMService implements Service.
type APMService struct {
	repo Repository
}

// NewService creates a new APMService.
func NewService(repo Repository) Service {
	return &APMService{repo: repo}
}

func (s *APMService) GetRPCDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRPCDuration(teamUUID, startMs, endMs)
}

func (s *APMService) GetRPCRequestRate(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetRPCRequestRate(teamUUID, startMs, endMs)
}

func (s *APMService) GetMessagingPublishDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetMessagingPublishDuration(teamUUID, startMs, endMs)
}

func (s *APMService) GetProcessCPU(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCPU(teamUUID, startMs, endMs)
}

func (s *APMService) GetProcessMemory(teamUUID string, startMs, endMs int64) (ProcessMemory, error) {
	return s.repo.GetProcessMemory(teamUUID, startMs, endMs)
}

func (s *APMService) GetOpenFDs(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetOpenFDs(teamUUID, startMs, endMs)
}

func (s *APMService) GetUptime(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetUptime(teamUUID, startMs, endMs)
}
