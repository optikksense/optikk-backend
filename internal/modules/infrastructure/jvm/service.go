package jvm

type Service interface {
	GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error)
	GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error)
	GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error)
	GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error)
}

type JVMService struct {
	repo Repository
}

func NewService(repo Repository) *JVMService {
	return &JVMService{repo: repo}
}

func (s *JVMService) GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	return s.repo.GetJVMMemory(teamID, startMs, endMs)
}

func (s *JVMService) GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetJVMGCDuration(teamID, startMs, endMs)
}

func (s *JVMService) GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetJVMGCCollections(teamID, startMs, endMs)
}

func (s *JVMService) GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetJVMThreadCount(teamID, startMs, endMs)
}

func (s *JVMService) GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	return s.repo.GetJVMClasses(teamID, startMs, endMs)
}

func (s *JVMService) GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	return s.repo.GetJVMCPU(teamID, startMs, endMs)
}

func (s *JVMService) GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	return s.repo.GetJVMBuffers(teamID, startMs, endMs)
}
