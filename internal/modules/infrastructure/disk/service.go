package disk

type Service interface {
	GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error)
	GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
}

type DiskService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &DiskService{repo: repo}
}

func (s *DiskService) GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskIO(teamID, startMs, endMs)
}

func (s *DiskService) GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetDiskOperations(teamID, startMs, endMs)
}

func (s *DiskService) GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetDiskIOTime(teamID, startMs, endMs)
}

func (s *DiskService) GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error) {
	return s.repo.GetFilesystemUsage(teamID, startMs, endMs)
}

func (s *DiskService) GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetFilesystemUtilization(teamID, startMs, endMs)
}
