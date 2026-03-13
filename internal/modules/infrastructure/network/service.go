package network

type Service interface {
	GetNetworkIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkPackets(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkErrors(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkDropped(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetNetworkConnections(teamID int64, startMs, endMs int64) ([]StateBucket, error)
}

type NetworkService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &NetworkService{repo: repo}
}

func (s *NetworkService) GetNetworkIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkIO(teamID, startMs, endMs)
}

func (s *NetworkService) GetNetworkPackets(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkPackets(teamID, startMs, endMs)
}

func (s *NetworkService) GetNetworkErrors(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkErrors(teamID, startMs, endMs)
}

func (s *NetworkService) GetNetworkDropped(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetNetworkDropped(teamID, startMs, endMs)
}

func (s *NetworkService) GetNetworkConnections(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkConnections(teamID, startMs, endMs)
}
