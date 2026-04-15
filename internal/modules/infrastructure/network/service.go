package network

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetNetworkIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkIO(ctx, teamID, startMs, endMs)
}

func (s *Service) GetNetworkPackets(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	return s.repo.GetNetworkPackets(ctx, teamID, startMs, endMs)
}

func (s *Service) GetNetworkErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkErrors(ctx, teamID, startMs, endMs)
}

func (s *Service) GetNetworkDropped(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetNetworkDropped(ctx, teamID, startMs, endMs)
}

func (s *Service) GetNetworkConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetNetworkConnections(ctx, teamID, startMs, endMs)
}

func (s *Service) GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgNetwork(ctx, teamID, startMs, endMs)
}

func (s *Service) GetNetworkByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return s.repo.GetNetworkByService(ctx, teamID, serviceName, startMs, endMs)
}

func (s *Service) GetNetworkByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return s.repo.GetNetworkByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
