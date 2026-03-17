package errors

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetServiceErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	rows, err := s.repo.GetServiceErrorRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapServiceErrorRateRows(rows), nil
}

func (s *Service) GetErrorVolume(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	rows, err := s.repo.GetErrorVolume(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapErrorVolumeRows(rows), nil
}

func (s *Service) GetLatencyDuringErrorWindows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	rows, err := s.repo.GetLatencyDuringErrorWindows(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapLatencyErrorRows(rows), nil
}

func (s *Service) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	rows, err := s.repo.GetErrorGroups(ctx, teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		return nil, err
	}
	return mapErrorGroupRows(rows), nil
}

func (s *Service) GetErrorGroupDetail(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*ErrorGroupDetail, error) {
	row, err := s.repo.GetErrorGroupDetail(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	return mapErrorGroupDetailRow(groupID, row), nil
}

func (s *Service) GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]ErrorGroupTrace, error) {
	rows, err := s.repo.GetErrorGroupTraces(ctx, teamID, startMs, endMs, groupID, limit)
	if err != nil {
		return nil, err
	}
	return mapErrorGroupTraceRows(rows), nil
}

func (s *Service) GetErrorGroupTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]TimeSeriesPoint, error) {
	rows, err := s.repo.GetErrorGroupTimeseries(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	return mapErrorGroupTimeseriesRows(rows), nil
}
