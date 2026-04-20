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

// Migrated from errortracking

func (s *Service) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error) {
	return s.repo.GetExceptionRateByType(ctx, teamID, startMs, endMs, serviceName)
}

func (s *Service) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error) {
	return s.repo.GetErrorHotspot(ctx, teamID, startMs, endMs)
}

func (s *Service) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error) {
	return s.repo.GetHTTP5xxByRoute(ctx, teamID, startMs, endMs, serviceName)
}

// Migrated from errorfingerprint

func (s *Service) ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorFingerprint, error) {
	return s.repo.ListFingerprints(ctx, teamID, startMs, endMs, serviceName, limit)
}

func (s *Service) GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]FingerprintTrendPoint, error) {
	return s.repo.GetFingerprintTrend(ctx, teamID, startMs, endMs, serviceName, operationName, exceptionType, statusMessage)
}
