package dashboard

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetAISummary(ctx context.Context, teamID int64, model string, startMs, endMs int64) (*AISummary, error) {
	row, err := s.repo.GetAISummary(ctx, teamID, model, startMs, endMs)
	if err != nil || row == nil {
		return nil, err
	}
	return row.toModel(), nil
}

func (s *Service) GetAIModels(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AIModel, error) {
	rows, err := s.repo.GetAIModels(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAIModelDTOs(rows), nil
}

func (s *Service) GetAIPerformanceMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	rows, err := s.repo.GetAIPerformanceMetrics(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAIPerformanceMetricDTOs(rows), nil
}

func (s *Service) GetAIPerformanceTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	rows, err := s.repo.GetAIPerformanceTimeSeries(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAIPerformanceTimeSeriesDTOs(rows), nil
}

func (s *Service) GetAILatencyHistogram(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	rows, err := s.repo.GetAILatencyHistogram(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAILatencyHistogramDTOs(rows), nil
}

func (s *Service) GetAICostMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AICostMetric, error) {
	rows, err := s.repo.GetAICostMetrics(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAICostMetricDTOs(rows), nil
}

func (s *Service) GetAICostTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AICostTimeSeries, error) {
	rows, err := s.repo.GetAICostTimeSeries(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAICostTimeSeriesDTOs(rows), nil
}

func (s *Service) GetAITokenBreakdown(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AITokenBreakdown, error) {
	rows, err := s.repo.GetAITokenBreakdown(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAITokenBreakdownDTOs(rows), nil
}

func (s *Service) GetAISecurityMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AISecurityMetric, error) {
	rows, err := s.repo.GetAISecurityMetrics(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAISecurityMetricDTOs(rows), nil
}

func (s *Service) GetAISecurityTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	rows, err := s.repo.GetAISecurityTimeSeries(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAISecurityTimeSeriesDTOs(rows), nil
}

func (s *Service) GetAIPiiCategories(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]AIPiiCategory, error) {
	rows, err := s.repo.GetAIPiiCategories(ctx, teamID, model, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapAIPIICategoryDTOs(rows), nil
}
