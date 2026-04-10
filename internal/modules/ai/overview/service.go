package overview

import "context"

// Service defines the business logic contract for the AI overview module.
type Service interface {
	GetSummary(f OverviewFilter) (SummaryDTO, error)
	GetModels(f OverviewFilter) ([]ModelDTO, error)
	GetOperations(f OverviewFilter) ([]OperationDTO, error)
	GetServices(f OverviewFilter) ([]ServiceDTO, error)
	GetModelHealth(f OverviewFilter) ([]ModelHealthDTO, error)
	GetTopSlow(f OverviewFilter, limit int) ([]TopSlowDTO, error)
	GetTopErrors(f OverviewFilter, limit int) ([]TopErrorDTO, error)
	GetFinishReasons(f OverviewFilter) ([]FinishReasonDTO, error)
	GetTimeseriesRequests(f OverviewFilter) ([]TimeseriesPointDTO, error)
	GetTimeseriesLatency(f OverviewFilter) ([]TimeseriesDualPointDTO, error)
	GetTimeseriesErrors(f OverviewFilter) ([]TimeseriesPointDTO, error)
	GetTimeseriesTokens(f OverviewFilter) ([]TimeseriesDualPointDTO, error)
	GetTimeseriesThroughput(f OverviewFilter) ([]TimeseriesPointDTO, error)
	GetTimeseriesCost(f OverviewFilter) ([]TimeseriesPointDTO, error)
}

type overviewService struct {
	repo Repository
}

// NewService creates a new Service backed by the given Repository.
func NewService(repo Repository) Service {
	return &overviewService{repo: repo}
}

func (s *overviewService) GetSummary(f OverviewFilter) (SummaryDTO, error) {
	row, err := s.repo.GetSummary(context.Background(), f)
	if err != nil {
		return SummaryDTO{}, err
	}
	totalTokens := row.TotalInTok + row.TotalOutTok
	durationSec := float64(f.EndMs-f.StartMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}
	tokPerSec := 0.0
	if durationSec > 0 && totalTokens > 0 {
		tokPerSec = float64(totalTokens) / durationSec
	}
	errorRate := 0.0
	if row.TotalCount > 0 {
		errorRate = float64(row.ErrorCount) * 100.0 / float64(row.TotalCount)
	}
	return SummaryDTO{
		TotalRequests:   row.TotalCount,
		ErrorCount:      row.ErrorCount,
		ErrorRate:       errorRate,
		AvgLatencyMs:    row.AvgLatencyMs,
		P50Ms:           row.P50Ms,
		P95Ms:           row.P95Ms,
		P99Ms:           row.P99Ms,
		TotalInputTok:   row.TotalInTok,
		TotalOutputTok:  row.TotalOutTok,
		TotalTokens:     totalTokens,
		AvgTokensPerSec: tokPerSec,
		UniqueModels:    row.UniqueModels,
		UniqueServices:  row.UniqueSvcs,
		UniqueOps:       row.UniqueOps,
	}, nil
}

func (s *overviewService) GetModels(f OverviewFilter) ([]ModelDTO, error) {
	rows, err := s.repo.GetModels(context.Background(), f)
	if err != nil {
		return nil, err
	}
	durationSec := float64(f.EndMs-f.StartMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}
	result := make([]ModelDTO, len(rows))
	for i, r := range rows {
		total := r.InputTokens + r.OutputTokens
		tps := 0.0
		if durationSec > 0 && total > 0 {
			tps = float64(total) / durationSec
		}
		result[i] = ModelDTO{
			Model:        r.Model,
			Provider:     r.Provider,
			RequestCount: r.RequestCount,
			AvgLatencyMs: r.AvgLatencyMs,
			P95Ms:        r.P95Ms,
			ErrorCount:   r.ErrorCount,
			ErrorRate:    r.ErrorRate,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			TotalTokens:  total,
			TokensPerSec: tps,
		}
	}
	return result, nil
}

func (s *overviewService) GetOperations(f OverviewFilter) ([]OperationDTO, error) {
	rows, err := s.repo.GetOperations(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]OperationDTO, len(rows))
	for i, r := range rows {
		result[i] = OperationDTO{
			Operation:    r.Operation,
			RequestCount: r.RequestCount,
			AvgLatencyMs: r.AvgLatencyMs,
			ErrorCount:   r.ErrorCount,
			ErrorRate:    r.ErrorRate,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			TotalTokens:  r.InputTokens + r.OutputTokens,
		}
	}
	return result, nil
}

func (s *overviewService) GetServices(f OverviewFilter) ([]ServiceDTO, error) {
	rows, err := s.repo.GetServices(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]ServiceDTO, len(rows))
	for i, r := range rows {
		result[i] = ServiceDTO{
			ServiceName:  r.ServiceName,
			RequestCount: r.RequestCount,
			Models:       r.Models,
			ErrorCount:   r.ErrorCount,
		}
	}
	return result, nil
}

func (s *overviewService) GetModelHealth(f OverviewFilter) ([]ModelHealthDTO, error) {
	rows, err := s.repo.GetModelHealth(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]ModelHealthDTO, len(rows))
	for i, r := range rows {
		health := "healthy"
		if r.ErrorRate > 10 || r.P95Ms > 5000 {
			health = "critical"
		} else if r.ErrorRate > 3 || r.P95Ms > 3000 {
			health = "degraded"
		}
		result[i] = ModelHealthDTO{
			Model:        r.Model,
			Provider:     r.Provider,
			RequestCount: r.RequestCount,
			AvgLatencyMs: r.AvgLatencyMs,
			P95Ms:        r.P95Ms,
			ErrorRate:    r.ErrorRate,
			Health:       health,
		}
	}
	return result, nil
}

func (s *overviewService) GetTopSlow(f OverviewFilter, limit int) ([]TopSlowDTO, error) {
	rows, err := s.repo.GetTopSlow(context.Background(), f, limit)
	if err != nil {
		return nil, err
	}
	result := make([]TopSlowDTO, len(rows))
	for i, r := range rows {
		result[i] = TopSlowDTO{
			Model:        r.Model,
			Operation:    r.Operation,
			P95Ms:        r.P95Ms,
			RequestCount: r.RequestCount,
		}
	}
	return result, nil
}

func (s *overviewService) GetTopErrors(f OverviewFilter, limit int) ([]TopErrorDTO, error) {
	rows, err := s.repo.GetTopErrors(context.Background(), f, limit)
	if err != nil {
		return nil, err
	}
	result := make([]TopErrorDTO, len(rows))
	for i, r := range rows {
		result[i] = TopErrorDTO{
			Model:        r.Model,
			Operation:    r.Operation,
			ErrorCount:   r.ErrorCount,
			ErrorRate:    r.ErrorRate,
			RequestCount: r.RequestCount,
		}
	}
	return result, nil
}

func (s *overviewService) GetFinishReasons(f OverviewFilter) ([]FinishReasonDTO, error) {
	rows, err := s.repo.GetFinishReasons(context.Background(), f)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, r := range rows {
		total += r.Count
	}
	result := make([]FinishReasonDTO, len(rows))
	for i, r := range rows {
		pct := 0.0
		if total > 0 {
			pct = float64(r.Count) * 100.0 / float64(total)
		}
		result[i] = FinishReasonDTO{
			FinishReason: r.FinishReason,
			Count:        r.Count,
			Percentage:   pct,
		}
	}
	return result, nil
}

// ---- timeseries pass-through ----

func (s *overviewService) GetTimeseriesRequests(f OverviewFilter) ([]TimeseriesPointDTO, error) {
	rows, err := s.repo.GetTimeseriesRequests(context.Background(), f)
	return toTSDTO(rows), err
}

func (s *overviewService) GetTimeseriesLatency(f OverviewFilter) ([]TimeseriesDualPointDTO, error) {
	rows, err := s.repo.GetTimeseriesLatency(context.Background(), f)
	return toDualDTO(rows), err
}

func (s *overviewService) GetTimeseriesErrors(f OverviewFilter) ([]TimeseriesPointDTO, error) {
	rows, err := s.repo.GetTimeseriesErrors(context.Background(), f)
	return toTSDTO(rows), err
}

func (s *overviewService) GetTimeseriesTokens(f OverviewFilter) ([]TimeseriesDualPointDTO, error) {
	rows, err := s.repo.GetTimeseriesTokens(context.Background(), f)
	return toDualDTO(rows), err
}

func (s *overviewService) GetTimeseriesThroughput(f OverviewFilter) ([]TimeseriesPointDTO, error) {
	rows, err := s.repo.GetTimeseriesThroughput(context.Background(), f)
	return toTSDTO(rows), err
}

func (s *overviewService) GetTimeseriesCost(f OverviewFilter) ([]TimeseriesPointDTO, error) {
	rows, err := s.repo.GetTimeseriesCost(context.Background(), f)
	return toTSDTO(rows), err
}

// ---- converters ----

func toTSDTO(rows []timeseriesRow) []TimeseriesPointDTO {
	result := make([]TimeseriesPointDTO, len(rows))
	for i, r := range rows {
		result[i] = TimeseriesPointDTO{Timestamp: r.Timestamp, Series: r.Series, Value: r.Value}
	}
	return result
}

func toDualDTO(rows []timeseriesDualRow) []TimeseriesDualPointDTO {
	result := make([]TimeseriesDualPointDTO, len(rows))
	for i, r := range rows {
		result[i] = TimeseriesDualPointDTO{Timestamp: r.Timestamp, Series: r.Series, Value1: r.Value1, Value2: r.Value2}
	}
	return result
}
