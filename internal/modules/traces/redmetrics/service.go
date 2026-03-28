package redmetrics

import "context"

type Service interface {
	GetSummary(teamID int64, startMs, endMs int64) (REDSummary, error)
	GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error)
	GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetRequestRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error)
	GetSpanKindBreakdown(teamID int64, startMs, endMs int64) ([]SpanKindPoint, error)
	GetErrorsByRoute(teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error)
	GetLatencyBreakdown(teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error)
}

type REDMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &REDMetricsService{repo: repo}
}

func (s *REDMetricsService) GetSummary(teamID int64, startMs, endMs int64) (REDSummary, error) {
	rows, err := s.repo.GetSummary(context.Background(), teamID, startMs, endMs)
	if err != nil {
		return REDSummary{}, err
	}

	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	var totalCount, totalErrors int64
	var totalP50, totalP95, totalP99 float64
	for _, row := range rows {
		totalCount += row.TotalCount
		totalErrors += row.ErrorCount
		totalP50 += row.P50Ms
		totalP95 += row.P95Ms
		totalP99 += row.P99Ms
	}
	serviceCount := int64(len(rows))

	avgErrorPct := 0.0
	if totalCount > 0 {
		avgErrorPct = float64(totalErrors) * 100.0 / float64(totalCount)
	}
	avgP50 := 0.0
	avgP95 := 0.0
	avgP99 := 0.0
	if serviceCount > 0 {
		avgP50 = totalP50 / float64(serviceCount)
		avgP95 = totalP95 / float64(serviceCount)
		avgP99 = totalP99 / float64(serviceCount)
	}
	return REDSummary{
		ServiceCount:   serviceCount,
		TotalSpanCount: totalCount,
		TotalRPS:       float64(totalCount) / durationSec,
		AvgErrorPct:    avgErrorPct,
		AvgP50Ms:       avgP50,
		AvgP95Ms:       avgP95,
		AvgP99Ms:       avgP99,
	}, nil
}

func (s *REDMetricsService) GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error) {
	rows, err := s.repo.GetApdex(context.Background(), teamID, startMs, endMs, satisfiedMs, toleratingMs)
	if err != nil {
		return nil, err
	}

	result := make([]ApdexScore, len(rows))
	for i, row := range rows {
		apdex := 0.0
		if row.TotalCount > 0 {
			apdex = (float64(row.Satisfied) + float64(row.Tolerating)*0.5) / float64(row.TotalCount)
		}
		result[i] = ApdexScore{
			ServiceName: row.ServiceName,
			Apdex:       apdex,
			Satisfied:   row.Satisfied,
			Tolerating:  row.Tolerating,
			Frustrated:  row.Frustrated,
			TotalCount:  row.TotalCount,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	return s.repo.GetTopSlowOperations(context.Background(), teamID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	return s.repo.GetTopErrorOperations(context.Background(), teamID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetRequestRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	return s.repo.GetRequestRateTimeSeries(context.Background(), teamID, startMs, endMs)
}

func (s *REDMetricsService) GetErrorRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	return s.repo.GetErrorRateTimeSeries(context.Background(), teamID, startMs, endMs)
}

func (s *REDMetricsService) GetP95LatencyTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	return s.repo.GetP95LatencyTimeSeries(context.Background(), teamID, startMs, endMs)
}

func (s *REDMetricsService) GetSpanKindBreakdown(teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	return s.repo.GetSpanKindBreakdown(context.Background(), teamID, startMs, endMs)
}

func (s *REDMetricsService) GetErrorsByRoute(teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	return s.repo.GetErrorsByRoute(context.Background(), teamID, startMs, endMs)
}

func (s *REDMetricsService) GetLatencyBreakdown(teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error) {
	rows, err := s.repo.GetLatencyBreakdown(context.Background(), teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	var grandTotal float64
	result := make([]LatencyBreakdown, len(rows))
	for i, row := range rows {
		grandTotal += row.TotalMs
		result[i] = LatencyBreakdown{
			ServiceName: row.ServiceName,
			TotalMs:     row.TotalMs,
			SpanCount:   row.SpanCount,
		}
	}
	if grandTotal > 0 {
		for i := range result {
			result[i].PctOfTotal = result[i].TotalMs * 100.0 / grandTotal
		}
	}
	return result, nil
}
