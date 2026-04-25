package redmetrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Service interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (REDSummary, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]ApdexScore, error)
	GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error)
	GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error)
	GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error)
}

type REDMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &REDMetricsService{repo: repo}
}

func (s *REDMetricsService) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (REDSummary, error) {
	rows, err := s.repo.GetSummary(ctx, teamID, startMs, endMs)
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
		totalCount += int64(row.TotalCount)	//nolint:gosec // domain-bounded
		totalErrors += int64(row.ErrorCount)	//nolint:gosec // domain-bounded
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
		ServiceCount:	serviceCount,
		TotalSpanCount:	totalCount,
		TotalRPS:	utils.SanitizeFloat(float64(totalCount) / durationSec),
		AvgErrorPct:	utils.SanitizeFloat(avgErrorPct),
		AvgP50Ms:	utils.SanitizeFloat(avgP50),
		AvgP95Ms:	utils.SanitizeFloat(avgP95),
		AvgP99Ms:	utils.SanitizeFloat(avgP99),
	}, nil
}

func (s *REDMetricsService) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]ApdexScore, error) {
	rows, err := s.repo.GetApdex(ctx, teamID, startMs, endMs, satisfiedMs, toleratingMs, serviceName)
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
			ServiceName:	row.ServiceName,
			Apdex:		apdex,
			Satisfied:	row.Satisfied,
			Tolerating:	row.Tolerating,
			Frustrated:	row.Frustrated,
			TotalCount:	row.TotalCount,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	rows, err := s.repo.GetTopSlowOperations(ctx, teamID, startMs, endMs, limit)
	if err != nil {
		return nil, err
	}
	result := make([]SlowOperation, len(rows))
	for i, row := range rows {
		result[i] = SlowOperation{
			OperationName:	row.OperationName,
			ServiceName:	row.ServiceName,
			P50Ms:		row.P50Ms,
			P95Ms:		row.P95Ms,
			P99Ms:		row.P99Ms,
			SpanCount:	int64(row.SpanCount),	//nolint:gosec // domain-bounded
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	rows, err := s.repo.GetTopErrorOperations(ctx, teamID, startMs, endMs, limit)
	if err != nil {
		return nil, err
	}
	result := make([]ErrorOperation, len(rows))
	for i, row := range rows {
		result[i] = ErrorOperation{
			OperationName:	row.OperationName,
			ServiceName:	row.ServiceName,
			ErrorRate:	row.ErrorRate,
			ErrorCount:	row.ErrorCount,
			TotalCount:	row.TotalCount,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	return s.repo.GetRequestRateTimeSeries(ctx, teamID, startMs, endMs)
}

func (s *REDMetricsService) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	return s.repo.GetErrorRateTimeSeries(ctx, teamID, startMs, endMs)
}

func (s *REDMetricsService) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	return s.repo.GetP95LatencyTimeSeries(ctx, teamID, startMs, endMs)
}

func (s *REDMetricsService) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	return s.repo.GetSpanKindBreakdown(ctx, teamID, startMs, endMs)
}

func (s *REDMetricsService) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	return s.repo.GetErrorsByRoute(ctx, teamID, startMs, endMs)
}

func (s *REDMetricsService) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error) {
	rows, err := s.repo.GetLatencyBreakdown(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	var grandTotal float64
	result := make([]LatencyBreakdown, len(rows))
	for i, row := range rows {
		grandTotal += row.TotalMs
		result[i] = LatencyBreakdown{
			ServiceName:	row.ServiceName,
			TotalMs:	row.TotalMs,
			SpanCount:	row.SpanCount,
		}
	}
	if grandTotal > 0 {
		for i := range result {
			result[i].PctOfTotal = result[i].TotalMs * 100.0 / grandTotal
		}
	}
	return result, nil
}
