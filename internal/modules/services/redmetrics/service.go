package redmetrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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
		totalCount += int64(row.TotalCount)  //nolint:gosec // domain-bounded
		totalErrors += int64(row.ErrorCount) //nolint:gosec // domain-bounded
		totalP50 += utils.SanitizeFloat(float64(row.P50Ms))
		totalP95 += utils.SanitizeFloat(float64(row.P95Ms))
		totalP99 += utils.SanitizeFloat(float64(row.P99Ms))
	}
	serviceCount := int64(len(rows))

	avgErrorPct := 0.0
	if totalCount > 0 {
		avgErrorPct = float64(totalErrors) * 100.0 / float64(totalCount)
	}
	avgP50, avgP95, avgP99 := 0.0, 0.0, 0.0
	if serviceCount > 0 {
		avgP50 = totalP50 / float64(serviceCount)
		avgP95 = totalP95 / float64(serviceCount)
		avgP99 = totalP99 / float64(serviceCount)
	}
	return REDSummary{
		ServiceCount:   serviceCount,
		TotalSpanCount: totalCount,
		TotalRPS:       utils.SanitizeFloat(float64(totalCount) / durationSec),
		AvgErrorPct:    utils.SanitizeFloat(avgErrorPct),
		AvgP50Ms:       utils.SanitizeFloat(avgP50),
		AvgP95Ms:       utils.SanitizeFloat(avgP95),
		AvgP99Ms:       utils.SanitizeFloat(avgP99),
	}, nil
}

func (s *REDMetricsService) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]ApdexScore, error) {
	var rows []apdexRow
	var err error
	if serviceName != "" {
		rows, err = s.repo.GetApdexByService(ctx, teamID, startMs, endMs, satisfiedMs, toleratingMs, serviceName)
	} else {
		rows, err = s.repo.GetApdex(ctx, teamID, startMs, endMs, satisfiedMs, toleratingMs)
	}
	if err != nil {
		return nil, err
	}

	result := make([]ApdexScore, len(rows))
	for i, row := range rows {
		total := int64(row.TotalCount)         //nolint:gosec // domain-bounded
		satisfied := int64(row.Satisfied)      //nolint:gosec // domain-bounded
		tolerating := int64(row.Tolerating)    //nolint:gosec // domain-bounded
		frustrated := total - satisfied - tolerating
		if frustrated < 0 {
			frustrated = 0
		}
		apdex := 0.0
		if total > 0 {
			apdex = (float64(satisfied) + float64(tolerating)*0.5) / float64(total)
		}
		result[i] = ApdexScore{
			ServiceName: row.ServiceName,
			Apdex:       apdex,
			Satisfied:   satisfied,
			Tolerating:  tolerating,
			Frustrated:  frustrated,
			TotalCount:  total,
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
			OperationName: row.OperationName,
			ServiceName:   row.ServiceName,
			P50Ms:         utils.SanitizeFloat(float64(row.P50Ms)),
			P95Ms:         utils.SanitizeFloat(float64(row.P95Ms)),
			P99Ms:         utils.SanitizeFloat(float64(row.P99Ms)),
			SpanCount:     int64(row.SpanCount), //nolint:gosec // domain-bounded
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
		total := int64(row.TotalCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)  //nolint:gosec // domain-bounded
		rate := 0.0
		if total > 0 {
			rate = float64(errs) / float64(total)
		}
		result[i] = ErrorOperation{
			OperationName: row.OperationName,
			ServiceName:   row.ServiceName,
			ErrorRate:     rate,
			ErrorCount:    errs,
			TotalCount:    total,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	rows, err := s.repo.GetRequestRateTimeSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	bucketSec := float64(timebucket.BucketSeconds)
	result := make([]ServiceRatePoint, len(rows))
	for i, row := range rows {
		result[i] = ServiceRatePoint{
			Timestamp:   row.Timestamp,
			ServiceName: row.ServiceName,
			RPS:         float64(row.RequestCount) / bucketSec,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	rows, err := s.repo.GetErrorRateTimeSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	result := make([]ServiceErrorRatePoint, len(rows))
	for i, row := range rows {
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
		pct := 0.0
		if total > 0 {
			pct = float64(errs) * 100.0 / float64(total)
		}
		result[i] = ServiceErrorRatePoint{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: total,
			ErrorCount:   errs,
			ErrorPct:     pct,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	rows, err := s.repo.GetP95LatencyTimeSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	result := make([]ServiceLatencyPoint, len(rows))
	for i, row := range rows {
		result[i] = ServiceLatencyPoint{
			Timestamp:   row.Timestamp,
			ServiceName: row.ServiceName,
			P95Ms:       utils.SanitizeFloat(float64(row.P95Ms)),
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	rows, err := s.repo.GetSpanKindBreakdown(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	result := make([]SpanKindPoint, len(rows))
	for i, row := range rows {
		result[i] = SpanKindPoint{
			Timestamp:  row.Timestamp,
			KindString: row.KindString,
			SpanCount:  int64(row.SpanCount), //nolint:gosec // domain-bounded
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	rows, err := s.repo.GetErrorsByRoute(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	result := make([]ErrorByRoutePoint, len(rows))
	for i, row := range rows {
		result[i] = ErrorByRoutePoint{
			Timestamp:    row.Timestamp,
			HttpRoute:    row.HTTPRoute,
			RequestCount: int64(row.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:   int64(row.ErrorCount),   //nolint:gosec // domain-bounded
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error) {
	rows, err := s.repo.GetLatencyBreakdown(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	var grandTotal float64
	result := make([]LatencyBreakdown, len(rows))
	for i, row := range rows {
		total := utils.SanitizeFloat(row.TotalMs)
		grandTotal += total
		result[i] = LatencyBreakdown{
			ServiceName: row.ServiceName,
			TotalMs:     total,
			SpanCount:   int64(row.SpanCount), //nolint:gosec // domain-bounded
		}
	}
	if grandTotal > 0 {
		for i := range result {
			result[i].PctOfTotal = result[i].TotalMs * 100.0 / grandTotal
		}
	}
	return result, nil
}
