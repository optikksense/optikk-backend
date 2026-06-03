package redmetrics

import (
	"context"
	"math"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Service interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (REDSummary, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]ApdexScore, error)
	GetRequestAndErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServicePerformancePoint, error)
	GetStatusTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]StatusTimeSeriesPoint, error)
	GetLatencyPercentilesTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]LatencyPercentilesPoint, error)
	GetTopEndpointsCombined(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursor TopEndpointsCursor) (PaginatedEndpoints, error)
	GetServiceSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (ServiceSummaryResponse, error)
	GetOperationBaseline(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) (OperationBaseline, error)
}

type REDMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &REDMetricsService{repo: repo}
}

func (s *REDMetricsService) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (REDSummary, error) {
	rows, err := s.repo.GetFleetREDMetrics(ctx, teamID, startMs, endMs)
	if err != nil {
		return REDSummary{}, err
	}

	// Fetch saturation aggregates for the fleet
	metricNames := []string{
		infraconsts.MetricSystemCPUUtilization,
		infraconsts.MetricSystemCPUUsage,
		infraconsts.MetricProcessCPUUsage,
		infraconsts.MetricSystemMemoryUtilization,
		infraconsts.MetricSystemDiskUtilization,
	}
	sats, err := s.repo.GetFleetSaturationAggs(ctx, teamID, startMs, endMs, metricNames)
	if err != nil {
		sats = nil // Fallback gracefully if query fails
	}

	// Map saturation per service
	type satMetrics struct {
		cpuValues []float64
		memVal    float64
		diskVal   float64
		hasMem    bool
		hasDisk   bool
	}
	byService := map[string]*satMetrics{}
	for _, row := range sats {
		if row.Service == "" {
			continue
		}
		sm, ok := byService[row.Service]
		if !ok {
			sm = &satMetrics{}
			byService[row.Service] = sm
		}
		switch row.MetricName {
		case infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage:
			if v := normalizeUtilization(row.Value); v != nil {
				sm.cpuValues = append(sm.cpuValues, *v)
			}
		case infraconsts.MetricSystemMemoryUtilization:
			if v := normalizeUtilization(row.Value); v != nil {
				sm.memVal = *v
				sm.hasMem = true
			}
		case infraconsts.MetricSystemDiskUtilization:
			if v := normalizeUtilization(row.Value); v != nil {
				sm.diskVal = *v
				sm.hasDisk = true
			}
		}
	}

	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	var totalCount, totalErrors int64
	var totalP50, totalP95, totalP99 float64
	services := make([]ServiceREDMetric, len(rows))
	for i, row := range rows {
		var cpuVal, memVal, diskVal float64
		if sm, ok := byService[row.ServiceName]; ok {
			cpuAvg := averageFloats(sm.cpuValues)
			if cpuAvg != nil {
				cpuVal = *cpuAvg
			}
			if sm.hasMem {
				memVal = sm.memVal
			}
			if sm.hasDisk {
				diskVal = sm.diskVal
			}
		}

		services[i] = ServiceREDMetric{
			ServiceName:       row.ServiceName,
			RequestCount:      int64(row.TotalCount),
			ErrorCount:        int64(row.ErrorCount),
			AvgLatency:        utils.SanitizeFloat(float64(row.P50Ms)),
			P95Latency:        utils.SanitizeFloat(float64(row.P95Ms)),
			P99Latency:        utils.SanitizeFloat(float64(row.P99Ms)),
			CPUUtilization:    utils.SanitizeFloat(cpuVal),
			MemoryUtilization: utils.SanitizeFloat(memVal),
			DiskUtilization:   utils.SanitizeFloat(diskVal),
		}
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
		TotalErrors:    totalErrors,
		TotalRPS:       utils.SanitizeFloat(float64(totalCount) / durationSec),
		AvgErrorPct:    utils.SanitizeFloat(avgErrorPct),
		AvgP50Ms:       utils.SanitizeFloat(avgP50),
		AvgP95Ms:       utils.SanitizeFloat(avgP95),
		AvgP99Ms:       utils.SanitizeFloat(avgP99),
		Services:       services,
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
		total := int64(row.TotalCount)      //nolint:gosec // domain-bounded
		satisfied := int64(row.Satisfied)   //nolint:gosec // domain-bounded
		tolerating := int64(row.Tolerating) //nolint:gosec // domain-bounded
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

func (s *REDMetricsService) GetOperationBaseline(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) (OperationBaseline, error) {
	row, err := s.repo.GetOperationBaseline(ctx, teamID, startMs, endMs, serviceName, operationName)
	if err != nil {
		return OperationBaseline{}, err
	}
	return OperationBaseline{
		ServiceName:   serviceName,
		OperationName: operationName,
		P50Ms:         utils.SanitizeFloat(float64(row.P50Ms)),
		P95Ms:         utils.SanitizeFloat(float64(row.P95Ms)),
		P99Ms:         utils.SanitizeFloat(float64(row.P99Ms)),
		SpanCount:     int64(row.SpanCount), //nolint:gosec // domain-bounded
	}, nil
}

func (s *REDMetricsService) GetRequestAndErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServicePerformancePoint, error) {
	rows, err := s.repo.GetRequestAndErrorRateTimeSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	// Divide by the display-grain width (not a fixed 5-min bucket) so RPS stays a
	// true per-second rate after the query coarsens buckets for wider windows.
	grainSec := float64(timebucket.DisplayGrain(endMs - startMs).Seconds())
	if grainSec <= 0 {
		grainSec = 60
	}
	result := make([]ServicePerformancePoint, len(rows))
	for i, row := range rows {
		var errorPct float64
		if row.RequestCount > 0 {
			errorPct = (float64(row.ErrorCount) / float64(row.RequestCount)) * 100.0
		}
		result[i] = ServicePerformancePoint{
			Timestamp:    row.BucketAt,
			ServiceName:  row.ServiceName,
			RPS:          float64(row.RequestCount) / grainSec,
			RequestCount: row.RequestCount,
			ErrorCount:   row.ErrorCount,
			ErrorPct:     utils.SanitizeFloat(errorPct),
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetServiceSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (ServiceSummaryResponse, error) {
	redRow, err := s.repo.GetServiceREDMetrics(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return ServiceSummaryResponse{}, err
	}

	metricNames := []string{
		infraconsts.MetricSystemCPUUtilization,
		infraconsts.MetricSystemCPUUsage,
		infraconsts.MetricProcessCPUUsage,
		infraconsts.MetricSystemMemoryUtilization,
		infraconsts.MetricSystemDiskUtilization,
	}

	sats, err := s.repo.GetServiceSaturationAggs(ctx, teamID, startMs, endMs, serviceName, metricNames)
	if err != nil {
		sats = nil // Fallback gracefully if query fails
	}

	// Map saturation
	var cpuValues []float64
	var memVal float64
	var diskVal float64

	for _, row := range sats {
		switch row.MetricName {
		case infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage:
			if v := normalizeUtilization(row.Value); v != nil {
				cpuValues = append(cpuValues, *v)
			}
		case infraconsts.MetricSystemMemoryUtilization:
			if v := normalizeUtilization(row.Value); v != nil {
				memVal = *v
			}
		case infraconsts.MetricSystemDiskUtilization:
			if v := normalizeUtilization(row.Value); v != nil {
				diskVal = *v
			}
		}
	}

	var cpuVal float64
	cpuAvg := averageFloats(cpuValues)
	if cpuAvg != nil {
		cpuVal = *cpuAvg
	}

	var reqCount, errCount int64
	var rps, errRate float64
	var p50, p95, p99 float64

	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	if redRow != nil {
		reqCount = int64(redRow.TotalCount)
		errCount = int64(redRow.ErrorCount)
		rps = float64(reqCount) / durationSec
		if reqCount > 0 {
			errRate = float64(errCount) * 100.0 / float64(reqCount)
		}
		p50 = utils.SanitizeFloat(float64(redRow.P50Ms))
		p95 = utils.SanitizeFloat(float64(redRow.P95Ms))
		p99 = utils.SanitizeFloat(float64(redRow.P99Ms))
	}

	return ServiceSummaryResponse{
		ServiceName:       serviceName,
		RequestCount:      reqCount,
		ErrorCount:        errCount,
		RPS:               utils.SanitizeFloat(rps),
		ErrorRate:         utils.SanitizeFloat(errRate),
		P50Ms:             p50,
		P95Ms:             p95,
		P99Ms:             p99,
		CPUUtilization:    utils.SanitizeFloat(cpuVal),
		MemoryUtilization: utils.SanitizeFloat(memVal),
		DiskUtilization:   utils.SanitizeFloat(diskVal),
	}, nil
}

func normalizeUtilization(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > infraconsts.PercentageThreshold*100 {
		return nil
	}
	if v <= infraconsts.PercentageThreshold {
		v = v * infraconsts.PercentageMultiplier
	}
	return &v
}

func averageFloats(vals []float64) *float64 {
	if len(vals) == 0 {
		return nil
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	avg := sum / float64(len(vals))
	return &avg
}

// GetStatusTimeSeries pivots per-bucket / per-status-class rows into one
// point per bucket with 2xx / 4xx / 5xx (and "other") counts.
func (s *REDMetricsService) GetStatusTimeSeries(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]StatusTimeSeriesPoint, error) {
	rows, err := s.repo.GetStatusTimeSeries(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	grainSec := float64(timebucket.DisplayGrain(endMs - startMs).Seconds())
	if grainSec <= 0 {
		grainSec = 60
	}
	return pivotStatusRows(rows, grainSec), nil
}

func pivotStatusRows(rows []statusBucketTimeseriesRow, grainSec float64) []StatusTimeSeriesPoint {
	byTs := make(map[int64]*StatusTimeSeriesPoint, len(rows))
	order := make([]int64, 0, len(rows))
	for _, row := range rows {
		key := row.BucketAt.Unix()
		pt, ok := byTs[key]
		if !ok {
			pt = &StatusTimeSeriesPoint{Timestamp: row.BucketAt}
			byTs[key] = pt
			order = append(order, key)
		}
		count := float64(row.RequestCount) / grainSec
		writeStatusCount(pt, row.StatusBucket, count)
	}
	out := make([]StatusTimeSeriesPoint, len(order))
	for i, key := range order {
		out[i] = *byTs[key]
	}
	return out
}

func writeStatusCount(pt *StatusTimeSeriesPoint, bucket string, count float64) {
	switch bucket {
	case "2xx":
		pt.Status2xx += count
	case "4xx":
		pt.Status4xx += count
	case "5xx":
		pt.Status5xx += count
	default:
		pt.StatusOther += count
	}
}

// GetLatencyPercentilesTimeSeries returns p50/p95/p99 over time for one service
// (or for all team services when serviceName is empty).
func (s *REDMetricsService) GetLatencyPercentilesTimeSeries(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]LatencyPercentilesPoint, error) {
	rows, err := s.repo.GetLatencyPercentilesTimeSeries(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	out := make([]LatencyPercentilesPoint, len(rows))
	for i, row := range rows {
		out[i] = LatencyPercentilesPoint{
			Timestamp: row.BucketAt,
			P50Ms:     utils.SanitizeFloat(float64(row.P50Ms)),
			P95Ms:     utils.SanitizeFloat(float64(row.P95Ms)),
			P99Ms:     utils.SanitizeFloat(float64(row.P99Ms)),
		}
	}
	return out, nil
}

// GetTopEndpointsCombined returns per-operation rate / errPct / p50 / p95 / p99
// sorted by request volume.
func (s *REDMetricsService) GetTopEndpointsCombined(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursorIn TopEndpointsCursor,
) (PaginatedEndpoints, error) {
	rows, err := s.repo.GetTopEndpointsCombined(ctx, teamID, startMs, endMs, serviceName, limit+1, cursorIn)
	if err != nil {
		return PaginatedEndpoints{}, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	results := make([]TopEndpoint, len(rows))
	for i, row := range rows {
		results[i] = toTopEndpoint(row, durationSec)
	}

	var nextCursor string
	if hasMore && len(rows) > 0 {
		lastRow := rows[len(rows)-1]
		nextCursor = cursor.Encode(TopEndpointsCursor{
			TotalCount:    lastRow.TotalCount,
			OperationName: lastRow.OperationName,
		})
	}

	return PaginatedEndpoints{
		Results: results,
		PageInfo: PageInfo{
			HasMore:    hasMore,
			NextCursor: nextCursor,
			Limit:      limit,
		},
	}, nil
}

func toTopEndpoint(row topEndpointRow, durationSec float64) TopEndpoint {
	total := int64(row.TotalCount) //nolint:gosec // domain-bounded
	errs := int64(row.ErrorCount)  //nolint:gosec // domain-bounded
	errRate := 0.0
	if total > 0 {
		errRate = float64(errs) / float64(total)
	}
	return TopEndpoint{
		OperationName: row.OperationName,
		ServiceName:   row.ServiceName,
		SpanKind:      row.SpanKind,
		HTTPRoute:     row.HTTPRoute,
		RPS:           float64(total) / durationSec,
		ErrorRate:     errRate,
		ErrorCount:    errs,
		TotalCount:    total,
		P50Ms:         utils.SanitizeFloat(float64(row.P50Ms)),
		P95Ms:         utils.SanitizeFloat(float64(row.P95Ms)),
		P99Ms:         utils.SanitizeFloat(float64(row.P99Ms)),
	}
}
