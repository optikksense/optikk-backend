package redmetrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// GetStatusTimeSeries pivots per-bucket / per-status-class rows into one
// point per bucket with 2xx / 4xx / 5xx (and "other") counts.
func (s *REDMetricsService) GetStatusTimeSeries(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]StatusTimeSeriesPoint, error) {
	rows, err := s.repo.GetStatusTimeSeries(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return pivotStatusRows(rows), nil
}

func pivotStatusRows(rows []statusBucketTimeseriesRow) []StatusTimeSeriesPoint {
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
		count := int64(row.RequestCount) //nolint:gosec // domain-bounded
		writeStatusCount(pt, row.StatusBucket, count)
	}
	out := make([]StatusTimeSeriesPoint, len(order))
	for i, key := range order {
		out[i] = *byTs[key]
	}
	return out
}

func writeStatusCount(pt *StatusTimeSeriesPoint, bucket string, count int64) {
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
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int,
) ([]TopEndpoint, error) {
	rows, err := s.repo.GetTopEndpointsCombined(ctx, teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		return nil, err
	}
	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}
	out := make([]TopEndpoint, len(rows))
	for i, row := range rows {
		out[i] = toTopEndpoint(row, durationSec)
	}
	return out, nil
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
