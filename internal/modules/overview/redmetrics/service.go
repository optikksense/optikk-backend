package redmetrics

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
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
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &REDMetricsService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *REDMetricsService) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (REDSummary, error) {
	rows, err := s.repo.GetSummary(ctx, teamID, startMs, endMs)
	if err != nil {
		slog.Error("redmetrics: GetSummary failed", slog.Any("error", err), slog.Int64("team_id", teamID))
		return REDSummary{}, err
	}

	if s.sketchQ != nil && len(rows) > 0 {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, 0.5, 0.95, 0.99)
		for i := range rows {
			dim := sketch.DimSpanService(rows[i].ServiceName)
			if v, ok := pcts[dim]; ok && len(v) == 3 {
				rows[i].P50Ms = v[0]
				rows[i].P95Ms = v[1]
				rows[i].P99Ms = v[2]
			}
		}
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

func (s *REDMetricsService) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]ApdexScore, error) {
	rows, err := s.repo.GetApdex(ctx, teamID, startMs, endMs, satisfiedMs, toleratingMs, serviceName)
	if err != nil {
		slog.Error("redmetrics: GetApdex failed", slog.Any("error", err), slog.Int64("team_id", teamID))
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

func (s *REDMetricsService) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	// Fetch a generous candidate set from CH (by traffic), then re-rank in Go
	// by p95 after percentiles are merged from the endpoint-level sketch.
	// We overshoot so that the final top-N by p95 is stable even when the
	// highest-traffic operations are not the slowest.
	candidateLimit := limit
	if candidateLimit < 1 {
		candidateLimit = 1
	}
	if candidateLimit < 100 {
		candidateLimit = candidateLimit * 5
	}
	rows, err := s.repo.GetTopSlowOperations(ctx, teamID, startMs, endMs, candidateLimit)
	if err != nil {
		slog.Error("redmetrics: GetTopSlowOperations failed", slog.Any("error", err), slog.Int64("team_id", teamID))
		return nil, err
	}

	if s.sketchQ != nil && len(rows) > 0 {
		// SpanLatencyEndpoint dim is service|operation|endpoint|method; collapse
		// all endpoint/method variants under each (service, operation) by
		// prefix-matching "service|operation|".
		prefixes := make([]string, 0, len(rows))
		seen := make(map[string]struct{}, len(rows))
		for _, r := range rows {
			p := r.ServiceName + "|" + r.OperationName + "|"
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			prefixes = append(prefixes, p)
		}
		pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyEndpoint, teamIDString(teamID), startMs, endMs, prefixes, 0.5, 0.95, 0.99)
		for i := range rows {
			p := rows[i].ServiceName + "|" + rows[i].OperationName + "|"
			if v, ok := pcts[p]; ok && len(v) == 3 {
				rows[i].P50Ms = v[0]
				rows[i].P95Ms = v[1]
				rows[i].P99Ms = v[2]
			}
		}
	}

	// Final sort by p95 desc; ties broken by span_count desc.
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].P95Ms == rows[j].P95Ms {
			return rows[i].SpanCount > rows[j].SpanCount
		}
		return rows[i].P95Ms > rows[j].P95Ms
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}

	result := make([]SlowOperation, len(rows))
	for i, row := range rows {
		result[i] = SlowOperation{
			OperationName: row.OperationName,
			ServiceName:   row.ServiceName,
			P50Ms:         row.P50Ms,
			P95Ms:         row.P95Ms,
			P99Ms:         row.P99Ms,
			SpanCount:     row.SpanCount,
		}
	}
	return result, nil
}

func (s *REDMetricsService) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	rows, err := s.repo.GetTopErrorOperations(ctx, teamID, startMs, endMs, limit)
	if err != nil {
		slog.Error("redmetrics: GetTopErrorOperations failed", slog.Any("error", err), slog.Int64("team_id", teamID))
		return nil, err
	}
	result := make([]ErrorOperation, len(rows))
	for i, row := range rows {
		result[i] = ErrorOperation{
			OperationName: row.OperationName,
			ServiceName:   row.ServiceName,
			ErrorRate:     row.ErrorRate,
			ErrorCount:    row.ErrorCount,
			TotalCount:    row.TotalCount,
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

// GetP95LatencyTimeSeries builds (service, bucket) coverage from CH, then
// overlays p95 values from the sketch timeseries for the same range. Buckets
// that exist in CH but have no sketch coverage (typical for cold starts) stay
// with p95=0 so the chart still draws the traffic line.
func (s *REDMetricsService) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	rows, err := s.repo.GetP95LatencyTimeSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return []ServiceLatencyPoint{}, nil
	}

	// Build sketch timeseries keyed by dim → bucket start (unix seconds) → p95.
	type key struct {
		dim string
		ts  int64
	}
	valueByKey := make(map[key]float64)
	if s.sketchQ != nil {
		stepSecs := stepSecondsFor(startMs, endMs)
		tsByDim, _ := s.sketchQ.PercentilesTimeseries(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, stepSecs, 0.95)
		for dim, points := range tsByDim {
			for _, p := range points {
				if len(p.Values) > 0 {
					valueByKey[key{dim: dim, ts: p.BucketTs}] = p.Values[0]
				}
			}
		}
	}

	out := make([]ServiceLatencyPoint, 0, len(rows))
	for _, r := range rows {
		dim := sketch.DimSpanService(r.ServiceName)
		ts := r.Timestamp.Unix()
		p95 := valueByKey[key{dim: dim, ts: ts}]
		out = append(out, ServiceLatencyPoint{
			Timestamp:   r.Timestamp,
			ServiceName: r.ServiceName,
			P95Ms:       p95,
		})
	}
	return out, nil
}

// stepSecondsFor mirrors timebucket.ExprForColumnTime so the sketch timeseries
// aligns with the CH bucket column (1m / 5m / 1h / 1d).
func stepSecondsFor(startMs, endMs int64) int64 {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return 60
	case h <= 24:
		return 300
	case h <= 168:
		return 3600
	default:
		return 86400
	}
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
		slog.Error("redmetrics: GetLatencyBreakdown failed", slog.Any("error", err), slog.Int64("team_id", teamID))
		return nil, err
	}

	// avg = sum/count in Go (div-by-zero guarded); TotalMs preserves the legacy
	// field meaning (per-service mean latency ms) so the downstream pct-of-total
	// math is unchanged.
	var grandTotal float64
	result := make([]LatencyBreakdown, len(rows))
	for i, row := range rows {
		avg := 0.0
		if row.SpanCount > 0 {
			avg = row.TotalMsSum / float64(row.SpanCount)
		}
		grandTotal += avg
		result[i] = LatencyBreakdown{
			ServiceName: row.ServiceName,
			TotalMs:     avg,
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
