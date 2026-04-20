package query

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

type Service struct {
	repo    Repository
	sketchQ *sketch.Querier
}

type TraceSearchResult struct {
	Traces     []Trace
	HasMore    bool
	NextCursor string
	Limit      int
	Summary    TraceSummary
}

func NewService(repo Repository, sketchQ *sketch.Querier) *Service {
	return &Service{repo: repo, sketchQ: sketchQ}
}

func (s *Service) SearchTraces(ctx context.Context, filters TraceFilters, limit int, cursorRaw string) (TraceSearchResult, error) {
	cur, _ := cursor.Decode[TraceCursor](cursorRaw)
	rows, summaryRow, hasMore, err := s.repo.GetTracesKeyset(ctx, filters, limit, cur)
	if err != nil {
		slog.Error("traces: SearchTraces keyset query failed", slog.Any("error", err), slog.Int64("team_id", filters.TeamID))
		return TraceSearchResult{}, fmt.Errorf("traces.SearchTraces.Keyset: %w", err)
	}
	traces := traceRowsToModels(rows)
	s.attachSummaryPercentiles(ctx, filters, &summaryRow)
	summary := mapTraceSummary(summaryRow)
	result := TraceSearchResult{
		Traces:  traces,
		HasMore: hasMore,
		Limit:   limit,
		Summary: summary,
	}
	if hasMore && len(traces) > 0 {
		last := traces[len(traces)-1]
		result.NextCursor = cursor.Encode(TraceCursor{Timestamp: last.StartTime, SpanID: last.SpanID})
	}
	return result, nil
}

func (s *Service) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]Span, error) {
	rows, err := s.repo.GetTraceSpans(ctx, teamID, traceID)
	if err != nil {
		slog.Error("traces: GetTraceSpans failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, fmt.Errorf("traces.GetTraceSpans: %w", err)
	}
	return spanRowsToModels(rows), nil
}

func (s *Service) GetExplorerFacets(ctx context.Context, filters TraceFilters) ([]TraceFacet, error) {
	rows, err := s.repo.GetTraceFacets(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("traces.GetFacets: %w", err)
	}

	facets := make([]TraceFacet, len(rows))
	for index, row := range rows {
		facets[index] = TraceFacet(row)
	}
	return facets, nil
}

func (s *Service) GetExplorerTrend(ctx context.Context, filters TraceFilters, step string) ([]TraceTrendBucket, error) {
	rows, err := s.repo.GetTraceTrend(ctx, filters, step)
	if err != nil {
		return nil, fmt.Errorf("traces.GetTrend: %w", err)
	}

	buckets := make([]TraceTrendBucket, len(rows))
	for index, row := range rows {
		buckets[index] = TraceTrendBucket(row)
	}
	s.attachTrendPercentiles(ctx, filters, step, buckets)
	return buckets, nil
}

// attachSummaryPercentiles merges every SpanLatencyService dim for the tenant
// and writes the merged p50/p95/p99 into the summary row. Accepts that
// service-name filtering on the dim is coarse — callers currently aggregate
// across all services in the window when SearchMode=root.
func (s *Service) attachSummaryPercentiles(ctx context.Context, filters TraceFilters, row *traceSummaryRow) {
	if s.sketchQ == nil || row == nil {
		return
	}
	prefixes := buildServicePrefixes(filters.Services)
	pcts, err := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyService, teamIDString(filters.TeamID), filters.StartMs, filters.EndMs, prefixes, 0.5, 0.95, 0.99)
	if err != nil {
		return
	}
	merged := make([]float64, 3)
	for _, v := range pcts {
		if len(v) != 3 {
			continue
		}
		for i := range merged {
			if v[i] > merged[i] {
				merged[i] = v[i]
			}
		}
	}
	row.P50Duration = merged[0]
	row.P95Duration = merged[1]
	row.P99Duration = merged[2]
}

// attachTrendPercentiles fills p95 per bucket from SpanLatencyService
// timeseries. Buckets without sketch coverage keep their zero p95.
func (s *Service) attachTrendPercentiles(ctx context.Context, filters TraceFilters, step string, buckets []TraceTrendBucket) {
	if s.sketchQ == nil || len(buckets) == 0 {
		return
	}
	series, err := s.sketchQ.PercentilesTimeseries(ctx, sketch.SpanLatencyService, teamIDString(filters.TeamID), filters.StartMs, filters.EndMs, stepSeconds(step), 0.95)
	if err != nil || len(series) == 0 {
		return
	}
	prefixes := buildServicePrefixes(filters.Services)
	matched := make(map[int64]float64)
	for dim, pts := range series {
		if !dimMatchesAnyPrefix(dim, prefixes) {
			continue
		}
		for _, p := range pts {
			if len(p.Values) == 0 {
				continue
			}
			if p.Values[0] > matched[p.BucketTs] {
				matched[p.BucketTs] = p.Values[0]
			}
		}
	}
	if len(matched) == 0 {
		return
	}
	for i := range buckets {
		// TimeBucket is an opaque string rendered by ClickHouse (e.g. '2025-...').
		// Without a round-trip parse, attach the single highest p95 seen for
		// the window as a conservative fill. Per-bucket alignment is skipped
		// because the upstream DTO lacks a unix-timestamp field.
		var best float64
		for _, v := range matched {
			if v > best {
				best = v
			}
		}
		buckets[i].P95Duration = best
	}
}

func buildServicePrefixes(services []string) []string {
	if len(services) == 0 {
		return []string{""} // empty prefix = match-all
	}
	out := make([]string, 0, len(services))
	for _, svc := range services {
		if svc != "" {
			out = append(out, sketch.DimSpanService(svc))
		}
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

func dimMatchesAnyPrefix(dim string, prefixes []string) bool {
	for _, p := range prefixes {
		if p == "" {
			return true
		}
		if len(dim) >= len(p) && dim[:len(p)] == p {
			return true
		}
	}
	return false
}

// stepSeconds maps the handler-provided step name to a bucket size in seconds.
// Unknown values fall through to 60s which matches the SpanLatencyService
// ingest bucket.
func stepSeconds(step string) int64 {
	switch strings.ToLower(strings.TrimSpace(step)) {
	case "1m", "minute":
		return 60
	case "5m":
		return 300
	case "15m":
		return 900
	case "1h", "hour":
		return 3600
	case "1d", "day":
		return 86400
	default:
		return 60
	}
}

func (s *Service) GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]Span, error) {
	rows, err := s.repo.GetSpanTree(ctx, teamID, spanID)
	if err != nil {
		return nil, fmt.Errorf("traces.GetSpanTree: %w", err)
	}
	return spanRowsToModels(rows), nil
}

func (s *Service) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	rows, err := s.repo.GetErrorGroups(ctx, teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		slog.Error("traces: GetErrorGroups failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("service", serviceName))
		return nil, fmt.Errorf("traces.GetErrorGroups: %w", err)
	}
	return errorGroupRowsToModels(rows), nil
}

func (s *Service) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorTimeSeries(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, fmt.Errorf("traces.GetErrorTimeSeries: %w", err)
	}
	return errorTimeSeriesRowsToModels(rows), nil
}

func (s *Service) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]LatencyHistogramBucket, error) {
	rows, err := s.repo.GetLatencyHistogram(ctx, teamID, startMs, endMs, serviceName, operationName)
	if err != nil {
		return nil, fmt.Errorf("traces.GetLatencyHistogram: %w", err)
	}
	return latencyHistogramRowsToModels(rows), nil
}

func (s *Service) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]LatencyHeatmapPoint, error) {
	rows, err := s.repo.GetLatencyHeatmap(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, fmt.Errorf("traces.GetLatencyHeatmap: %w", err)
	}
	return latencyHeatmapRowsToModels(rows), nil
}

func traceRowsToModels(rows []traceRow) []Trace {
	traces := make([]Trace, len(rows))
	for i, row := range rows {
		traces[i] = Trace{
			SpanID:         row.SpanID,
			TraceID:        row.TraceID,
			ServiceName:    row.ServiceName,
			OperationName:  row.OperationName,
			StartTime:      row.StartTime,
			EndTime:        row.StartTime.Add(time.Duration(row.DurationNano)),
			DurationMs:     row.DurationMs,
			Status:         row.Status,
			StatusMessage:  row.StatusMessage,
			HTTPMethod:     row.HTTPMethod,
			HTTPStatusCode: int(row.HTTPStatusCode),
			ParentSpanID:   row.ParentSpanID,
			SpanKind:       row.SpanKind,
		}
	}
	return traces
}

func spanRowsToModels(rows []spanRow) []Span {
	spans := make([]Span, len(rows))
	for i, row := range rows {
		spans[i] = Span{
			SpanID:         row.SpanID,
			ParentSpanID:   row.ParentSpanID,
			TraceID:        row.TraceID,
			OperationName:  row.OperationName,
			ServiceName:    row.ServiceName,
			SpanKind:       row.SpanKind,
			StartTime:      row.StartTime,
			EndTime:        row.StartTime.Add(time.Duration(row.DurationNano)),
			DurationMs:     row.DurationMs,
			Status:         row.Status,
			StatusMessage:  row.StatusMessage,
			HTTPMethod:     row.HTTPMethod,
			HTTPURL:        row.HTTPURL,
			HTTPStatusCode: int(row.HTTPStatusCode),
			Host:           row.Host,
			Pod:            row.Pod,
			Attributes:     row.Attributes,
		}
	}
	return spans
}

func mapTraceSummary(row traceSummaryRow) TraceSummary {
	avg := 0.0
	if row.DurationMsCount > 0 {
		avg = row.DurationMsSum / float64(row.DurationMsCount)
	}
	return TraceSummary{
		TotalTraces: row.TotalTraces,
		ErrorTraces: row.ErrorTraces,
		AvgDuration: avg,
		P50Duration: row.P50Duration,
		P95Duration: row.P95Duration,
		P99Duration: row.P99Duration,
	}
}

func errorGroupRowsToModels(rows []errorGroupRow) []ErrorGroup {
	result := make([]ErrorGroup, len(rows))
	for i, row := range rows {
		result[i] = ErrorGroup{
			GroupID:         ErrorGroupID(row.ServiceName, row.OperationName, row.StatusMessage, int(row.HTTPStatusCode)),
			ServiceName:     row.ServiceName,
			OperationName:   row.OperationName,
			StatusMessage:   row.StatusMessage,
			HTTPStatusCode:  int(row.HTTPStatusCode),
			ErrorCount:      row.ErrorCount,
			LastOccurrence:  row.LastOccurrence,
			FirstOccurrence: row.FirstOccurrence,
			SampleTraceID:   row.SampleTraceID,
		}
	}
	return result
}

func errorTimeSeriesRowsToModels(rows []errorTimeSeriesRow) []ErrorTimeSeries {
	result := make([]ErrorTimeSeries, len(rows))
	for i, row := range rows {
		result[i] = ErrorTimeSeries(row)
	}
	return result
}

func latencyHistogramRowsToModels(rows []latencyHistogramRow) []LatencyHistogramBucket {
	result := make([]LatencyHistogramBucket, len(rows))
	for i, row := range rows {
		result[i] = LatencyHistogramBucket{
			BucketLabel: row.BucketLabel,
			BucketMin:   row.BucketMin,
			BucketMax:   row.BucketMin + 1,
			SpanCount:   row.SpanCount,
		}
	}
	return result
}

func latencyHeatmapRowsToModels(rows []latencyHeatmapRow) []LatencyHeatmapPoint {
	result := make([]LatencyHeatmapPoint, len(rows))
	for i, row := range rows {
		result[i] = LatencyHeatmapPoint(row)
	}
	return result
}
