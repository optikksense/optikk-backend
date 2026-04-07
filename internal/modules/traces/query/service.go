package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

type Service struct {
	repo Repository
}

type TraceSearchResult struct {
	Traces     []Trace
	HasMore    bool
	NextCursor string
	Offset     int
	Limit      int
	Total      int64
	Summary    TraceSummary
	UsesKeyset bool
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) SearchTraces(ctx context.Context, filters TraceFilters, limit int, cursorRaw string, offset int) (TraceSearchResult, error) {
	if cursorRaw != "" || offset == 0 {
		cursor := decodeCursor(cursorRaw)
		rows, summaryRow, hasMore, err := s.repo.GetTracesKeyset(ctx, filters, limit, cursor)
		if err != nil {
			return TraceSearchResult{}, fmt.Errorf("traces.SearchTraces.Keyset: %w", err)
		}
		traces := traceRowsToModels(rows)
		summary := mapTraceSummary(summaryRow)
		result := TraceSearchResult{
			Traces:     traces,
			HasMore:    hasMore,
			Limit:      limit,
			Total:      summary.TotalTraces,
			Summary:    summary,
			UsesKeyset: true,
		}
		if hasMore && len(traces) > 0 {
			last := traces[len(traces)-1]
			result.NextCursor = encodeCursor(TraceCursor{Timestamp: last.StartTime, SpanID: last.SpanID})
		}
		return result, nil
	}

	rows, total, summaryRow, err := s.repo.GetTraces(ctx, filters, limit, offset)
	if err != nil {
		return TraceSearchResult{}, fmt.Errorf("traces.SearchTraces.Offset: %w", err)
	}
	traces := traceRowsToModels(rows)
	return TraceSearchResult{
		Traces:  traces,
		HasMore: len(traces) >= limit,
		Offset:  offset,
		Limit:   limit,
		Total:   total,
		Summary: mapTraceSummary(summaryRow),
	}, nil
}

func (s *Service) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]Span, error) {
	rows, err := s.repo.GetTraceSpans(ctx, teamID, traceID)
	if err != nil {
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
	return buckets, nil
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

func encodeCursor(cur TraceCursor) string {
	b, _ := json.Marshal(cur)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(raw string) TraceCursor {
	if raw == "" {
		return TraceCursor{}
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return TraceCursor{}
	}
	var cur TraceCursor
	_ = json.Unmarshal(b, &cur)
	return cur
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
	return TraceSummary(row)
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
