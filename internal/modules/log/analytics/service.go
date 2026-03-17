package analytics

import (
	"context"
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

var allowedFieldColumns = map[string]string{
	"severity_text": "severity_text",
	"service":       "service",
	"host":          "host",
	"pod":           "pod",
	"container":     "container",
	"scope_name":    "scope_name",
	"environment":   "environment",
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetLogHistogram(ctx context.Context, f shared.LogFilters, step string) (LogHistogramData, error) {
	rows, err := s.repo.GetLogHistogram(ctx, f, step)
	if err != nil {
		return LogHistogramData{}, err
	}
	buckets := make([]LogHistogramBucket, len(rows))
	for i, row := range rows {
		buckets[i] = LogHistogramBucket{TimeBucket: row.TimeBucket, Severity: row.Severity, Count: row.Count}
	}
	return LogHistogramData{Buckets: buckets, Step: step}, nil
}

func (s *Service) GetLogVolume(ctx context.Context, f shared.LogFilters, step string) (LogVolumeData, error) {
	rows, err := s.repo.GetLogVolume(ctx, f, step)
	if err != nil {
		return LogVolumeData{}, err
	}
	buckets := make([]LogVolumeBucket, len(rows))
	for i, row := range rows {
		buckets[i] = LogVolumeBucket{
			TimeBucket: row.TimeBucket,
			Total:      row.Total,
			Errors:     row.Errors,
			Warnings:   row.Warnings,
			Infos:      row.Infos,
			Debugs:     row.Debugs,
			Fatals:     row.Fatals,
		}
	}
	return LogVolumeData{Buckets: buckets, Step: step}, nil
}

func (s *Service) GetLogStats(ctx context.Context, f shared.LogFilters) (LogStats, error) {
	rows, err := s.repo.GetLogStats(ctx, f)
	if err != nil {
		return LogStats{}, err
	}
	fields := map[string][]Facet{
		"level":        {},
		"service_name": {},
		"host":         {},
		"pod":          {},
		"scope_name":   {},
	}
	limits := map[string]int{
		"level":        100,
		"service_name": 50,
		"host":         50,
		"pod":          50,
		"scope_name":   50,
	}

	for _, row := range rows {
		facets := fields[row.Dim]
		limit := limits[row.Dim]
		if limit == 0 || len(facets) < limit {
			fields[row.Dim] = append(facets, Facet{Value: row.Value, Count: row.Count})
		}
	}

	var total int64
	for _, facet := range fields["level"] {
		total += facet.Count
	}

	return LogStats{Total: total, Fields: fields}, nil
}

func (s *Service) GetLogFields(ctx context.Context, f shared.LogFilters, field string) (FieldValuesResponse, error) {
	col, ok := allowedFieldColumns[field]
	if !ok {
		return FieldValuesResponse{}, fmt.Errorf("field is required and must be one of: severity_text, service, host, pod, container, scope_name, environment")
	}
	rows, err := s.repo.GetLogFields(ctx, f, col)
	if err != nil {
		return FieldValuesResponse{}, err
	}
	values := make([]Facet, len(rows))
	for i, row := range rows {
		values[i] = Facet{Value: row.Value, Count: row.Count}
	}
	return FieldValuesResponse{Field: field, Values: values}, nil
}

func (s *Service) GetLogAggregate(ctx context.Context, f shared.LogFilters, req LogAggregateRequest) (LogAggregateResponse, error) {
	query, err := buildAggregateQuery(req)
	if err != nil {
		return LogAggregateResponse{}, err
	}

	topRows, err := s.repo.GetTopGroups(ctx, f, query)
	if err != nil {
		return LogAggregateResponse{}, err
	}
	groups := make([]string, 0, len(topRows))
	for _, row := range topRows {
		if row.GroupValue != "" {
			groups = append(groups, row.GroupValue)
		}
	}

	rows, err := s.repo.GetAggregateSeries(ctx, f, query, groups)
	if err != nil {
		return LogAggregateResponse{}, err
	}
	respRows := make([]LogAggregateRow, len(rows))
	for i, row := range rows {
		respRows[i] = LogAggregateRow{
			TimeBucket: dbutil.TimeFromAny(row.TimeBucket).UTC().Format(time.RFC3339),
			GroupValue: row.GroupValue,
			Count:      row.Count,
			ErrorRate:  row.ErrorRate,
		}
	}

	return LogAggregateResponse{
		GroupBy: query.GroupBy,
		Step:    query.Step,
		Metric:  query.Metric,
		Rows:    respRows,
	}, nil
}

func buildAggregateQuery(req LogAggregateRequest) (LogAggregateQuery, error) {
	groupBy := req.GroupBy
	if groupBy == "" {
		groupBy = "service"
	}
	groupCol, ok := allowedFieldColumns[groupBy]
	if !ok {
		return LogAggregateQuery{}, fmt.Errorf("invalid groupBy field: %s", req.GroupBy)
	}
	step := req.Step
	if step == "" {
		step = "5m"
	}
	topN := req.TopN
	if topN <= 0 || topN > 100 {
		topN = 20
	}
	metric := req.Metric
	if metric == "" {
		metric = "count"
	}
	if metric != "count" && metric != "error_rate" {
		return LogAggregateQuery{}, fmt.Errorf("invalid metric: %s", req.Metric)
	}

	return LogAggregateQuery{
		GroupBy:  groupBy,
		GroupCol: groupCol,
		Step:     step,
		TopN:     topN,
		Metric:   metric,
	}, nil
}
