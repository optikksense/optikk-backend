package analytics

import (
	"context"
	"fmt"
	"math"
	"strconv"
)

const (
	maxGroupByDimensions = 3
	maxAggregations      = 5
	maxTimeRangeMs       = 30 * 24 * 60 * 60 * 1000
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// RunQuery validates the analytics query and executes it.
func (s *Service) RunQuery(ctx context.Context, teamID int64, q AnalyticsQuery) (*AnalyticsResult, error) {
	if err := validate(q); err != nil {
		return nil, err
	}
	result, err := s.repo.Execute(ctx, teamID, q)
	if err != nil || result == nil {
		return nil, err
	}
	return &AnalyticsResult{
		Columns: result.Columns,
		Rows:    rebuildAnalyticsRows(result.Rows, result.GroupBy, result.Aggregations),
	}, nil
}

func rebuildAnalyticsRows(rows []analyticsRowDTO, groupBy []string, aggs []Aggregation) []AnalyticsRow {
	aggByAlias := make(map[string]Aggregation, len(aggs))
	for _, agg := range aggs {
		aggByAlias[agg.Alias] = agg
	}

	out := make([]AnalyticsRow, 0, len(rows))
	for _, row := range rows {
		cells := make([]AnalyticsCell, 0, len(row.DimensionKeys)+len(row.MetricKeys))
		for i, key := range row.DimensionKeys {
			if i >= len(row.DimensionValues) {
				continue
			}
			cells = append(cells, analyticsCellFromValue(key, normalizeDimensionValue(key, row.DimensionValues[i])))
		}
		for i, key := range row.MetricKeys {
			if i >= len(row.MetricValues) {
				continue
			}
			cells = append(cells, analyticsCellFromValue(key, normalizeMetricValue(aggByAlias[key], row.MetricValues[i])))
		}
		out = append(out, AnalyticsRow{Cells: cells})
	}
	return out
}

func normalizeDimensionValue(key, raw string) any {
	switch key {
	case "http.status_code", "response_status_code", "rpc.grpc.status_code":
		if raw == "" {
			return int64(0)
		}
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v
		}
	}
	return raw
}

func normalizeMetricValue(agg Aggregation, raw float64) any {
	switch agg.Type {
	case "count", "countIf", "rate":
		return int64(math.Round(raw))
	case "min", "max", "sum":
		if agg.Field == "duration_nano" {
			return int64(math.Round(raw))
		}
	}
	return raw
}

func analyticsCellFromValue(key string, value any) AnalyticsCell {
	switch typed := value.(type) {
	case int64:
		v := typed
		return AnalyticsCell{Key: key, Type: AnalyticsValueInteger, IntegerValue: &v}
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			typed = 0
		}
		v := math.Round(typed*100) / 100
		return AnalyticsCell{Key: key, Type: AnalyticsValueNumber, NumberValue: &v}
	case bool:
		v := typed
		return AnalyticsCell{Key: key, Type: AnalyticsValueBoolean, BooleanValue: &v}
	default:
		v := fmt.Sprint(value)
		return AnalyticsCell{Key: key, Type: AnalyticsValueString, StringValue: &v}
	}
}

func validate(q AnalyticsQuery) error {
	if len(q.GroupBy) == 0 {
		return fmt.Errorf("at least one groupBy dimension is required")
	}
	if len(q.GroupBy) > maxGroupByDimensions {
		return fmt.Errorf("maximum %d groupBy dimensions allowed", maxGroupByDimensions)
	}
	if len(q.Aggregations) == 0 {
		return fmt.Errorf("at least one aggregation is required")
	}
	if len(q.Aggregations) > maxAggregations {
		return fmt.Errorf("maximum %d aggregations allowed", maxAggregations)
	}

	for _, dim := range q.GroupBy {
		if _, ok := LookupDimension(dim); !ok {
			return fmt.Errorf("invalid groupBy dimension: %q", dim)
		}
	}

	if q.Filters.StartMs <= 0 {
		return fmt.Errorf("filters.startMs is required")
	}
	if q.Filters.EndMs <= 0 {
		return fmt.Errorf("filters.endMs is required")
	}
	if q.Filters.EndMs <= q.Filters.StartMs {
		return fmt.Errorf("filters.endMs must be after filters.startMs")
	}
	if (q.Filters.EndMs - q.Filters.StartMs) > maxTimeRangeMs {
		return fmt.Errorf("time range cannot exceed 30 days")
	}

	seen := make(map[string]bool, len(q.Aggregations))
	for _, a := range q.Aggregations {
		if a.Alias == "" {
			return fmt.Errorf("aggregation alias is required")
		}
		if seen[a.Alias] {
			return fmt.Errorf("duplicate aggregation alias: %q", a.Alias)
		}
		seen[a.Alias] = true
	}
	return nil
}
