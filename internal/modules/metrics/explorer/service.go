package explorer

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/metrics/filter"
)

type Service interface {
	ListMetricNames(ctx context.Context, teamID, startMs, endMs int64, search string) ([]MetricNameResult, error)
	ListTagKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string) ([]TagKeyResult, error)
	ListTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error)
	ListTags(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]FETagEntry, error)
	QueryForFrontend(ctx context.Context, teamID int64, req FEQueryRequest) (*FEQueryResponse, error)
}

type MetricsExplorerService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &MetricsExplorerService{repo: repo}
}

func (s *MetricsExplorerService) ListMetricNames(ctx context.Context, teamID, startMs, endMs int64, search string) ([]MetricNameResult, error) {
	return s.repo.ListMetricNames(ctx, teamID, startMs, endMs, search)
}

func (s *MetricsExplorerService) ListTagKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string) ([]TagKeyResult, error) {
	return s.repo.ListTagKeys(ctx, teamID, startMs, endMs, metricName)
}

func (s *MetricsExplorerService) ListTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error) {
	return s.repo.ListTagValues(ctx, teamID, startMs, endMs, metricName, tagKey)
}

// ListTags merges tag keys and their values into the frontend-expected
// format. If tagKey is non-empty, only that key's values are returned.
func (s *MetricsExplorerService) ListTags(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]FETagEntry, error) {
	if tagKey != "" {
		values, err := s.repo.ListTagValues(ctx, teamID, startMs, endMs, metricName, tagKey)
		if err != nil {
			return nil, err
		}
		vals := make([]string, len(values))
		for i, v := range values {
			vals[i] = v.TagValue
		}
		return []FETagEntry{{Key: tagKey, Values: vals}}, nil
	}

	keys, err := s.repo.ListTagKeys(ctx, teamID, startMs, endMs, metricName)
	if err != nil {
		return nil, err
	}

	keyNames := make([]string, len(keys))
	for i, k := range keys {
		keyNames[i] = k.TagKey
	}

	// One query for every key's values, instead of one query per key.
	rows, err := s.repo.ListTagValuesForKeys(ctx, teamID, startMs, endMs, metricName, keyNames)
	if err != nil {
		return nil, err
	}

	// Fold (key, value) rows into per-key value lists. Rows arrive ordered by
	// (tag_key, count DESC), so the most common values come first per key.
	valuesByKey := make(map[string][]string, len(keys))
	for _, row := range rows {
		valuesByKey[row.TagKey] = append(valuesByKey[row.TagKey], row.TagValue)
	}

	tags := make([]FETagEntry, len(keys))
	for i, k := range keys {
		vals := valuesByKey[k.TagKey]
		if vals == nil {
			vals = []string{} // preserve [] (not null) for keys with no values
		}
		tags[i] = FETagEntry{Key: k.TagKey, Values: vals}
	}
	return tags, nil
}

// QueryForFrontend executes each query in the request and returns columnar
// results. Each query is converted to a typed filter.Filters and validated
// before reaching the repo.
func (s *MetricsExplorerService) QueryForFrontend(ctx context.Context, teamID int64, req FEQueryRequest) (*FEQueryResponse, error) {
	results := make(map[string]FEQueryResult, len(req.Queries))

	for _, feq := range req.Queries {
		f := convertFEQuery(teamID, req.StartTime, req.EndTime, req.Step, feq)
		if err := f.Validate(); err != nil {
			return nil, fmt.Errorf("query %q: %w", feq.ID, err)
		}

		points, err := s.repo.QueryTimeseries(ctx, f)
		if err != nil {
			return nil, fmt.Errorf("query %q: %w", feq.ID, err)
		}

		results[feq.ID] = buildColumnarResult(points)
	}

	return &FEQueryResponse{Results: results}, nil
}

// convertFEQuery folds the request-level time range/step plus the per-query
// frontend filter shape into the typed filter.Filters the repo consumes.
func convertFEQuery(teamID, startMs, endMs int64, step string, feq FEMetricQuery) filter.Filters {
	tags := make([]filter.TagFilter, 0, len(feq.Where))
	for _, w := range feq.Where {
		tags = append(tags, filter.TagFilter{
			Key:      w.Key,
			Operator: mapOperator(w.Operator),
			Values:   extractValues(w.Value),
		})
	}
	return filter.Filters{
		TeamID:      teamID,
		StartMs:     startMs,
		EndMs:       endMs,
		MetricName:  feq.MetricName,
		Aggregation: feq.Aggregation,
		Step:        step,
		GroupBy:     feq.GroupBy,
		Tags:        tags,
	}
}

// mapOperator converts frontend operator names to SQL operators.
func mapOperator(op string) string {
	switch op {
	case "eq":
		return "="
	case "neq":
		return "!="
	case "in":
		return "IN"
	case "not_in":
		return "NOT IN"
	default:
		return op
	}
}

// extractValues normalises the frontend filter value (string or []string)
// to []string.
func extractValues(v any) []string {
	switch val := v.(type) {
	case string:
		return []string{val}
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return val
	default:
		if s := fmt.Sprint(v); s != "" {
			return []string{s}
		}
		return nil
	}
}

// buildColumnarResult converts flat TimeseriesPoint rows into the columnar
// format the frontend expects: shared timestamps array + values per series.
func buildColumnarResult(points []TimeseriesPoint) FEQueryResult {
	if len(points) == 0 {
		return FEQueryResult{Timestamps: []int64{}, Series: []FESeries{}}
	}

	tsSet := make(map[string]int64, len(points))
	for _, p := range points {
		if _, exists := tsSet[p.Timestamp]; !exists {
			tsSet[p.Timestamp] = parseTimestampMs(p.Timestamp)
		}
	}

	timestamps := make([]int64, 0, len(tsSet))
	for _, ms := range tsSet {
		timestamps = append(timestamps, ms)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	tsIndex := make(map[string]int, len(timestamps))
	for _, p := range points {
		ms := tsSet[p.Timestamp]
		for idx, t := range timestamps {
			if t == ms {
				tsIndex[p.Timestamp] = idx
				break
			}
		}
	}

	values := make([]*float64, len(timestamps))
	for _, p := range points {
		idx := tsIndex[p.Timestamp]
		v := p.Value
		values[idx] = &v
	}

	return FEQueryResult{
		Timestamps: timestamps,
		Series: []FESeries{{
			Tags:   map[string]string{},
			Values: values,
		}},
	}
}

// parseTimestampMs parses a ClickHouse formatted timestamp string to epoch
// milliseconds.
func parseTimestampMs(ts string) int64 {
	t, err := time.Parse("2006-01-02 15:04:00", ts)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}
