package metricsexplorer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/exprparser"
)

type Service interface {
	ListMetricNames(ctx context.Context, teamID int64, startMs, endMs int64, search string) ([]MetricNameResult, error)
	ListTagKeys(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TagKeyResult, error)
	ListTagValues(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error)
	ListTags(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]FETagEntry, error)
	Query(ctx context.Context, teamID int64, startMs, endMs int64, req ExplorerQueryRequest) (*ExplorerQueryResult, error)
	QueryForFrontend(ctx context.Context, teamID int64, req FEQueryRequest) (*FEQueryResponse, error)
}

type MetricsExplorerService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &MetricsExplorerService{repo: repo}
}

func (s *MetricsExplorerService) ListMetricNames(ctx context.Context, teamID int64, startMs, endMs int64, search string) ([]MetricNameResult, error) {
	return s.repo.ListMetricNames(ctx, teamID, startMs, endMs, search)
}

func (s *MetricsExplorerService) ListTagKeys(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TagKeyResult, error) {
	return s.repo.ListTagKeys(ctx, teamID, startMs, endMs, metricName)
}

func (s *MetricsExplorerService) ListTagValues(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error) {
	return s.repo.ListTagValues(ctx, teamID, startMs, endMs, metricName, tagKey)
}

// ListTags merges tag keys and their values into the frontend-expected format.
// If tagKey is non-empty, only that key's values are returned.
func (s *MetricsExplorerService) ListTags(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]FETagEntry, error) {
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

	tags := make([]FETagEntry, 0, len(keys))
	for _, k := range keys {
		values, err := s.repo.ListTagValues(ctx, teamID, startMs, endMs, metricName, k.TagKey)
		if err != nil {
			return nil, err
		}
		vals := make([]string, len(values))
		for i, v := range values {
			vals[i] = v.TagValue
		}
		tags = append(tags, FETagEntry{Key: k.TagKey, Values: vals})
	}
	return tags, nil
}

func (s *MetricsExplorerService) Query(ctx context.Context, teamID int64, startMs, endMs int64, req ExplorerQueryRequest) (*ExplorerQueryResult, error) {
	queryResults := make(map[string][]SeriesResult, len(req.Queries))

	for _, q := range req.Queries {
		points, err := s.repo.QueryTimeseries(ctx, teamID, startMs, endMs, q, "")
		if err != nil {
			return nil, fmt.Errorf("query %q: %w", q.Name, err)
		}
		series := assembleSeriesFromPoints(q, points)
		queryResults[q.Name] = series
	}

	var allSeries []SeriesResult
	for _, series := range queryResults {
		allSeries = append(allSeries, series...)
	}

	for _, f := range req.Formulas {
		formulaSeries, err := evaluateFormula(f, queryResults)
		if err != nil {
			return nil, fmt.Errorf("formula %q: %w", f.Name, err)
		}
		allSeries = append(allSeries, formulaSeries...)
	}

	return &ExplorerQueryResult{Series: allSeries}, nil
}

// QueryForFrontend executes queries and returns results in the frontend-expected format.
func (s *MetricsExplorerService) QueryForFrontend(ctx context.Context, teamID int64, req FEQueryRequest) (*FEQueryResponse, error) {
	results := make(map[string]FEQueryResult, len(req.Queries))

	for _, feq := range req.Queries {
		internalQuery := convertFEQuery(feq)

		points, err := s.repo.QueryTimeseries(ctx, teamID, req.StartTime, req.EndTime, internalQuery, req.Step)
		if err != nil {
			return nil, fmt.Errorf("query %q: %w", feq.ID, err)
		}

		results[feq.ID] = buildColumnarResult(points)
	}

	return &FEQueryResponse{Results: results}, nil
}

// convertFEQuery converts a frontend query to an internal MetricQuery.
func convertFEQuery(feq FEMetricQuery) MetricQuery {
	filters := make([]TagFilter, 0, len(feq.Where))
	for _, w := range feq.Where {
		filters = append(filters, TagFilter{
			Key:      w.Key,
			Operator: mapOperator(w.Operator),
			Values:   extractValues(w.Value),
		})
	}
	return MetricQuery{
		Name:        feq.ID,
		MetricName:  feq.MetricName,
		Aggregation: feq.Aggregation,
		GroupBy:     feq.GroupBy,
		Filters:     filters,
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

// extractValues normalises the frontend filter value (string or []string) to []string.
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
		if s := fmt.Sprintf("%v", v); s != "" {
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

	// Collect unique timestamps and sort them.
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

	// Build index: timestamp string -> position in the timestamps array.
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

	// Build values array aligned with timestamps (nil for missing).
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

// parseTimestampMs parses a ClickHouse formatted timestamp string to epoch milliseconds.
func parseTimestampMs(ts string) int64 {
	t, err := time.Parse("2006-01-02 15:04:00", ts)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

// assembleSeriesFromPoints groups flat TimeseriesPoint rows into SeriesResult slices.
func assembleSeriesFromPoints(q MetricQuery, points []TimeseriesPoint) []SeriesResult {
	if len(points) == 0 {
		return nil
	}

	datapoints := make([]Datapoint, len(points))
	for i, p := range points {
		datapoints[i] = Datapoint(p)
	}

	return []SeriesResult{{
		Name:       q.Name,
		GroupTags:  map[string]string{},
		Datapoints: datapoints,
	}}
}

// evaluateFormula computes a formula across query results.
func evaluateFormula(f Formula, queryResults map[string][]SeriesResult) ([]SeriesResult, error) {
	refs := exprparser.ExtractVariables(f.Expression)
	if len(refs) == 0 {
		return nil, fmt.Errorf("no query references found in expression %q", f.Expression)
	}

	valueMaps := make(map[string]map[string]float64, len(refs))
	var timestamps []string

	for _, ref := range refs {
		series, ok := queryResults[ref]
		if !ok || len(series) == 0 {
			return nil, fmt.Errorf("referenced query %q not found or empty", ref)
		}
		vm := make(map[string]float64, len(series[0].Datapoints))
		for _, dp := range series[0].Datapoints {
			vm[dp.Timestamp] = dp.Value
		}
		valueMaps[ref] = vm

		if timestamps == nil {
			timestamps = make([]string, len(series[0].Datapoints))
			for i, dp := range series[0].Datapoints {
				timestamps[i] = dp.Timestamp
			}
		}
	}

	datapoints := make([]Datapoint, 0, len(timestamps))
	for _, ts := range timestamps {
		val, err := exprparser.Evaluate(f.Expression, func(ref string) float64 {
			if vm, ok := valueMaps[ref]; ok {
				return vm[ts]
			}
			return 0
		})
		if err != nil {
			continue
		}
		if math.IsNaN(val) || math.IsInf(val, 0) {
			val = 0
		}
		datapoints = append(datapoints, Datapoint{Timestamp: ts, Value: val})
	}

	return []SeriesResult{{
		Name:       f.Name,
		GroupTags:  map[string]string{},
		Datapoints: datapoints,
	}}, nil
}
