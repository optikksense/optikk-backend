package metricsexplorer

import (
	"context"
	"fmt"
	"math"

	"github.com/Optikk-Org/optikk-backend/internal/shared/exprparser"
)

type Service interface {
	ListMetricNames(ctx context.Context, teamID int64, startMs, endMs int64, search string) ([]MetricNameResult, error)
	ListTagKeys(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TagKeyResult, error)
	ListTagValues(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error)
	Query(ctx context.Context, teamID int64, startMs, endMs int64, req ExplorerQueryRequest) (*ExplorerQueryResult, error)
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

func (s *MetricsExplorerService) Query(ctx context.Context, teamID int64, startMs, endMs int64, req ExplorerQueryRequest) (*ExplorerQueryResult, error) {
	// Execute each metric query and collect series by query name.
	queryResults := make(map[string][]SeriesResult, len(req.Queries))

	for _, q := range req.Queries {
		points, err := s.repo.QueryTimeseries(ctx, teamID, startMs, endMs, q)
		if err != nil {
			return nil, fmt.Errorf("query %q: %w", q.Name, err)
		}

		series := assembleSeriesFromPoints(q, points)
		queryResults[q.Name] = series
	}

	// Collect all series from queries.
	var allSeries []SeriesResult
	for _, series := range queryResults {
		allSeries = append(allSeries, series...)
	}

	// Evaluate formulas.
	for _, f := range req.Formulas {
		formulaSeries, err := evaluateFormula(f, queryResults)
		if err != nil {
			return nil, fmt.Errorf("formula %q: %w", f.Name, err)
		}
		allSeries = append(allSeries, formulaSeries...)
	}

	return &ExplorerQueryResult{Series: allSeries}, nil
}

// assembleSeriesFromPoints groups flat TimeseriesPoint rows into SeriesResult slices.
// When there are no group-by dimensions, all points become a single series.
// With group-by, the repository returns extra `group_<key>` columns, but since
// our current TimeseriesPoint struct only has timestamp+value, grouped queries
// produce one row per (time_bucket, group combination). We rely on the repository
// ORDER BY to keep groups contiguous — but since the repo only orders by time_bucket,
// we need to re-query per group or handle it differently.
//
// For the initial implementation without group-by column extraction in the DTO,
// we return all points as a single flat series named after the query.
// Group-by support will be enhanced once we add dynamic column scanning.
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
// Only supports single-series queries (no group-by) for now.
func evaluateFormula(f Formula, queryResults map[string][]SeriesResult) ([]SeriesResult, error) {
	// Parse referenced query names from the expression.
	refs := exprparser.ExtractVariables(f.Expression)
	if len(refs) == 0 {
		return nil, fmt.Errorf("no query references found in expression %q", f.Expression)
	}

	// Build timestamp-indexed value maps for each referenced query.
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

		// Use the first referenced query's timestamps as the base.
		if timestamps == nil {
			timestamps = make([]string, len(series[0].Datapoints))
			for i, dp := range series[0].Datapoints {
				timestamps[i] = dp.Timestamp
			}
		}
	}

	// Evaluate the expression for each timestamp.
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
