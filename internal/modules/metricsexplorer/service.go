package metricsexplorer

import (
	"context"
	"fmt"
	"math"
	"strings"
	"unicode"
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
	refs := parseFormulaRefs(f.Expression)
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
		val, err := evalExpr(f.Expression, func(ref string) float64 {
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

// parseFormulaRefs extracts single-letter query references from a formula expression.
func parseFormulaRefs(expr string) []string {
	seen := map[string]bool{}
	var refs []string
	for _, r := range expr {
		if unicode.IsLetter(r) && r >= 'a' && r <= 'z' {
			s := string(r)
			if !seen[s] {
				seen[s] = true
				refs = append(refs, s)
			}
		}
	}
	return refs
}

// evalExpr evaluates a simple arithmetic expression with single-letter variable references.
// Supports +, -, *, / operators and parentheses.
func evalExpr(expr string, resolve func(string) float64) (float64, error) {
	p := &exprParser{input: strings.TrimSpace(expr), pos: 0, resolve: resolve}
	val := p.parseExpr()
	if p.err != nil {
		return 0, p.err
	}
	return val, nil
}

type exprParser struct {
	input   string
	pos     int
	resolve func(string) float64
	err     error
}

func (p *exprParser) parseExpr() float64 {
	result := p.parseTerm()
	for p.pos < len(p.input) {
		p.skipSpaces()
		if p.pos >= len(p.input) {
			break
		}
		op := p.input[p.pos]
		if op != '+' && op != '-' {
			break
		}
		p.pos++
		right := p.parseTerm()
		if op == '+' {
			result += right
		} else {
			result -= right
		}
	}
	return result
}

func (p *exprParser) parseTerm() float64 {
	result := p.parseFactor()
	for p.pos < len(p.input) {
		p.skipSpaces()
		if p.pos >= len(p.input) {
			break
		}
		op := p.input[p.pos]
		if op != '*' && op != '/' {
			break
		}
		p.pos++
		right := p.parseFactor()
		if op == '*' {
			result *= right
		} else {
			if right == 0 {
				return 0
			}
			result /= right
		}
	}
	return result
}

func (p *exprParser) parseFactor() float64 {
	p.skipSpaces()
	if p.pos >= len(p.input) {
		return 0
	}

	// Parenthesized sub-expression.
	if p.input[p.pos] == '(' {
		p.pos++
		val := p.parseExpr()
		p.skipSpaces()
		if p.pos < len(p.input) && p.input[p.pos] == ')' {
			p.pos++
		}
		return val
	}

	// Number literal.
	if p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.input[p.pos] == '.' {
		return p.parseNumber()
	}

	// Single-letter variable reference.
	if p.input[p.pos] >= 'a' && p.input[p.pos] <= 'z' {
		ref := string(p.input[p.pos])
		p.pos++
		return p.resolve(ref)
	}

	p.err = fmt.Errorf("unexpected character at position %d: %q", p.pos, string(p.input[p.pos]))
	return 0
}

func (p *exprParser) parseNumber() float64 {
	start := p.pos
	for p.pos < len(p.input) && (p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.input[p.pos] == '.') {
		p.pos++
	}
	var val float64
	_, _ = fmt.Sscanf(p.input[start:p.pos], "%f", &val)
	return val
}

func (p *exprParser) skipSpaces() {
	for p.pos < len(p.input) && p.input[p.pos] == ' ' {
		p.pos++
	}
}
