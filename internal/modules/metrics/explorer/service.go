package explorer

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// materializedColumns maps user-facing tag keys to materialized ClickHouse columns.
var materializedColumns = map[string]string{
	"service.name":           "service",
	"host.name":              "host",
	"deployment.environment": "environment",
	"k8s.namespace.name":     "k8s_namespace",
	"http.method":            "http_method",
}

// safeTagKeyPattern validates that tag keys contain only safe characters.
var safeTagKeyPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.]*$`)

// allowedSteps is the set of valid time step values.
var allowedSteps = map[string]bool{
	"1m": true, "5m": true, "15m": true, "1h": true, "1d": true,
}

// allowedAggregations is the set of valid aggregation functions.
var allowedAggregations = map[string]bool{
	"avg": true, "sum": true, "min": true, "max": true, "count": true,
	"p50": true, "p95": true, "p99": true, "rate": true,
}

// maxTimeseriesRows caps the result set for timeseries queries.
const maxTimeseriesRows = 10000

// queryTimeout is appended to ClickHouse queries as a SETTINGS clause.
const queryTimeout = "SETTINGS max_execution_time = 30"

// Service implements the metrics explorer business logic.
type Service struct {
	db *dbutil.NativeQuerier
}

// NewService creates a new metrics explorer service.
func NewService(db *dbutil.NativeQuerier) *Service {
	return &Service{db: db}
}

// FetchMetricNames returns a list of distinct metric names matching the search filter.
func (s *Service) FetchMetricNames(ctx context.Context, teamID, startMs, endMs int64, search string) (*MetricNamesResponse, error) {
	var whereParts []string
	var args []any

	whereParts = append(whereParts, "team_id = @teamID")
	args = append(args, clickhouse.Named("teamID", uint32(teamID)))

	whereParts = append(whereParts, "timestamp BETWEEN @start AND @end")
	args = append(args,
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)

	if search != "" {
		whereParts = append(whereParts, "metric_name ILIKE @search")
		args = append(args, clickhouse.Named("search", "%"+search+"%"))
	}

	query := fmt.Sprintf(`
		SELECT
			metric_name AS name,
			any(metric_type) AS type,
			any(unit) AS unit,
			any(description) AS description
		FROM observability.metrics
		WHERE %s
		GROUP BY metric_name
		ORDER BY metric_name ASC
		LIMIT 500
	`, strings.Join(whereParts, " AND "))

	var rows []MetricNameEntry
	if err := s.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("metric names query failed: %w", err)
	}

	if rows == nil {
		rows = []MetricNameEntry{}
	}
	return &MetricNamesResponse{Metrics: rows}, nil
}

// FetchMetricTags returns tag keys and their values for a given metric.
func (s *Service) FetchMetricTags(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) (*MetricTagsResponse, error) {
	baseArgs := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	baseWhere := "team_id = @teamID AND metric_name = @metricName AND timestamp BETWEEN @start AND @end"

	if tagKey != "" {
		return s.fetchSingleTagValues(ctx, baseWhere, baseArgs, tagKey)
	}
	return s.fetchAllTags(ctx, baseWhere, baseArgs)
}

func (s *Service) fetchAllTags(ctx context.Context, baseWhere string, baseArgs []any) (*MetricTagsResponse, error) {
	// Batch query: fetch tag keys and top-10 values in a single query.
	query := fmt.Sprintf(`
		SELECT
			tag_key,
			groupUniqArray(10)(tag_value) AS tag_values
		FROM (
			SELECT
				arrayJoin(JSONAllPaths(attributes)) AS tag_key,
				JSONExtractString(attributes, tag_key) AS tag_value
			FROM observability.metrics
			WHERE %s
		)
		WHERE tag_value != ''
		GROUP BY tag_key
		ORDER BY tag_key ASC
		LIMIT 100
		%s
	`, baseWhere, queryTimeout)

	var rows []tagKeyValuesDTO
	if err := s.db.Select(ctx, &rows, query, baseArgs...); err != nil {
		return nil, fmt.Errorf("tag keys query failed: %w", err)
	}

	// Prepend materialized columns that are always available.
	tags := make([]MetricTag, 0, len(materializedColumns)+len(rows))
	for userKey := range materializedColumns {
		tags = append(tags, MetricTag{Key: userKey, Values: nil})
	}
	for _, row := range rows {
		tags = append(tags, MetricTag{Key: row.TagKey, Values: row.TagValues})
	}

	return &MetricTagsResponse{Tags: tags}, nil
}

func (s *Service) fetchSingleTagValues(ctx context.Context, baseWhere string, baseArgs []any, tagKey string) (*MetricTagsResponse, error) {
	colExpr, err := resolveTagColumn(tagKey)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT %s AS tag_value
		FROM observability.metrics
		WHERE %s AND %s != ''
		ORDER BY tag_value ASC
		LIMIT 200
		%s
	`, colExpr, baseWhere, colExpr, queryTimeout)

	var valRows []tagValueDTO
	if err := s.db.Select(ctx, &valRows, query, baseArgs...); err != nil {
		return nil, fmt.Errorf("tag values query failed: %w", err)
	}

	values := make([]string, 0, len(valRows))
	for _, vr := range valRows {
		values = append(values, vr.TagValue)
	}

	return &MetricTagsResponse{
		Tags: []MetricTag{{Key: tagKey, Values: values}},
	}, nil
}

// ExecuteQuery runs the metric explorer queries and returns time-series results.
func (s *Service) ExecuteQuery(ctx context.Context, teamID int64, req MetricExplorerRequest) (*MetricExplorerResponse, error) {
	results := make(map[string]*MetricQueryResult, len(req.Queries))

	for _, q := range req.Queries {
		if q.MetricName == "" {
			continue
		}
		result, err := s.executeSingleQuery(ctx, teamID, req.StartTime, req.EndTime, req.Step, q)
		if err != nil {
			return nil, fmt.Errorf("query %q failed: %w", q.ID, err)
		}
		results[q.ID] = result
	}

	return &MetricExplorerResponse{Results: results}, nil
}

func (s *Service) executeSingleQuery(ctx context.Context, teamID, startMs, endMs int64, step string, q MetricQueryRequest) (*MetricQueryResult, error) {
	strat := timebucket.ByName(step)
	bucketExpr := strat.GetBucketExpression()

	aggExpr := buildAggExpression(q.Aggregation)

	// Build group-by columns.
	groupByCols := make([]string, 0, len(q.GroupBy))
	for _, g := range q.GroupBy {
		col, err := resolveTagColumn(g)
		if err != nil {
			return nil, fmt.Errorf("invalid groupBy key: %w", err)
		}
		groupByCols = append(groupByCols, col)
	}

	// Build WHERE.
	var whereParts []string
	var args []any

	whereParts = append(whereParts, "team_id = @teamID")
	args = append(args, clickhouse.Named("teamID", uint32(teamID)))

	whereParts = append(whereParts, "metric_name = @metricName")
	args = append(args, clickhouse.Named("metricName", q.MetricName))

	whereParts = append(whereParts, "timestamp BETWEEN @start AND @end")
	args = append(args,
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)

	for i, w := range q.Where {
		frag, wArgs, wErr := buildWhereClause(w, i)
		if wErr != nil {
			return nil, fmt.Errorf("invalid where clause: %w", wErr)
		}
		if frag != "" {
			whereParts = append(whereParts, frag)
			args = append(args, wArgs...)
		}
	}

	// Build SELECT columns.
	selectParts := []string{
		fmt.Sprintf("toInt64(%s) AS ts", bucketExpr),
	}

	// Tag keys/values arrays for group-by.
	if len(groupByCols) > 0 {
		tagKeyLiterals := make([]string, len(q.GroupBy))
		tagValExprs := make([]string, len(groupByCols))
		for i, g := range q.GroupBy {
			tagKeyLiterals[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(g, "'", "\\'"))
			tagValExprs[i] = fmt.Sprintf("toString(%s)", groupByCols[i])
		}
		selectParts = append(selectParts,
			fmt.Sprintf("[%s] AS tag_keys", strings.Join(tagKeyLiterals, ", ")),
			fmt.Sprintf("[%s] AS tag_vals", strings.Join(tagValExprs, ", ")),
		)
	} else {
		selectParts = append(selectParts,
			"[] AS tag_keys",
			"[] AS tag_vals",
		)
	}

	selectParts = append(selectParts, fmt.Sprintf("toFloat64(%s) AS agg_value", aggExpr))

	// Build GROUP BY.
	groupByParts := []string{bucketExpr}
	groupByParts = append(groupByParts, groupByCols...)

	query := fmt.Sprintf(`SELECT %s FROM observability.metrics WHERE %s GROUP BY %s ORDER BY ts ASC LIMIT %d %s`,
		strings.Join(selectParts, ", "),
		strings.Join(whereParts, " AND "),
		strings.Join(groupByParts, ", "),
		maxTimeseriesRows,
		queryTimeout,
	)

	var rows []timeseriesRowDTO
	if err := s.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("timeseries query failed: %w", err)
	}

	return buildTimeseriesResult(rows, q.GroupBy), nil
}

func buildAggExpression(agg string) string {
	switch agg {
	case "avg":
		return "avg(value)"
	case "sum":
		return "sum(value)"
	case "min":
		return "min(value)"
	case "max":
		return "max(value)"
	case "count":
		return "count()"
	case "p50":
		return "quantile(0.5)(value)"
	case "p95":
		return "quantile(0.95)(value)"
	case "p99":
		return "quantile(0.99)(value)"
	case "rate":
		return "(max(value) - min(value)) / greatest(toUnixTimestamp(max(timestamp)) - toUnixTimestamp(min(timestamp)), 1)"
	default:
		return "avg(value)"
	}
}

func resolveTagColumn(tagKey string) (string, error) {
	if mat, ok := materializedColumns[tagKey]; ok {
		return mat, nil
	}
	if !safeTagKeyPattern.MatchString(tagKey) {
		return "", fmt.Errorf("invalid tag key: %q", tagKey)
	}
	return fmt.Sprintf("JSONExtractString(attributes, '%s')", tagKey), nil
}

func buildWhereClause(w MetricWhereClause, idx int) (string, []any, error) {
	col, err := resolveTagColumn(w.Key)
	if err != nil {
		return "", nil, err
	}
	paramName := fmt.Sprintf("w%d", idx)

	switch w.Operator {
	case "eq":
		val := fmt.Sprint(w.Value)
		return fmt.Sprintf("%s = @%s", col, paramName), []any{clickhouse.Named(paramName, val)}, nil
	case "neq":
		val := fmt.Sprint(w.Value)
		return fmt.Sprintf("%s != @%s", col, paramName), []any{clickhouse.Named(paramName, val)}, nil
	case "in":
		vals := toStringSlice(w.Value)
		return fmt.Sprintf("%s IN @%s", col, paramName), []any{clickhouse.Named(paramName, vals)}, nil
	case "not_in":
		vals := toStringSlice(w.Value)
		return fmt.Sprintf("%s NOT IN @%s", col, paramName), []any{clickhouse.Named(paramName, vals)}, nil
	case "wildcard":
		val := fmt.Sprint(w.Value)
		return fmt.Sprintf("%s LIKE @%s", col, paramName), []any{clickhouse.Named(paramName, val)}, nil
	default:
		return "", nil, fmt.Errorf("unsupported operator: %q", w.Operator)
	}
}

func toStringSlice(v any) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			out = append(out, fmt.Sprint(item))
		}
		return out
	default:
		return []string{fmt.Sprint(v)}
	}
}

func buildTimeseriesResult(rows []timeseriesRowDTO, groupByKeys []string) *MetricQueryResult {
	tsSet := make(map[int64]bool)
	type sKey = string

	seriesMap := make(map[sKey]*MetricSeriesData)
	seriesOrder := make([]sKey, 0)

	for _, row := range rows {
		tsSet[row.Ts] = true

		key := stableSeriesKey(groupByKeys, row.TagVals)
		if _, exists := seriesMap[key]; !exists {
			tags := make(map[string]string, len(groupByKeys))
			for i, k := range groupByKeys {
				if i < len(row.TagVals) {
					tags[k] = row.TagVals[i]
				}
			}
			seriesMap[key] = &MetricSeriesData{Tags: tags}
			seriesOrder = append(seriesOrder, key)
		}
	}

	// Sort timestamps.
	timestamps := make([]int64, 0, len(tsSet))
	for ts := range tsSet {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	// Build timestamp index for O(1) lookup.
	tsIndex := make(map[int64]int, len(timestamps))
	for i, ts := range timestamps {
		tsIndex[ts] = i
	}

	// Initialize series values with nil.
	for _, sd := range seriesMap {
		sd.Values = make([]*float64, len(timestamps))
	}

	// Fill values.
	for _, row := range rows {
		key := stableSeriesKey(groupByKeys, row.TagVals)
		if sd, ok := seriesMap[key]; ok {
			if idx, found := tsIndex[row.Ts]; found {
				v := row.AggValue
				sd.Values[idx] = &v
			}
		}
	}

	// Build ordered series slice.
	series := make([]MetricSeriesData, 0, len(seriesOrder))
	for _, key := range seriesOrder {
		series = append(series, *seriesMap[key])
	}

	return &MetricQueryResult{
		Timestamps: timestamps,
		Series:     series,
	}
}

// stableSeriesKey builds a deterministic key from group-by keys and tag values.
func stableSeriesKey(groupByKeys []string, tagVals []string) string {
	var b strings.Builder
	for i, k := range groupByKeys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		if i < len(tagVals) {
			b.WriteString(tagVals[i])
		}
	}
	return b.String()
}
