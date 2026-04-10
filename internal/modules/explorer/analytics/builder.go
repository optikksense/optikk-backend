package analytics

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	maxGroupBy      = 4
	maxAggregations = 8
	maxLimit        = 1000
	defaultLimit    = 100
)

// BuildQuery constructs a ClickHouse SQL query from an AnalyticsRequest and ScopeConfig.
// The queryWhere is the compiled WHERE fragment from the query parser (may be empty).
// Returns the SQL string and args for parameterized execution.
func BuildQuery(req AnalyticsRequest, cfg ScopeConfig, queryWhere string, queryArgs []any) (string, []any, error) {
	if err := validateRequest(req, cfg); err != nil {
		return "", nil, err
	}

	groupCols, err := resolveGroupBy(req.GroupBy, cfg)
	if err != nil {
		return "", nil, err
	}

	aggExprs, aliases, err := resolveAggregations(req.Aggregations, cfg)
	if err != nil {
		return "", nil, err
	}

	baseFrag, baseArgs := cfg.BaseWhereFunc(0, req.StartTime, req.EndTime)

	// Merge query parser WHERE.
	whereFrag := baseFrag
	args := baseArgs
	if queryWhere != "" {
		whereFrag += " AND " + queryWhere
		args = append(args, queryArgs...)
	}

	isTimeseries := req.VizMode == VizTimeseries && req.Step != ""

	// Build dimension keys/values for the array-based DTO pattern.
	dimKeys := req.GroupBy
	dimColExprs := groupCols

	if isTimeseries {
		bucketExpr := cfg.TimeBucketFunc(req.StartTime, req.EndTime, req.Step)
		dimKeys = append([]string{"time_bucket"}, dimKeys...)
		dimColExprs = append([]string{bucketExpr}, dimColExprs...)
	}

	var groupByParts []string
	if isTimeseries {
		groupByParts = append(groupByParts, dimColExprs[0]) // time bucket expr
	}
	groupByParts = append(groupByParts, groupCols...)

	// Build ORDER BY.
	orderBy := resolveOrderBy(req, aliases, aggExprs, groupCols, isTimeseries)

	limit := req.Limit
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}

	tableRef := cfg.Table
	if cfg.TableAlias != "" {
		tableRef += " " + cfg.TableAlias
	}

	query := fmt.Sprintf(
		"SELECT %s AS dimension_keys, %s AS dimension_values, %s AS metric_keys, %s AS metric_values FROM %s WHERE %s GROUP BY %s ORDER BY %s LIMIT %d",
		stringArrayLiteral(dimKeys),
		stringArrayExpr(dimColExprs),
		stringArrayLiteral(aliases),
		floatArrayExpr(aggExprs),
		tableRef,
		whereFrag,
		strings.Join(groupByParts, ", "),
		orderBy,
		limit,
	)

	return query, args, nil
}

func validateRequest(req AnalyticsRequest, cfg ScopeConfig) error {
	if len(req.GroupBy) == 0 {
		return fmt.Errorf("at least one groupBy dimension is required")
	}
	if len(req.GroupBy) > maxGroupBy {
		return fmt.Errorf("maximum %d groupBy dimensions allowed", maxGroupBy)
	}
	if len(req.Aggregations) == 0 {
		return fmt.Errorf("at least one aggregation is required")
	}
	if len(req.Aggregations) > maxAggregations {
		return fmt.Errorf("maximum %d aggregations allowed", maxAggregations)
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		return fmt.Errorf("valid startTime and endTime are required")
	}

	seen := make(map[string]bool, len(req.Aggregations))
	for _, a := range req.Aggregations {
		if a.Alias == "" {
			return fmt.Errorf("aggregation alias is required")
		}
		if seen[a.Alias] {
			return fmt.Errorf("duplicate aggregation alias: %q", a.Alias)
		}
		seen[a.Alias] = true
	}
	_ = cfg
	return nil
}

func resolveGroupBy(dims []string, cfg ScopeConfig) ([]string, error) {
	cols := make([]string, 0, len(dims))
	for _, d := range dims {
		col, ok := cfg.Dimensions[d]
		if !ok {
			return nil, fmt.Errorf("invalid groupBy dimension: %q", d)
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func resolveAggregations(aggs []Aggregation, cfg ScopeConfig) (exprs []string, aliases []string, err error) {
	for _, a := range aggs {
		expr, buildErr := buildAggExpr(a, cfg)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		exprs = append(exprs, expr)
		aliases = append(aliases, a.Alias)
	}
	return exprs, aliases, nil
}

func buildAggExpr(a Aggregation, cfg ScopeConfig) (string, error) {
	resolveField := func() (string, error) {
		if a.Field == "" {
			return "", fmt.Errorf("field required for aggregation function %q", a.Function)
		}
		col, ok := cfg.AggFields[a.Field]
		if !ok {
			return "", fmt.Errorf("invalid aggregation field: %q", a.Field)
		}
		return col, nil
	}

	switch a.Function {
	case "count":
		return "count()", nil
	case "count_unique":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("uniq(%s)", col), nil
	case "avg", "min", "max", "sum":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s(%s)", a.Function, col), nil
	case "p50":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.5)(%s)", col), nil
	case "p75":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.75)(%s)", col), nil
	case "p90":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.9)(%s)", col), nil
	case "p95":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.95)(%s)", col), nil
	case "p99":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.99)(%s)", col), nil
	default:
		return "", fmt.Errorf("unsupported aggregation function: %q", a.Function)
	}
}

func resolveOrderBy(req AnalyticsRequest, aliases []string, aggExprs []string, groupCols []string, isTimeseries bool) string {
	dir := strings.ToUpper(req.OrderDir)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	if isTimeseries {
		// Timeseries always sorted by time first.
		secondary := ""
		if len(aggExprs) > 0 {
			secondary = ", " + aggExprs[0] + " " + dir
		}
		return "time_bucket ASC" + secondary
	}

	if req.OrderBy != "" {
		for i, alias := range aliases {
			if alias == req.OrderBy {
				return aggExprs[i] + " " + dir
			}
		}
		for i, dim := range req.GroupBy {
			if dim == req.OrderBy {
				return groupCols[i] + " " + dir
			}
		}
	}

	// Default: first aggregation descending.
	if len(aggExprs) > 0 {
		return aggExprs[0] + " " + dir
	}
	return "count() DESC"
}

func stringArrayLiteral(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "\\'") + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func stringArrayExpr(exprs []string) string {
	items := make([]string, len(exprs))
	for i, expr := range exprs {
		items[i] = fmt.Sprintf("toString(%s)", expr)
	}
	return "[" + strings.Join(items, ", ") + "]"
}

func floatArrayExpr(exprs []string) string {
	items := make([]string, len(exprs))
	for i, expr := range exprs {
		items[i] = fmt.Sprintf("toFloat64(%s)", expr)
	}
	return "[" + strings.Join(items, ", ") + "]"
}

// AnalyticsRowDTO mirrors the ClickHouse result: arrays of keys/values.
type AnalyticsRowDTO struct {
	DimensionKeys   []string  `ch:"dimension_keys"`
	DimensionValues []string  `ch:"dimension_values"`
	MetricKeys      []string  `ch:"metric_keys"`
	MetricValues    []float64 `ch:"metric_values"`
}

// BuildResult converts raw ClickHouse rows into the structured analytics response.
func BuildResult(req AnalyticsRequest, rows []AnalyticsRowDTO) *AnalyticsResult {
	isTimeseries := req.VizMode == VizTimeseries && req.Step != ""

	var columns []string
	if isTimeseries {
		columns = append(columns, "time_bucket")
	}
	columns = append(columns, req.GroupBy...)
	for _, a := range req.Aggregations {
		columns = append(columns, a.Alias)
	}

	resultRows := make([]AnalyticsRow, 0, len(rows))
	for _, row := range rows {
		cells := make([]AnalyticsCell, 0, len(row.DimensionKeys)+len(row.MetricKeys))
		for i, key := range row.DimensionKeys {
			if i < len(row.DimensionValues) {
				cells = append(cells, cellFromString(key, row.DimensionValues[i]))
			}
		}
		for i, key := range row.MetricKeys {
			if i < len(row.MetricValues) {
				v := row.MetricValues[i]
				if math.IsNaN(v) || math.IsInf(v, 0) {
					v = 0
				}
				rounded := math.Round(v*100) / 100
				cells = append(cells, AnalyticsCell{Key: key, Type: ValueNumber, NumberValue: &rounded})
			}
		}
		resultRows = append(resultRows, AnalyticsRow{Cells: cells})
	}

	return &AnalyticsResult{
		Columns: columns,
		Rows:    resultRows,
	}
}

func cellFromString(key, val string) AnalyticsCell {
	if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return AnalyticsCell{Key: key, Type: ValueInteger, IntegerValue: &i}
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		rounded := math.Round(f*100) / 100
		return AnalyticsCell{Key: key, Type: ValueNumber, NumberValue: &rounded}
	}
	s := val
	return AnalyticsCell{Key: key, Type: ValueString, StringValue: &s}
}
