package analytics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const orderDESC = "DESC"

type analyticsRowDTO struct {
	DimensionKeys   []string  `ch:"dimension_keys"`
	DimensionValues []string  `ch:"dimension_values"`
	MetricKeys      []string  `ch:"metric_keys"`
	MetricValues    []float64 `ch:"metric_values"`
}

type analyticsQueryResultDTO struct {
	Columns      []string
	Rows         []analyticsRowDTO
	GroupBy      []string
	Aggregations []Aggregation
}

type Repository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Execute(ctx context.Context, teamID int64, q AnalyticsQuery) (*analyticsQueryResultDTO, error) {
	groupCols, err := resolveGroupBy(q.GroupBy)
	if err != nil {
		return nil, err
	}

	_, aggRawExprs, aliases, err := resolveAggregations(q.Aggregations)
	if err != nil {
		return nil, err
	}

	whereFrag, args := buildAnalyticsWhere(teamID, q.Filters)
	orderBy := resolveOrderBy(q.OrderBy, q.OrderDir, q.GroupBy, aliases, aggRawExprs)
	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(
		`SELECT %s AS dimension_keys,
		        %s AS dimension_values,
		        %s AS metric_keys,
		        %s AS metric_values
		FROM observability.spans s%s
		GROUP BY %s
		ORDER BY %s
		LIMIT %d`,
		stringArrayLiteral(q.GroupBy),
		stringArrayExpr(groupCols),
		stringArrayLiteral(aliases),
		floatArrayExpr(aggRawExprs),
		whereFrag,
		strings.Join(groupCols, ", "),
		orderBy,
		limit,
	)

	var rows []analyticsRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return &analyticsQueryResultDTO{
		Columns:      append(append([]string{}, q.GroupBy...), aliases...),
		Rows:         rows,
		GroupBy:      append([]string{}, q.GroupBy...),
		Aggregations: append([]Aggregation{}, q.Aggregations...),
	}, nil
}

func resolveGroupBy(dims []string) ([]string, error) {
	cols := make([]string, 0, len(dims))
	for _, d := range dims {
		col, ok := LookupDimension(d)
		if !ok {
			return nil, fmt.Errorf("invalid groupBy dimension: %q", d)
		}
		cols = append(cols, col)
	}
	return cols, nil
}

var allowedAggFields = map[string]string{
	"duration_nano": "s.duration_nano",
	"duration_ms":   "s.duration_nano / 1000000.0",
}

func resolveAggregations(aggs []Aggregation) (selectExprs []string, rawExprs []string, aliases []string, err error) {
	for _, a := range aggs {
		if a.Alias == "" {
			return nil, nil, nil, errors.New("aggregation alias is required")
		}
		expr, buildErr := buildAggExpr(a)
		if buildErr != nil {
			return nil, nil, nil, buildErr
		}
		selectExprs = append(selectExprs, expr+` AS `+safeAlias(a.Alias))
		rawExprs = append(rawExprs, expr)
		aliases = append(aliases, a.Alias)
	}
	return selectExprs, rawExprs, aliases, nil
}

func buildAggExpr(a Aggregation) (string, error) {
	resolveField := func() (string, error) {
		if a.Field == "" {
			return "", fmt.Errorf("field required for aggregation type %q", a.Type)
		}
		col, ok := allowedAggFields[a.Field]
		if !ok {
			return "", fmt.Errorf("invalid aggregation field: %q", a.Field)
		}
		return col, nil
	}

	switch a.Type {
	case "count":
		return "count()", nil
	case "countIf":
		return "countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)", nil
	case "rate":
		return "count()", nil
	case "avg", "min", "max", "sum":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s(%s)", a.Type, col), nil
	case "p50":
		col, err := resolveField()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.5)(%s)", col), nil
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
		return "", fmt.Errorf("unsupported aggregation type: %q", a.Type)
	}
}

func buildAnalyticsWhere(teamID int64, f QueryFilters) (frag string, args []any) {
	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	const maxRange = 30 * 24 * 60 * 60 * 1000
	if startMs <= 0 || (endMs-startMs) > maxRange {
		startMs = endMs - maxRange
	}

	frag = ` WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end`
	args = []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	if f.SearchMode != "all" {
		frag += ` AND ` + rootspan.Condition("s")
	}
	if f.SpanKind != "" {
		frag += ` AND s.kind_string = ?`
		args = append(args, f.SpanKind)
	}
	if f.SpanName != "" {
		frag += ` AND s.name = ?`
		args = append(args, f.SpanName)
	}
	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		frag += ` AND s.service_name IN ` + in
		args = append(args, vals...)
	}
	if f.Status == "ERROR" {
		frag += ` AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
	} else if f.Status != "" {
		frag += ` AND s.status_code_string = ?`
		args = append(args, f.Status)
	}
	if f.MinDuration != "" {
		frag += ` AND s.duration_nano >= ?`
		args = append(args, dbutil.MustAtoi64(f.MinDuration, 0)*1_000_000)
	}
	if f.MaxDuration != "" {
		frag += ` AND s.duration_nano <= ?`
		args = append(args, dbutil.MustAtoi64(f.MaxDuration, 0)*1_000_000)
	}
	if f.Operation != "" {
		frag += ` AND s.name LIKE ?`
		args = append(args, "%"+f.Operation+"%")
	}
	if f.HTTPMethod != "" {
		frag += ` AND upper(s.http_method) = upper(?)`
		args = append(args, f.HTTPMethod)
	}
	if f.HTTPStatus != "" {
		frag += ` AND s.response_status_code = ?`
		args = append(args, f.HTTPStatus)
	}
	return frag, args
}

func resolveOrderBy(field, dir string, groupBy []string, aliases, aggExprs []string) string {
	if dir == "" {
		dir = orderDESC
	}
	dir = strings.ToUpper(dir)
	if dir != "ASC" && dir != orderDESC {
		dir = orderDESC
	}

	if field != "" {
		for i, alias := range aliases {
			if alias == field {
				return aggExprs[i] + " " + dir
			}
		}
		for _, dim := range groupBy {
			if dim == field {
				if col, ok := LookupDimension(dim); ok {
					return col + " " + dir
				}
			}
		}
	}

	if len(aggExprs) > 0 {
		return aggExprs[0] + " " + dir
	}
	if len(groupBy) > 0 {
		if col, ok := LookupDimension(groupBy[0]); ok {
			return col + " " + dir
		}
	}
	return "count() DESC"
}

func safeAlias(s string) string {
	clean := strings.ReplaceAll(s, "`", "")
	return "`" + clean + "`"
}

func stringArrayLiteral(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
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
