package analytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// Execute builds and runs a dynamic GROUP BY query against the spans table.
// All groupBy columns and aggregation fields are validated against allow-lists.
func (r *Repository) Execute(ctx context.Context, teamID int64, q AnalyticsQuery) (*AnalyticsResult, error) {
	groupCols, err := resolveGroupBy(q.GroupBy)
	if err != nil {
		return nil, err
	}

	aggExprs, aliases, err := resolveAggregations(q.Aggregations)
	if err != nil {
		return nil, err
	}

	whereFrag, args := buildAnalyticsWhere(teamID, q.Filters)

	selectParts := append(groupCols, aggExprs...)
	groupByParts := make([]string, len(groupCols))
	copy(groupByParts, groupCols)

	orderBy := resolveOrderBy(q.OrderBy, q.OrderDir, aliases, groupCols)
	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(
		`SELECT %s FROM observability.spans s%s GROUP BY %s ORDER BY %s LIMIT %d`,
		strings.Join(selectParts, ", "),
		whereFrag,
		strings.Join(groupByParts, ", "),
		orderBy,
		limit,
	)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(q.GroupBy)+len(aliases))
	columns = append(columns, q.GroupBy...)
	columns = append(columns, aliases...)

	return &AnalyticsResult{
		Columns: columns,
		Rows:    dbutil.NormalizeRows(rows),
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

// allowedAggFields maps user-facing field names to safe ClickHouse expressions.
var allowedAggFields = map[string]string{
	"duration_nano": "s.duration_nano",
	"duration_ms":   "s.duration_nano / 1000000.0",
}

func resolveAggregations(aggs []Aggregation) (exprs []string, aliases []string, err error) {
	for _, a := range aggs {
		if a.Alias == "" {
			return nil, nil, fmt.Errorf("aggregation alias is required")
		}
		expr, buildErr := buildAggExpr(a)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		exprs = append(exprs, expr+` AS `+safeAlias(a.Alias))
		aliases = append(aliases, a.Alias)
	}
	return exprs, aliases, nil
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
		return "count()", nil // caller divides by time range client-side
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

func buildAnalyticsWhere(teamID int64, f QueryFilters) (string, []any) {
	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	const maxRange = 30 * 24 * 60 * 60 * 1000
	if startMs <= 0 || (endMs-startMs) > maxRange {
		startMs = endMs - maxRange
	}

	frag := ` WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`
	args := []any{
		uint32(teamID),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
	}

	if f.SearchMode != "all" {
		frag += ` AND s.parent_span_id = ''`
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

func resolveOrderBy(field, dir string, aliases, groupCols []string) string {
	if dir == "" {
		dir = "DESC"
	}
	dir = strings.ToUpper(dir)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	if field != "" {
		// Check if it matches an alias
		for _, a := range aliases {
			if a == field {
				return safeAlias(field) + " " + dir
			}
		}
		// Check if it matches a groupBy column
		for _, g := range groupCols {
			if g == field {
				return g + " " + dir
			}
		}
	}

	// Default: order by first aggregation
	if len(aliases) > 0 {
		return safeAlias(aliases[0]) + " " + dir
	}
	return groupCols[0] + " " + dir
}

// safeAlias backtick-wraps an alias to prevent injection.
func safeAlias(s string) string {
	// Strip any backticks from the input to prevent escaping
	clean := strings.ReplaceAll(s, "`", "")
	return "`" + clean + "`"
}
