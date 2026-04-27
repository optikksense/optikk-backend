package trace_analytics	//nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

const tracesIndexTable = "observability.signoz_index_v3"

type Repository interface {
	Analytics(ctx context.Context, req AnalyticsRequest, f querycompiler.Filters) ([]AnalyticsRow, []string, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// Analytics runs a group-by + aggregation query against traces_index.
func (r *ClickHouseRepository) Analytics(ctx context.Context, req AnalyticsRequest, f querycompiler.Filters) ([]AnalyticsRow, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	query := buildAnalyticsQuery(req, f, compiled)
	rows, err := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), r.db, "trace_analytics.Analytics", query, compiled.Args...)
	if err != nil {
		return nil, compiled.DroppedClauses, err
	}
	defer rows.Close()
	out, err := scanAnalyticsRows(rows)
	if err != nil {
		return nil, compiled.DroppedClauses, err
	}
	return out, compiled.DroppedClauses, nil
}

func buildAnalyticsQuery(req AnalyticsRequest, f querycompiler.Filters, compiled querycompiler.Compiled) string {
	groupCols := resolveGroupCols(req.GroupBy)
	aggExprs := resolveAggsOrDefaults(req.Aggregations)
	selectParts, groupingList := buildSelectAndGrouping(req, f, groupCols, aggExprs)
	groupClause := ""
	if len(groupingList) > 0 {
		groupClause = " GROUP BY " + strings.Join(groupingList, ", ")
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	return fmt.Sprintf(`SELECT %s FROM %s PREWHERE %s WHERE %s AND is_root = 1 %s ORDER BY %s LIMIT %d`,
		strings.Join(selectParts, ", "),
		tracesIndexTable, compiled.PreWhere, compiled.Where, groupClause,
		defaultOrder(req, groupingList), limit,
	)
}

func buildSelectAndGrouping(
	req AnalyticsRequest, f querycompiler.Filters,
	groupCols, aggExprs []string,
) ([]string, []string) {
	var selectParts []string
	if req.VizMode == "timeseries" {
		bucketExpr := fmt.Sprintf("toInt64(toUnixTimestamp(%s))", utils.ExprForColumnTime(f.StartMs, f.EndMs, "toDateTime(intDiv(toUnixTimestamp64Nano(timestamp), 1000000000))"))
		selectParts = append(selectParts, bucketExpr+" AS time_bucket")
	}
	selectParts = append(selectParts, groupCols...)
	selectParts = append(selectParts, aggExprs...)
	groupingList := append([]string(nil), groupCols...)
	if req.VizMode == "timeseries" {
		groupingList = append([]string{"time_bucket"}, groupingList...)
	}
	return selectParts, groupingList
}

func resolveAggsOrDefaults(aggs []Aggregation) []string {
	out := resolveAggs(aggs)
	if len(out) == 0 {
		// Datadog-parity default: rate(count) + error_count + p95 on every group-by row.
		return []string{
			"count() AS count",
			"countIf(has_error) AS error_count",
			"quantile(0.95)(duration_ns) AS p95",
		}
	}
	return out
}

func resolveGroupCols(groupBy []string) []string {
	out := make([]string, 0, len(groupBy))
	for _, g := range groupBy {
		if col := groupColumn(g); col != "" {
			out = append(out, col)
		}
	}
	return out
}

func groupColumn(g string) string {
	switch strings.ToLower(g) {
	case "service", "root_service", "service_name":
		return "service_name"
	case "operation", "root_operation", "name":
		return "name"
	case "http_method":
		return "http_method"
	case "http_status", "response_status_code":
		return "response_status_code"
	case "status", "status_code_string":
		return "status_code_string"
	}
	return ""
}

func resolveAggs(aggs []Aggregation) []string {
	out := make([]string, 0, len(aggs))
	for _, a := range aggs {
		if expr := aggExpr(a); expr != "" {
			out = append(out, expr)
		}
	}
	return out
}

func aggExpr(a Aggregation) string {
	alias := a.Alias
	if alias == "" {
		alias = a.Fn
	}
	switch strings.ToLower(a.Fn) {
	case "count":
		return fmt.Sprintf("count() AS %s", alias)
	case "error_count":
		return fmt.Sprintf("countIf(has_error) AS %s", alias)
	case "avg":
		return fmt.Sprintf("avg(duration_nano) AS %s", alias)
	case "p50":
		return fmt.Sprintf("quantile(0.50)(duration_nano) AS %s", alias)
	case "p95":
		return fmt.Sprintf("quantile(0.95)(duration_nano) AS %s", alias)
	case "p99":
		return fmt.Sprintf("quantile(0.99)(duration_nano) AS %s", alias)
	case "uniq":
		return fmt.Sprintf("uniq(trace_id) AS %s", alias)
	}
	return ""
}

func defaultOrder(req AnalyticsRequest, grouping []string) string {
	if req.OrderBy != "" {
		return req.OrderBy
	}
	if req.VizMode == "timeseries" {
		return "time_bucket ASC"
	}
	if len(grouping) == 0 {
		return "1"
	}
	return "2 DESC"
}

func scanAnalyticsRows(rows interface {
	Columns() []string
	Next() bool
	Scan(dest ...any) error
}) ([]AnalyticsRow, error) {
	cols := rows.Columns()
	var out []AnalyticsRow
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(AnalyticsRow, len(cols))
		for i, c := range cols {
			row[c] = values[i]
		}
		out = append(out, row)
	}
	return out, nil
}
