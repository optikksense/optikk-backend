package explorer

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// Analytics runs a group-by + aggregation query against traces_index.
// For v1 it targets traces_index only; raw-span fallback can be added later.
func (r *Repository) Analytics(ctx context.Context, req AnalyticsRequest, f querycompiler.Filters) ([]AnalyticsRow, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetTracesIndex)
	groupCols := resolveGroupCols(req.GroupBy)
	aggExprs := resolveAggs(req.Aggregations)
	if len(aggExprs) == 0 {
		aggExprs = []string{"count() AS count"}
	}

	selectParts := []string{}
	groupClause := ""
	if req.VizMode == "timeseries" {
		bucketExpr := utils.ExprForColumn(f.StartMs, f.EndMs, "toDateTime(intDiv(start_ms, 1000))")
		selectParts = append(selectParts, bucketExpr+" AS time_bucket")
	}
	selectParts = append(selectParts, groupCols...)
	selectParts = append(selectParts, aggExprs...)

	groupingList := append([]string(nil), groupCols...)
	if req.VizMode == "timeseries" {
		groupingList = append([]string{"time_bucket"}, groupingList...)
	}
	if len(groupingList) > 0 {
		groupClause = " GROUP BY " + strings.Join(groupingList, ", ")
	}

	orderBy := " ORDER BY " + defaultOrder(req, groupingList)
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	query := fmt.Sprintf(`SELECT %s FROM %s PREWHERE %s WHERE %s%s%s LIMIT %d`,
		strings.Join(selectParts, ", "),
		tracesIndexTable, compiled.PreWhere, compiled.Where, groupClause, orderBy, limit,
	)

	rows, err := r.db.Query(dbutil.ExplorerCtx(ctx), query, compiled.Args...)
	if err != nil {
		return nil, compiled.DroppedClauses, err
	}
	defer rows.Close()

	cols := rows.Columns()
	var out []AnalyticsRow
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, compiled.DroppedClauses, err
		}
		row := make(AnalyticsRow, len(cols))
		for i, c := range cols {
			row[c] = values[i]
		}
		out = append(out, row)
	}
	return out, compiled.DroppedClauses, nil
}

func resolveGroupCols(groupBy []string) []string {
	out := make([]string, 0, len(groupBy))
	for _, g := range groupBy {
		switch strings.ToLower(g) {
		case "service", "root_service":
			out = append(out, "root_service")
		case "operation", "root_operation":
			out = append(out, "root_operation")
		case "http_method":
			out = append(out, "root_http_method")
		case "http_status":
			out = append(out, "root_http_status")
		case "status":
			out = append(out, "root_status")
		}
	}
	return out
}

func resolveAggs(aggs []Aggregation) []string {
	out := make([]string, 0, len(aggs))
	for _, a := range aggs {
		alias := a.Alias
		if alias == "" {
			alias = a.Fn
		}
		switch strings.ToLower(a.Fn) {
		case "count":
			out = append(out, fmt.Sprintf("count() AS %s", alias))
		case "error_count":
			out = append(out, fmt.Sprintf("countIf(has_error) AS %s", alias))
		case "avg":
			out = append(out, fmt.Sprintf("avg(duration_ns) AS %s", alias))
		case "p50":
			out = append(out, fmt.Sprintf("quantile(0.50)(duration_ns) AS %s", alias))
		case "p95":
			out = append(out, fmt.Sprintf("quantile(0.95)(duration_ns) AS %s", alias))
		case "p99":
			out = append(out, fmt.Sprintf("quantile(0.99)(duration_ns) AS %s", alias))
		case "uniq":
			out = append(out, fmt.Sprintf("uniq(trace_id) AS %s", alias))
		}
	}
	return out
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
