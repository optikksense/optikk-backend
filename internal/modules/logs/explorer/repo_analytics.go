package explorer

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

// Analytics runs a group-by + aggregation query over the logs corpus. When the
// filters + group-by dims are expressible against the rollup cascade we hit
// `logs_rollup_{1m,5m,1h}` via rollup.TierTableFor; otherwise we fall back
// to raw `observability.logs`. Returns the materialized rows + the step
// token used (client aligns x-axis) + compile warnings.
func (r *Repository) Analytics(ctx context.Context, f querycompiler.Filters, req AnalyticsRequest) ([]AnalyticsRow, string, []string, error) {
	viz := normalizeVizMode(req.VizMode)
	plan := planAnalytics(f, req, viz)
	query, args := buildAnalyticsQuery(plan)
	rows, err := r.runAnalyticsQuery(ctx, query, args, plan)
	if err != nil {
		return nil, "", nil, err
	}
	return rows, plan.stepToken, plan.dropped, nil
}

func normalizeVizMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "topn", "table", "pie":
		return strings.ToLower(v)
	default:
		return "timeseries"
	}
}

type analyticsPlan struct {
	table		string
	where		string
	baseArgs	[]any
	groupBy		[]string
	aggs		[]aggSpec
	viz		string
	stepMin		int64
	stepToken	string
	limit		int
	orderBy		string
	rollup		bool
	dropped		[]string
}

type aggSpec struct {
	alias	string
	expr	string
}

// planAnalytics decides rollup-vs-raw + resolves group-by columns, aggs, step.
func planAnalytics(f querycompiler.Filters, req AnalyticsRequest, viz string) analyticsPlan {
	groupBy, groupOK := normalizeGroupBy(req.GroupBy, true)
	if groupOK && canUseRollup(f) {
		table, tierStep := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
		compiled := querycompiler.Compile(f, querycompiler.TargetRollup)
		return analyticsPlan{
			table:	table, where: compiled.Where, baseArgs: compiled.Args,
			groupBy:	groupBy, aggs: buildAggsRollup(req.Aggregations), viz: viz,
			stepMin:	resolveStepMinutes(req.Step, tierStep, f.StartMs, f.EndMs),
			stepToken:	req.Step, limit: pickAnaLimit(req.Limit), orderBy: req.OrderBy,
			rollup:	true, dropped: compiled.DroppedClauses,
		}
	}
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	groupByRaw, _ := normalizeGroupBy(req.GroupBy, false)
	return analyticsPlan{
		table:	rawLogsTable, where: compiled.Where, baseArgs: compiled.Args,
		groupBy:	groupByRaw, aggs: buildAggsRaw(req.Aggregations), viz: viz,
		stepMin:	resolveStepMinutes(req.Step, 1, f.StartMs, f.EndMs),
		stepToken:	req.Step, limit: pickAnaLimit(req.Limit), orderBy: req.OrderBy,
		rollup:	false, dropped: compiled.DroppedClauses,
	}
}

func pickAnaLimit(v int) int {
	if v <= 0 {
		return 100
	}
	if v > 1000 {
		return 1000
	}
	return v
}

// canUseRollup restricts rollup dispatch to the rollup-key dim set.
func canUseRollup(f querycompiler.Filters) bool {
	if f.Search != "" || f.TraceID != "" || f.SpanID != "" || len(f.Attributes) > 0 {
		return false
	}
	return len(f.Containers) == 0
}

var rollupKeyDims = map[string]string{
	"severity_bucket":	"severity_bucket", "severity": "severity_bucket",
	"service":	"service", "service_name": "service",
	"environment":	"environment", "env": "environment",
	"host":	"host", "pod": "pod",
}

var rawKeyDims = map[string]string{
	"severity_bucket":	"severity_bucket", "severity": "severity_text",
	"service":	"service", "service_name": "service",
	"environment":	"environment", "env": "environment",
	"host":	"host", "pod": "pod", "container": "container",
	"trace_id":	"trace_id", "span_id": "span_id",
}

func normalizeGroupBy(in []string, rollupOnly bool) ([]string, bool) {
	out := make([]string, 0, len(in))
	table := rawKeyDims
	if rollupOnly {
		table = rollupKeyDims
	}
	for _, dim := range in {
		col, ok := table[strings.ToLower(strings.TrimSpace(dim))]
		if !ok {
			return nil, false
		}
		out = append(out, col)
	}
	return out, true
}

// buildAnalyticsQuery stitches the plan into a parameterized CH query. Named
// binding is preserved from Compile; no field bindings are introduced here.
func buildAnalyticsQuery(p analyticsPlan) (string, []any) {
	selects, groups := analyticsProjections(p)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(selects, ", "), p.table, p.where)
	if len(groups) > 0 {
		query += " GROUP BY " + strings.Join(groups, ", ")
	}
	query += " ORDER BY " + analyticsOrder(p)
	query += fmt.Sprintf(" LIMIT %d", p.limit)
	return query, p.baseArgs
}

func analyticsProjections(p analyticsPlan) ([]string, []string) {
	var selects, groups []string
	if p.viz == "timeseries" {
		selects = append(selects, analyticsBucketExpr(p)+" AS time_bucket")
		groups = append(groups, "time_bucket")
	}
	for _, g := range p.groupBy {
		selects = append(selects, g)
		groups = append(groups, g)
	}
	for _, agg := range p.aggs {
		selects = append(selects, agg.expr+" AS "+agg.alias)
	}
	return selects, groups
}

func analyticsBucketExpr(p analyticsPlan) string {
	col := "timestamp"
	if p.rollup {
		col = "bucket_ts"
	}
	return fmt.Sprintf(
		"formatDateTime(toStartOfInterval(%s, toIntervalMinute(%d)), '%%Y-%%m-%%d %%H:%%i:00')",
		col, p.stepMin,
	)
}

func analyticsOrder(p analyticsPlan) string {
	if p.orderBy != "" {
		return p.orderBy
	}
	if p.viz == "timeseries" {
		return "time_bucket ASC"
	}
	if len(p.aggs) > 0 {
		return p.aggs[0].alias + " DESC"
	}
	return "1"
}

// runAnalyticsQuery scans the dynamic-shape result into AnalyticsRow via the
// generic Query() path + rows.Scan on per-column `any` pointers.
func (r *Repository) runAnalyticsQuery(ctx context.Context, query string, args []any, p analyticsPlan) ([]AnalyticsRow, error) {
	rows, err := dbutil.QueryCH(dbutil.OverviewCtx(ctx), r.db, "explorer.runAnalyticsQuery", query, args...)
	if err != nil {
		return nil, fmt.Errorf("logs.analytics.query: %w", err)
	}
	defer rows.Close()	//nolint:errcheck

	cols := rows.Columns()
	aggSet := aggAliasSet(p.aggs)
	out := []AnalyticsRow{}
	for rows.Next() {
		values, err := scanAnyRow(rows, len(cols))
		if err != nil {
			return nil, err
		}
		out = append(out, mapAnalyticsRow(cols, values, aggSet))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
