package explorer

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

type analyticsSource string

const (
	analyticsSourceRaw    analyticsSource = "raw"
	analyticsSourceVolume analyticsSource = "volume"
	analyticsSourceFacets analyticsSource = "facets"
)

// Analytics runs a group-by + aggregation query over the logs corpus. When the
// filters + group-by dims are expressible against a rollup cascade we stay on
// pre-aggregated tables; otherwise we fall back to raw `observability.logs`.
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

type analyticsGroupSpec struct {
	Expr  string
	Alias string
}

type analyticsPlan struct {
	table     string
	where     string
	baseArgs  []any
	groupBy   []analyticsGroupSpec
	aggs      []aggSpec
	viz       string
	stepMin   int64
	stepToken string
	limit     int
	orderBy   string
	source    analyticsSource
	dropped   []string
}

type aggSpec struct {
	alias string
	expr  string
}

func planAnalytics(f querycompiler.Filters, req AnalyticsRequest, viz string) analyticsPlan {
	if canUseRollup(f) {
		groupBy, groupOK := normalizeRollupGroupBy(req.GroupBy)
		if groupOK {
			if table, tierStep, aggs, source, ok := chooseRollupAnalyticsSource(f, req, groupBy); ok {
				compiled := querycompiler.Compile(f, querycompiler.TargetRollup)
				return analyticsPlan{
					table:     table,
					where:     compiled.Where,
					baseArgs:  compiled.Args,
					groupBy:   groupBy,
					aggs:      aggs,
					viz:       viz,
					stepMin:   resolveStepMinutes(req.Step, tierStep, f.StartMs, f.EndMs),
					stepToken: req.Step,
					limit:     pickAnaLimit(req.Limit),
					orderBy:   req.OrderBy,
					source:    source,
					dropped:   compiled.DroppedClauses,
				}
			}
		}
	}

	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	groupByRaw, _ := normalizeRawGroupBy(req.GroupBy)
	return analyticsPlan{
		table:     rawLogsTable,
		where:     compiled.Where,
		baseArgs:  compiled.Args,
		groupBy:   groupByRaw,
		aggs:      buildAggsRaw(req.Aggregations),
		viz:       viz,
		stepMin:   resolveStepMinutes(req.Step, 1, f.StartMs, f.EndMs),
		stepToken: req.Step,
		limit:     pickAnaLimit(req.Limit),
		orderBy:   req.OrderBy,
		source:    analyticsSourceRaw,
		dropped:   compiled.DroppedClauses,
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

func canUseRollup(f querycompiler.Filters) bool {
	if f.Search != "" || f.TraceID != "" || f.SpanID != "" || len(f.Attributes) > 0 {
		return false
	}
	return len(f.Containers) == 0
}

func chooseRollupAnalyticsSource(
	f querycompiler.Filters, req AnalyticsRequest, groupBy []analyticsGroupSpec,
) (table string, tierStep int64, aggs []aggSpec, source analyticsSource, ok bool) {
	if requiresFacetRollup(req.Aggregations) {
		if facetedAggs, facetOK := buildAggsRollupFacets(req.Aggregations); facetOK && canUseFacetRollup(f, groupBy) {
			tier := rollup.For(logsFacetRollupPrefix, f.StartMs, f.EndMs)
			table, tierStep = tier.Table, tier.StepMin
			return table, tierStep, facetedAggs, analyticsSourceFacets, true
		}
		return "", 0, nil, "", false
	}
	if volumeAggs, volumeOK := buildAggsRollupVolume(req.Aggregations); volumeOK {
		tier := rollup.For(logsRollupPrefix, f.StartMs, f.EndMs)
		table, tierStep = tier.Table, tier.StepMin
		return table, tierStep, volumeAggs, analyticsSourceVolume, true
	}
	if facetedAggs, facetOK := buildAggsRollupFacets(req.Aggregations); facetOK && canUseFacetRollup(f, groupBy) {
		tier := rollup.For(logsFacetRollupPrefix, f.StartMs, f.EndMs)
		table, tierStep = tier.Table, tier.StepMin
		return table, tierStep, facetedAggs, analyticsSourceFacets, true
	}
	return "", 0, nil, "", false
}

func requiresFacetRollup(in []Aggregation) bool {
	for _, agg := range in {
		switch agg.Fn {
		case "uniq", "count_distinct":
			return true
		}
	}
	return false
}

func canUseFacetRollup(f querycompiler.Filters, groupBy []analyticsGroupSpec) bool {
	if len(f.Hosts) > 0 || len(f.Pods) > 0 || len(f.ExcludeHosts) > 0 {
		return false
	}
	for _, spec := range groupBy {
		switch spec.Alias {
		case "service", "environment", "severity_text", "severity_bucket":
			continue
		default:
			return false
		}
	}
	return true
}

func normalizeRollupGroupBy(in []string) ([]analyticsGroupSpec, bool) {
	out := make([]analyticsGroupSpec, 0, len(in))
	for _, raw := range in {
		dim := strings.ToLower(strings.TrimSpace(raw))
		switch dim {
		case "severity", "severity_text":
			out = append(out, analyticsGroupSpec{
				Expr:  severityBucketLabelExpr("severity_bucket"),
				Alias: "severity_text",
			})
		case "severity_bucket":
			out = append(out, analyticsGroupSpec{Expr: "severity_bucket", Alias: "severity_bucket"})
		case "service", "service_name":
			out = append(out, analyticsGroupSpec{Expr: "service", Alias: "service"})
		case "environment", "env":
			out = append(out, analyticsGroupSpec{Expr: "environment", Alias: "environment"})
		case "host":
			out = append(out, analyticsGroupSpec{Expr: "host", Alias: "host"})
		case "pod":
			out = append(out, analyticsGroupSpec{Expr: "pod", Alias: "pod"})
		default:
			return nil, false
		}
	}
	return out, true
}

func normalizeRawGroupBy(in []string) ([]analyticsGroupSpec, bool) {
	out := make([]analyticsGroupSpec, 0, len(in))
	for _, raw := range in {
		dim := strings.ToLower(strings.TrimSpace(raw))
		switch dim {
		case "severity", "severity_text":
			out = append(out, analyticsGroupSpec{Expr: "severity_text", Alias: "severity_text"})
		case "severity_bucket":
			out = append(out, analyticsGroupSpec{Expr: "severity_bucket", Alias: "severity_bucket"})
		case "service", "service_name":
			out = append(out, analyticsGroupSpec{Expr: "service", Alias: "service"})
		case "environment", "env":
			out = append(out, analyticsGroupSpec{Expr: "environment", Alias: "environment"})
		case "host":
			out = append(out, analyticsGroupSpec{Expr: "host", Alias: "host"})
		case "pod":
			out = append(out, analyticsGroupSpec{Expr: "pod", Alias: "pod"})
		case "container":
			out = append(out, analyticsGroupSpec{Expr: "container", Alias: "container"})
		case "trace_id":
			out = append(out, analyticsGroupSpec{Expr: "trace_id", Alias: "trace_id"})
		case "span_id":
			out = append(out, analyticsGroupSpec{Expr: "span_id", Alias: "span_id"})
		default:
			return nil, false
		}
	}
	return out, true
}

func severityBucketLabelExpr(col string) string {
	return fmt.Sprintf(
		"multiIf(%[1]s = 0, 'trace', %[1]s = 1, 'debug', %[1]s = 2, 'info', %[1]s = 3, 'warn', %[1]s = 4, 'error', 'fatal')",
		col,
	)
}

func buildAnalyticsQuery(p analyticsPlan) (string, []any) {
	selects, groups, validFields := analyticsProjections(p)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(selects, ", "), p.table, p.where)
	if len(groups) > 0 {
		query += " GROUP BY " + strings.Join(groups, ", ")
	}
	query += " ORDER BY " + analyticsOrder(p, validFields)
	query += fmt.Sprintf(" LIMIT %d", p.limit)
	return query, p.baseArgs
}

func analyticsProjections(p analyticsPlan) ([]string, []string, map[string]struct{}) {
	validFields := make(map[string]struct{}, len(p.groupBy)+len(p.aggs)+1)
	var selects, groups []string
	if p.viz == "timeseries" {
		selects = append(selects, analyticsBucketExpr(p)+" AS time_bucket")
		groups = append(groups, "time_bucket")
		validFields["time_bucket"] = struct{}{}
	}
	for _, g := range p.groupBy {
		selects = append(selects, g.Expr+" AS "+g.Alias)
		groups = append(groups, g.Alias)
		validFields[g.Alias] = struct{}{}
	}
	for _, agg := range p.aggs {
		selects = append(selects, agg.expr+" AS "+agg.alias)
		validFields[agg.alias] = struct{}{}
	}
	return selects, groups, validFields
}

func analyticsBucketExpr(p analyticsPlan) string {
	col := "timestamp"
	if p.source != analyticsSourceRaw {
		col = "bucket_ts"
	}
	return fmt.Sprintf(
		"formatDateTime(toStartOfInterval(%s, toIntervalMinute(%d)), '%%Y-%%m-%%d %%H:%%i:00')",
		col, p.stepMin,
	)
}

func analyticsOrder(p analyticsPlan, validFields map[string]struct{}) string {
	if clause, ok := sanitizeOrderBy(p.orderBy, validFields); ok {
		return clause
	}
	if p.viz == "timeseries" {
		return "time_bucket ASC"
	}
	if len(p.aggs) > 0 {
		return p.aggs[0].alias + " DESC"
	}
	return "1"
}

func sanitizeOrderBy(raw string, validFields map[string]struct{}) (string, bool) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 || len(parts) > 2 {
		return "", false
	}
	field := strings.ToLower(parts[0])
	if _, ok := validFields[field]; !ok {
		return "", false
	}
	dir := "ASC"
	if len(parts) == 2 {
		switch strings.ToUpper(parts[1]) {
		case "ASC", "DESC":
			dir = strings.ToUpper(parts[1])
		default:
			return "", false
		}
	}
	return field + " " + dir, true
}

func (r *Repository) runAnalyticsQuery(ctx context.Context, query string, args []any, p analyticsPlan) ([]AnalyticsRow, error) {
	rows, err := dbutil.QueryCH(dbutil.OverviewCtx(ctx), r.db, "explorer.runAnalyticsQuery", query, args...)
	if err != nil {
		return nil, fmt.Errorf("logs.analytics.query: %w", err)
	}
	defer rows.Close() //nolint:errcheck

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
