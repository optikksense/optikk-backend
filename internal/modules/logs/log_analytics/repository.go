package log_analytics //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/analytics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/resource"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/step"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

type groupSpec struct {
	Expr  string
	Alias string
}

type plan struct {
	preWhere  string
	where     string
	baseArgs  []any
	groupBy   []groupSpec
	aggs      []analytics.Spec
	viz       string
	stepMin   int64
	stepToken string
	limit     int
	orderBy   string
	dropped   []string
}

// Analytics is the ClickHouse-side aggregate path: PREWHERE on raw logs +
// GROUP BY + agg projections.
func (r *Repository) Analytics(ctx context.Context, f querycompiler.Filters, req Request) ([]models.AnalyticsRow, string, []string, error) {
	viz := normalizeVizMode(req.VizMode)
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return nil, "", nil, err
	}
	if empty {
		return nil, req.Step, compiled.DroppedClauses, nil
	}

	groupBy, _ := normalizeRawGroupBy(req.GroupBy)
	p := plan{
		preWhere:  preWhere,
		where:     compiled.Where,
		baseArgs:  args,
		groupBy:   groupBy,
		aggs:      analytics.BuildAggsRaw(req.Aggregations),
		viz:       viz,
		stepMin:   step.ResolveStepMinutes(req.Step, 1, f.StartMs, f.EndMs),
		stepToken: req.Step,
		limit:     pickLimit(req.Limit),
		orderBy:   req.OrderBy,
		dropped:   compiled.DroppedClauses,
	}

	query, qargs := buildQuery(p)
	rows, err := r.runQuery(ctx, query, qargs, p)
	fmt.Println("QUERY:", query)
	if err != nil {
		return nil, "", nil, err
	}
	return rows, p.stepToken, p.dropped, nil
}

func normalizeVizMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "topn", "table", "pie":
		return strings.ToLower(v)
	default:
		return "timeseries"
	}
}

func pickLimit(v int) int {
	if v <= 0 {
		return 100
	}
	if v > 1000 {
		return 1000
	}
	return v
}

func normalizeRawGroupBy(in []string) ([]groupSpec, bool) {
	out := make([]groupSpec, 0, len(in))
	for _, raw := range in {
		dim := strings.ToLower(strings.TrimSpace(raw))
		switch dim {
		case "severity", "severity_text":
			out = append(out, groupSpec{Expr: "severity_text", Alias: raw})
		case "severity_bucket":
			out = append(out, groupSpec{Expr: "severity_bucket", Alias: raw})
		case "service", "service_name":
			out = append(out, groupSpec{Expr: "service", Alias: raw})
		case "environment", "env":
			out = append(out, groupSpec{Expr: "environment", Alias: raw})
		case "host":
			out = append(out, groupSpec{Expr: "host", Alias: raw})
		case "pod":
			out = append(out, groupSpec{Expr: "pod", Alias: raw})
		case "container":
			out = append(out, groupSpec{Expr: "container", Alias: raw})
		case "trace_id":
			out = append(out, groupSpec{Expr: "trace_id", Alias: raw})
		case "span_id":
			out = append(out, groupSpec{Expr: "span_id", Alias: raw})
		default:
			return nil, false
		}
	}
	return out, true
}

func buildQuery(p plan) (string, []any) {
	selects, groups, validFields := projections(p)
	query := fmt.Sprintf("SELECT %s FROM %s PREWHERE %s WHERE %s", strings.Join(selects, ", "), models.RawLogsTable, p.preWhere, p.where)
	if len(groups) > 0 {
		query += " GROUP BY " + strings.Join(groups, ", ")
	}
	query += " ORDER BY " + orderClause(p, validFields)
	query += fmt.Sprintf(" LIMIT %d", p.limit)
	return query, p.baseArgs
}

func projections(p plan) ([]string, []string, map[string]struct{}) {
	validFields := make(map[string]struct{}, len(p.groupBy)+len(p.aggs)+1)
	var selects, groups []string
	if p.viz == "timeseries" {
		selects = append(selects, bucketExpr(p)+" AS time_bucket")
		groups = append(groups, "time_bucket")
		validFields["time_bucket"] = struct{}{}
	}
	for _, g := range p.groupBy {
		selects = append(selects, g.Expr+" AS "+g.Alias)
		groups = append(groups, g.Alias)
		validFields[g.Alias] = struct{}{}
	}
	for _, agg := range p.aggs {
		selects = append(selects, agg.Expr+" AS "+agg.Alias)
		validFields[agg.Alias] = struct{}{}
	}
	return selects, groups, validFields
}

func bucketExpr(p plan) string {
	return fmt.Sprintf(
		"toStartOfInterval(timestamp, toIntervalMinute(%d))",
		p.stepMin,
	)
}

func orderClause(p plan, validFields map[string]struct{}) string {
	if clause, ok := sanitizeOrderBy(p.orderBy, validFields); ok {
		return clause
	}
	if p.viz == "timeseries" {
		return "time_bucket ASC"
	}
	if len(p.aggs) > 0 {
		return p.aggs[0].Alias + " DESC"
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

func (r *Repository) runQuery(ctx context.Context, query string, args []any, p plan) ([]models.AnalyticsRow, error) {
	rows, err := dbutil.QueryCH(dbutil.OverviewCtx(ctx), r.db, "logsAnalytics.runQuery", query, args...)
	if err != nil {
		return nil, fmt.Errorf("logs.analytics.query: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	cols := rows.Columns()
	aggSet := analytics.AliasSet(p.aggs)
	out := []models.AnalyticsRow{}
	for rows.Next() {
		values, err := analytics.ScanAnyRow(rows, len(cols))
		if err != nil {
			return nil, err
		}
		out = append(out, analytics.MapRow(cols, values, aggSet))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

var _ = clickhouse.Named
