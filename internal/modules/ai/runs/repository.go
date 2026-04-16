package runs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	aishared "github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
)

// Repository defines the data access methods for the AI runs module.
type Repository interface {
	SearchRuns(ctx context.Context, q aishared.QueryContext, limit, offset int, orderBy, orderDir string) ([]aishared.AIRunRow, uint64, error)
	SummarizeRuns(ctx context.Context, q aishared.QueryContext) (aishared.AIOverview, error)
	TrendRuns(ctx context.Context, q aishared.QueryContext, step string) ([]aishared.AITrendPoint, error)
	FacetRuns(ctx context.Context, q aishared.QueryContext) (AIExplorerFacets, error)
	RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest, queryWhere string, queryArgs []any) (*analytics.AnalyticsResult, error)
	GetRunDetail(ctx context.Context, teamID int64, traceID, spanID string) (*aishared.AIRunDetail, error)
	GetRunLogs(ctx context.Context, teamID int64, traceID, spanID string, limit int) ([]logRow, error)
	GetMatchingAlerts(ctx context.Context, teamID int64, detail *aishared.AIRunDetail) ([]AIAlertTargetRef, error)
}

type repository struct {
	db     *dbutil.NativeQuerier
	sqlDB  *sql.DB
}

// NewRepository creates a new runs repository.
func NewRepository(db *dbutil.NativeQuerier, sqlDB *sql.DB) Repository {
	return &repository{db: db, sqlDB: sqlDB}
}

func (r *repository) SearchRuns(ctx context.Context, q aishared.QueryContext, limit, offset int, orderBy, orderDir string) ([]aishared.AIRunRow, uint64, error) {
	if limit <= 0 || limit > aishared.MaxPageSize {
		limit = aishared.DefaultPageSize
	}
	if offset < 0 {
		offset = 0
	}

	where, args := aishared.BuildWhereClause(q)
	orderExpr := aishared.ResolveOrderBy(orderBy)
	direction := "DESC"
	if strings.EqualFold(strings.TrimSpace(orderDir), "asc") {
		direction = "ASC"
	}

	query := fmt.Sprintf(`
		SELECT
			ai.run_id, ai.trace_id, ai.span_id, ai.service_name,
			ai.provider, ai.request_model, ai.response_model, ai.operation,
			ai.prompt_template, ai.prompt_template_version,
			ai.conversation_id, ai.session_id, ai.finish_reason,
			ai.guardrail_state, ai.status, ai.status_message, ai.has_error,
			ai.start_time, ai.latency_ms, ai.ttft_ms,
			ai.input_tokens, ai.output_tokens, ai.total_tokens,
			ai.cost_usd, ai.quality_score, ai.feedback_score, ai.quality_bucket
		FROM %s ai
		WHERE %s
		ORDER BY %s %s, ai.run_id DESC
		LIMIT %d OFFSET %d
	`, aishared.AIRunsSubquery(), where, orderExpr, direction, limit, offset)

	var rows []aishared.AIRunRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, 0, err
	}

	countQuery := fmt.Sprintf(`SELECT count() FROM %s ai WHERE %s`, aishared.AIRunsSubquery(), where)
	var total uint64
	if err := r.db.QueryRow(ctx, &total, countQuery, args...); err != nil {
		return nil, 0, err
	}

	return rows, total, nil
}

func (r *repository) SummarizeRuns(ctx context.Context, q aishared.QueryContext) (aishared.AIOverview, error) {
	return aishared.SummarizeRuns(ctx, r.db, q)
}

func (r *repository) TrendRuns(ctx context.Context, q aishared.QueryContext, step string) ([]aishared.AITrendPoint, error) {
	return aishared.TrendRuns(ctx, r.db, q, step)
}

func (r *repository) FacetRuns(ctx context.Context, q aishared.QueryContext) (AIExplorerFacets, error) {
	where, args := aishared.BuildWhereClause(q)
	source := aishared.AIRunsSubquery()
	definitions := []facetDefinition{
		{key: "provider", label: "Provider", expr: "ai.provider"},
		{key: "request_model", label: "Model", expr: "ai.request_model"},
		{key: "operation", label: "Operation", expr: "ai.operation"},
		{key: "service_name", label: "Service", expr: "ai.service_name"},
		{key: "prompt_template", label: "Prompt template", expr: "ai.prompt_template"},
		{key: "finish_reason", label: "Finish reason", expr: "ai.finish_reason"},
		{key: "guardrail_state", label: "Guardrail", expr: "if(ai.guardrail_state = '', 'not_evaluated', ai.guardrail_state)"},
		{key: "quality_bucket", label: "Quality", expr: "ai.quality_bucket"},
		{key: "status", label: "Status", expr: "ai.status"},
	}

	out := AIExplorerFacets{}
	for _, def := range definitions {
		query := fmt.Sprintf(`
			SELECT %s AS value, count() AS count
			FROM %s ai
			WHERE %s AND %s != ''
			GROUP BY value
			ORDER BY count DESC
			LIMIT %d
		`, def.expr, source, where, def.expr, aishared.DefaultBreakdownLimit)

		var buckets []aishared.FacetBucket
		if err := r.db.Select(ctx, &buckets, query, args...); err != nil {
			return AIExplorerFacets{}, err
		}

		switch def.key {
		case "provider":
			out.Provider = buckets
		case "request_model":
			out.RequestModel = buckets
		case "operation":
			out.Operation = buckets
		case "service_name":
			out.ServiceName = buckets
		case "prompt_template":
			out.PromptTemplate = buckets
		case "finish_reason":
			out.FinishReason = buckets
		case "guardrail_state":
			out.GuardrailState = buckets
		case "quality_bucket":
			out.QualityBucket = buckets
		case "status":
			out.Status = buckets
		}
	}

	return out, nil
}

func (r *repository) RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest, queryWhere string, queryArgs []any) (*analytics.AnalyticsResult, error) {
	cfg := analytics.ScopeConfig{
		Table:      aishared.AIRunsSubquery(),
		TableAlias: "ai",
		TimeBucketFunc: func(_, _ int64, step string) string {
			return aishared.ResolveTimeBucket(step)
		},
		BaseWhereFunc: func(_ int64, startMs, endMs int64) (string, []any) {
			return aishared.BuildWhereClause(aishared.QueryContext{TeamID: teamID, Start: startMs, End: endMs})
		},
		Dimensions: map[string]string{
			"provider": "ai.provider", "service": "ai.service_name",
			"service_name": "ai.service_name", "model": "ai.request_model",
			"request_model": "ai.request_model", "operation": "ai.operation",
			"prompt_template": "ai.prompt_template", "session_id": "ai.session_id",
			"conversation_id": "ai.conversation_id", "finish_reason": "ai.finish_reason",
			"guardrail_state": "if(ai.guardrail_state = '', 'not_evaluated', ai.guardrail_state)",
			"quality_bucket": "ai.quality_bucket", "status": "ai.status",
		},
		AggFields: map[string]string{
			"latency_ms": "ai.latency_ms", "ttft_ms": "ai.ttft_ms",
			"input_tokens": "ai.input_tokens", "output_tokens": "ai.output_tokens",
			"total_tokens": "ai.total_tokens", "cost_usd": "ai.cost_usd",
			"quality_score": "ai.quality_score",
		},
	}

	sqlQuery, args, err := analytics.BuildQuery(req, cfg, queryWhere, queryArgs)
	if err != nil {
		return nil, err
	}

	var rows []analytics.AnalyticsRowDTO
	if err := r.db.Select(ctx, &rows, sqlQuery, args...); err != nil {
		return nil, err
	}
	return analytics.BuildResult(req, rows), nil
}

func (r *repository) GetRunDetail(ctx context.Context, teamID int64, traceID, spanID string) (*aishared.AIRunDetail, error) {
	query := fmt.Sprintf(`
		SELECT
			ai.run_id, ai.trace_id, ai.span_id, ai.service_name,
			ai.provider, ai.request_model, ai.response_model, ai.operation,
			ai.prompt_template, ai.prompt_template_version,
			ai.conversation_id, ai.session_id, ai.finish_reason,
			ai.guardrail_state, ai.status, ai.status_message, ai.has_error,
			ai.start_time, ai.latency_ms, ai.ttft_ms,
			ai.input_tokens, ai.output_tokens, ai.total_tokens,
			ai.cost_usd, ai.quality_score, ai.feedback_score, ai.quality_bucket,
			ai.span_name, ai.service_version, ai.environment,
			ai.prompt_snippet, ai.response_snippet, ai.cache_hit, ai.attributes
		FROM %s ai
		WHERE ai.team_id = @teamID AND ai.trace_id = @traceID AND ai.span_id = @spanID
		LIMIT 1
	`, aishared.AIRunsSubquery())

	var rows []aishared.AIRunDetail
	if err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
	); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (r *repository) GetRunLogs(ctx context.Context, teamID int64, traceID, spanID string, limit int) ([]logRow, error) {
	if limit <= 0 || limit > 200 {
		limit = aishared.DefaultLogLimit
	}

	query := `
		SELECT timestamp, severity_text, body, service AS service_name, trace_id, span_id
		FROM observability.logs
		WHERE team_id = @teamID AND trace_id = @traceID AND (span_id = @spanID OR @spanID = '')
		ORDER BY timestamp DESC
		LIMIT @limit
	`

	var rows []logRow
	if err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("limit", limit),
	); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *repository) GetMatchingAlerts(ctx context.Context, teamID int64, detail *aishared.AIRunDetail) ([]AIAlertTargetRef, error) {
	if r.sqlDB == nil || detail == nil {
		return nil, nil
	}

	rows, err := r.sqlDB.QueryContext(ctx, `
		SELECT id, name, condition_type, rule_state, target_ref
		FROM observability.alerts
		WHERE team_id = ? AND enabled = 1
		ORDER BY id DESC
		LIMIT 200
	`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []alertRow
	for rows.Next() {
		var row alertRow
		if scanErr := rows.Scan(&row.ID, &row.Name, &row.ConditionType, &row.RuleState, &row.TargetRef); scanErr != nil {
			return nil, scanErr
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]AIAlertTargetRef, 0, len(candidates))
	for _, candidate := range candidates {
		var target map[string]any
		if err := json.Unmarshal([]byte(candidate.TargetRef), &target); err != nil {
			continue
		}
		if !targetMatchesRun(target, detail) {
			continue
		}
		out = append(out, AIAlertTargetRef{
			AlertID:       candidate.ID,
			RuleName:      candidate.Name,
			ConditionType: candidate.ConditionType,
			RuleState:     candidate.RuleState,
			TargetRef:     target,
		})
	}
	return out, nil
}

func targetMatchesRun(target map[string]any, detail *aishared.AIRunDetail) bool {
	matchers := 0
	matched := 0

	compare := func(value string, keys ...string) {
		if value == "" {
			return
		}
		for _, key := range keys {
			raw, ok := target[key]
			if !ok {
				continue
			}
			str, ok := raw.(string)
			if !ok || str == "" {
				continue
			}
			matchers++
			if strings.EqualFold(str, value) {
				matched++
			}
			return
		}
	}

	compare(detail.ServiceName, "service_name", "service", "serviceName")
	compare(detail.Provider, "provider")
	compare(detail.RequestModel, "model", "request_model", "requestModel")
	compare(detail.PromptTemplate, "prompt_template", "promptTemplate")
	compare(detail.GuardrailState, "guardrail_state", "guardrailState")

	if matchers == 0 {
		return false
	}
	return matched == matchers
}
