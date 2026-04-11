package ai

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
)

type Repository interface {
	GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error)
	GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error)
	GetTopModels(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIModelBreakdown, error)
	GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIPromptBreakdown, error)
	GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error)
	SearchRuns(ctx context.Context, q queryContext, limit, offset int, orderBy, orderDir string) ([]AIRunRow, uint64, error)
	SummarizeRuns(ctx context.Context, q queryContext) (AIOverview, error)
	TrendRuns(ctx context.Context, q queryContext, step string) ([]AITrendPoint, error)
	FacetRuns(ctx context.Context, q queryContext) (AIExplorerFacets, error)
	RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest, queryWhere string, queryArgs []any) (*analytics.AnalyticsResult, error)
	GetRunDetail(ctx context.Context, teamID int64, traceID, spanID string) (*AIRunDetail, error)
	GetRunLogs(ctx context.Context, teamID int64, traceID, spanID string, limit int) ([]AIRelatedLog, error)
	GetMatchingAlerts(ctx context.Context, teamID int64, detail *AIRunDetail) ([]AIAlertTargetRef, error)
}

type repository struct {
	db  *dbutil.NativeQuerier
	sql *sql.DB
}

func NewRepository(db *dbutil.NativeQuerier, sqlDB *sql.DB) Repository {
	return &repository{db: db, sql: sqlDB}
}

func (r *repository) GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error) {
	q := queryContext{teamID: teamID, start: startMs, end: endMs}
	return r.SummarizeRuns(ctx, q)
}

func (r *repository) GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error) {
	return r.TrendRuns(ctx, queryContext{teamID: teamID, start: startMs, end: endMs}, step)
}

func (r *repository) GetTopModels(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIModelBreakdown, error) {
	if limit <= 0 {
		limit = defaultBreakdownLimit
	}

	where, args := buildAIWhereClause(queryContext{teamID: teamID, start: startMs, end: endMs})
	query := fmt.Sprintf(`
		SELECT
			ai.provider,
			ai.request_model,
			count() AS requests,
			countIf(ai.has_error) AS error_runs,
			avg(ai.latency_ms) AS avg_latency_ms,
			avg(ai.ttft_ms) AS avg_ttft_ms,
			sum(ai.total_tokens) AS total_tokens,
			sum(ai.cost_usd) AS total_cost_usd,
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score
		FROM %s ai
		WHERE %s AND ai.request_model != ''
		GROUP BY ai.provider, ai.request_model
		ORDER BY requests DESC
		LIMIT %d
	`, aiRunsSubquery(), where, limit)

	var rows []AIModelBreakdown
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	for idx := range rows {
		rows[idx].ErrorRatePct = ratioPct(rows[idx].ErrorRuns, rows[idx].Requests)
	}
	return rows, nil
}

func (r *repository) GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIPromptBreakdown, error) {
	if limit <= 0 {
		limit = defaultBreakdownLimit
	}

	where, args := buildAIWhereClause(queryContext{teamID: teamID, start: startMs, end: endMs})
	query := fmt.Sprintf(`
		SELECT
			ai.prompt_template,
			ai.prompt_template_version,
			count() AS requests,
			avg(ai.latency_ms) AS avg_latency_ms,
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score,
			sum(ai.cost_usd) AS total_cost_usd,
			countIf(ai.has_error) AS error_runs
		FROM %s ai
		WHERE %s AND ai.prompt_template != ''
		GROUP BY ai.prompt_template, ai.prompt_template_version
		ORDER BY requests DESC
		LIMIT %d
	`, aiRunsSubquery(), where, limit)

	type promptRow struct {
		AIPromptBreakdown
		ErrorRuns uint64 `ch:"error_runs"`
	}

	var rows []promptRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]AIPromptBreakdown, 0, len(rows))
	for _, row := range rows {
		item := row.AIPromptBreakdown
		item.ErrorRatePct = ratioPct(row.ErrorRuns, item.Requests)
		out = append(out, item)
	}
	return out, nil
}

func (r *repository) GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error) {
	where, args := buildAIWhereClause(queryContext{teamID: teamID, start: startMs, end: endMs})
	source := aiRunsSubquery()

	summaryQuery := fmt.Sprintf(`
		SELECT
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score,
			if(countIf(ai.feedback_score > 0) = 0, 0, avgIf(ai.feedback_score, ai.feedback_score > 0)) AS avg_feedback_score,
			countIf(ai.quality_score > 0) AS scored_runs,
			countIf(ai.feedback_score > 0) AS feedback_runs,
			countIf(lower(ai.guardrail_state) IN ('blocked', 'rejected', 'failed')) AS guardrail_blocks
		FROM %s ai
		WHERE %s
	`, source, where)

	var summary qualitySummaryRow
	if err := r.db.QueryRow(ctx, &summary, summaryQuery, args...); err != nil {
		return AIQualitySummary{}, err
	}

	guardrailQuery := fmt.Sprintf(`
		SELECT
			if(ai.guardrail_state = '', 'not_evaluated', ai.guardrail_state) AS guardrail_state,
			count() AS count
		FROM %s ai
		WHERE %s
		GROUP BY guardrail_state
		ORDER BY count DESC
	`, source, where)
	var guardrailRows []AIGuardrailBreakdown
	if err := r.db.Select(ctx, &guardrailRows, guardrailQuery, args...); err != nil {
		return AIQualitySummary{}, err
	}

	qualityQuery := fmt.Sprintf(`
		SELECT
			ai.quality_bucket AS bucket,
			count() AS count
		FROM %s ai
		WHERE %s
		GROUP BY ai.quality_bucket
		ORDER BY count DESC
	`, source, where)
	var qualityRows []AIQualityBucket
	if err := r.db.Select(ctx, &qualityRows, qualityQuery, args...); err != nil {
		return AIQualitySummary{}, err
	}

	return AIQualitySummary{
		AvgQualityScore:    summary.AvgQualityScore,
		AvgFeedbackScore:   summary.AvgFeedbackScore,
		ScoredRuns:         summary.ScoredRuns,
		FeedbackRuns:       summary.FeedbackRuns,
		GuardrailBlocks:    summary.GuardrailBlocks,
		GuardrailBreakdown: guardrailRows,
		QualityBuckets:     qualityRows,
	}, nil
}

func (r *repository) SearchRuns(ctx context.Context, q queryContext, limit, offset int, orderBy, orderDir string) ([]AIRunRow, uint64, error) {
	if limit <= 0 || limit > maxPageSize {
		limit = defaultPageSize
	}
	if offset < 0 {
		offset = 0
	}

	where, args := buildAIWhereClause(q)
	orderExpr := resolveOrderBy(orderBy)
	direction := "DESC"
	if strings.EqualFold(strings.TrimSpace(orderDir), "asc") {
		direction = "ASC"
	}

	query := fmt.Sprintf(`
		SELECT
			ai.run_id,
			ai.trace_id,
			ai.span_id,
			ai.service_name,
			ai.provider,
			ai.request_model,
			ai.response_model,
			ai.operation,
			ai.prompt_template,
			ai.prompt_template_version,
			ai.conversation_id,
			ai.session_id,
			ai.finish_reason,
			ai.guardrail_state,
			ai.status,
			ai.status_message,
			ai.has_error,
			ai.start_time,
			ai.latency_ms,
			ai.ttft_ms,
			ai.input_tokens,
			ai.output_tokens,
			ai.total_tokens,
			ai.cost_usd,
			ai.quality_score,
			ai.feedback_score,
			ai.quality_bucket
		FROM %s ai
		WHERE %s
		ORDER BY %s %s, ai.run_id DESC
		LIMIT %d OFFSET %d
	`, aiRunsSubquery(), where, orderExpr, direction, limit, offset)

	var rows []AIRunRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, 0, err
	}

	countQuery := fmt.Sprintf(`SELECT count() FROM %s ai WHERE %s`, aiRunsSubquery(), where)
	var total uint64
	if err := r.db.QueryRow(ctx, &total, countQuery, args...); err != nil {
		return nil, 0, err
	}

	return rows, total, nil
}

func (r *repository) SummarizeRuns(ctx context.Context, q queryContext) (AIOverview, error) {
	where, args := buildAIWhereClause(q)
	query := fmt.Sprintf(`
		SELECT
			count() AS total_runs,
			countIf(ai.has_error) AS error_runs,
			avg(ai.latency_ms) AS avg_latency_ms,
			quantile(0.95)(ai.latency_ms) AS p95_latency_ms,
			avg(ai.ttft_ms) AS avg_ttft_ms,
			sum(ai.total_tokens) AS total_tokens,
			sum(ai.cost_usd) AS total_cost_usd,
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score,
			uniqIf(ai.provider, ai.provider != '') AS provider_count,
			uniqIf(ai.request_model, ai.request_model != '') AS model_count,
			uniqIf(ai.prompt_template, ai.prompt_template != '') AS prompt_count,
			countIf(lower(ai.guardrail_state) IN ('blocked', 'rejected', 'failed')) AS guardrail_blocked
		FROM %s ai
		WHERE %s
	`, aiRunsSubquery(), where)

	var summary AIOverview
	if err := r.db.QueryRow(ctx, &summary, query, args...); err != nil {
		return AIOverview{}, err
	}
	summary.ErrorRatePct = ratioPct(summary.ErrorRuns, summary.TotalRuns)
	return summary, nil
}

func (r *repository) TrendRuns(ctx context.Context, q queryContext, step string) ([]AITrendPoint, error) {
	where, args := buildAIWhereClause(q)
	bucket := utils.ByName(step).GetRawExpression("ai.start_time")
	query := fmt.Sprintf(`
		SELECT
			%s AS time_bucket,
			count() AS requests,
			countIf(ai.has_error) AS error_runs,
			quantile(0.95)(ai.latency_ms) AS p95_latency_ms,
			avg(ai.ttft_ms) AS avg_ttft_ms,
			sum(ai.total_tokens) AS total_tokens,
			sum(ai.cost_usd) AS total_cost_usd,
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score,
			countIf(lower(ai.guardrail_state) IN ('blocked', 'rejected', 'failed')) AS guardrail_blocked
		FROM %s ai
		WHERE %s
		GROUP BY time_bucket
		ORDER BY time_bucket ASC
	`, bucket, aiRunsSubquery(), where)

	var rows []AITrendPoint
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	for idx := range rows {
		rows[idx].ErrorRatePct = ratioPct(rows[idx].ErrorRuns, rows[idx].Requests)
	}
	return rows, nil
}

func (r *repository) FacetRuns(ctx context.Context, q queryContext) (AIExplorerFacets, error) {
	where, args := buildAIWhereClause(q)
	source := aiRunsSubquery()
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
	for _, definition := range definitions {
		query := fmt.Sprintf(`
			SELECT %s AS value, count() AS count
			FROM %s ai
			WHERE %s AND %s != ''
			GROUP BY value
			ORDER BY count DESC
			LIMIT %d
		`, definition.expr, source, where, definition.expr, defaultBreakdownLimit)

		var buckets []FacetBucket
		if err := r.db.Select(ctx, &buckets, query, args...); err != nil {
			return AIExplorerFacets{}, err
		}

		switch definition.key {
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
		Table:      aiRunsSubquery(),
		TableAlias: "ai",
		TimeBucketFunc: func(startMs, endMs int64, step string) string {
			return utils.ByName(step).GetRawExpression("ai.start_time")
		},
		BaseWhereFunc: func(_ int64, startMs, endMs int64) (string, []any) {
			return buildAIWhereClause(queryContext{teamID: teamID, start: startMs, end: endMs})
		},
		Dimensions: map[string]string{
			"provider":        "ai.provider",
			"service":         "ai.service_name",
			"service_name":    "ai.service_name",
			"model":           "ai.request_model",
			"request_model":   "ai.request_model",
			"operation":       "ai.operation",
			"prompt_template": "ai.prompt_template",
			"session_id":      "ai.session_id",
			"conversation_id": "ai.conversation_id",
			"finish_reason":   "ai.finish_reason",
			"guardrail_state": "if(ai.guardrail_state = '', 'not_evaluated', ai.guardrail_state)",
			"quality_bucket":  "ai.quality_bucket",
			"status":          "ai.status",
		},
		AggFields: map[string]string{
			"latency_ms":    "ai.latency_ms",
			"ttft_ms":       "ai.ttft_ms",
			"input_tokens":  "ai.input_tokens",
			"output_tokens": "ai.output_tokens",
			"total_tokens":  "ai.total_tokens",
			"cost_usd":      "ai.cost_usd",
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

func (r *repository) GetRunDetail(ctx context.Context, teamID int64, traceID, spanID string) (*AIRunDetail, error) {
	query := fmt.Sprintf(`
		SELECT
			ai.run_id,
			ai.trace_id,
			ai.span_id,
			ai.service_name,
			ai.provider,
			ai.request_model,
			ai.response_model,
			ai.operation,
			ai.prompt_template,
			ai.prompt_template_version,
			ai.conversation_id,
			ai.session_id,
			ai.finish_reason,
			ai.guardrail_state,
			ai.status,
			ai.status_message,
			ai.has_error,
			ai.start_time,
			ai.latency_ms,
			ai.ttft_ms,
			ai.input_tokens,
			ai.output_tokens,
			ai.total_tokens,
			ai.cost_usd,
			ai.quality_score,
			ai.feedback_score,
			ai.quality_bucket,
			ai.span_name,
			ai.service_version,
			ai.environment,
			ai.prompt_snippet,
			ai.response_snippet,
			ai.cache_hit,
			ai.attributes
		FROM %s ai
		WHERE ai.team_id = @teamID AND ai.trace_id = @traceID AND ai.span_id = @spanID
		LIMIT 1
	`, aiRunsSubquery())

	var rows []AIRunDetail
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

func (r *repository) GetRunLogs(ctx context.Context, teamID int64, traceID, spanID string, limit int) ([]AIRelatedLog, error) {
	if limit <= 0 || limit > 200 {
		limit = defaultLogLimit
	}

	query := `
		SELECT
			timestamp,
			severity_text,
			body,
			service AS service_name,
			trace_id,
			span_id
		FROM observability.logs
		WHERE team_id = @teamID
		  AND trace_id = @traceID
		  AND (span_id = @spanID OR @spanID = '')
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

	out := make([]AIRelatedLog, 0, len(rows))
	for _, row := range rows {
		out = append(out, AIRelatedLog{
			Timestamp:    row.Timestamp,
			SeverityText: row.SeverityText,
			Body:         row.Body,
			ServiceName:  row.ServiceName,
			TraceID:      row.TraceID,
			SpanID:       row.SpanID,
		})
	}
	return out, nil
}

func (r *repository) GetMatchingAlerts(ctx context.Context, teamID int64, detail *AIRunDetail) ([]AIAlertTargetRef, error) {
	if r.sql == nil || detail == nil {
		return nil, nil
	}

	rows, err := r.sql.QueryContext(ctx, `
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

func buildAIWhereClause(q queryContext) (string, []any) {
	where := "ai.team_id = @teamID AND ai.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ai.start_time BETWEEN @start AND @end"
	args := []any{
		clickhouse.Named("teamID", uint32(q.teamID)),
		clickhouse.Named("bucketStart", utils.SpansBucketStart(q.start/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(q.end/1000)),
		clickhouse.Named("start", time.UnixMilli(q.start)),
		clickhouse.Named("end", time.UnixMilli(q.end)),
	}
	if strings.TrimSpace(q.where) != "" {
		where += " AND " + q.where
		args = append(args, q.args...)
	}
	return where, args
}

func resolveOrderBy(orderBy string) string {
	switch strings.ToLower(strings.TrimSpace(orderBy)) {
	case "latency_ms":
		return "ai.latency_ms"
	case "ttft_ms":
		return "ai.ttft_ms"
	case "cost_usd":
		return "ai.cost_usd"
	case "quality_score":
		return "ai.quality_score"
	case "total_tokens":
		return "ai.total_tokens"
	case "start_time":
		return "ai.start_time"
	default:
		return "ai.start_time"
	}
}

func ratioPct(errorRuns, totalRuns uint64) float64 {
	if totalRuns == 0 {
		return 0
	}
	return (float64(errorRuns) / float64(totalRuns)) * 100
}

func aiRunsSubquery() string {
	return fmt.Sprintf(`(
		SELECT
			s.team_id AS team_id,
			s.ts_bucket_start AS ts_bucket_start,
			s.timestamp AS start_time,
			s.trace_id AS trace_id,
			s.span_id AS span_id,
			concat(s.trace_id, ':', s.span_id) AS run_id,
			s.service_name AS service_name,
			s.name AS span_name,
			if(s.status_code_string = '', if(s.has_error, 'ERROR', 'UNSET'), s.status_code_string) AS status,
			s.status_message AS status_message,
			s.has_error AS has_error,
			s.duration_nano / 1000000.0 AS latency_ms,
			%s AS provider,
			%s AS request_model,
			if(%s = '', %s, %s) AS response_model,
			if(%s = '', s.name, %s) AS operation,
			%s AS prompt_template,
			%s AS prompt_template_version,
			%s AS conversation_id,
			%s AS session_id,
			%s AS finish_reason,
			%s AS guardrail_state,
			%s AS prompt_snippet,
			%s AS response_snippet,
			%s AS service_version,
			%s AS environment,
			%s AS input_tokens,
			%s AS output_tokens,
			multiIf(%s, %s, (%s + %s) > 0, (%s + %s), 0.0) AS total_tokens,
			%s AS cost_usd,
			%s AS ttft_ms,
			%s AS quality_score,
			%s AS feedback_score,
			%s AS cache_hit,
			%s AS quality_bucket,
			s.attributes AS attributes
		FROM observability.spans s
		WHERE %s
	)`,
		stringAttrExpr("s", providerAttributeKeys...),
		stringAttrExpr("s", requestModelAttributeKeys...),
		stringAttrExpr("s", responseModelAttributeKeys...),
		stringAttrExpr("s", requestModelAttributeKeys...),
		stringAttrExpr("s", responseModelAttributeKeys...),
		stringAttrExpr("s", operationAttributeKeys...),
		stringAttrExpr("s", operationAttributeKeys...),
		stringAttrExpr("s", promptTemplateAttributeKeys...),
		stringAttrExpr("s", promptTemplateVersionAttributeKeys...),
		stringAttrExpr("s", conversationAttributeKeys...),
		stringAttrExpr("s", sessionAttributeKeys...),
		stringAttrExpr("s", finishReasonAttributeKeys...),
		stringAttrExpr("s", guardrailAttributeKeys...),
		stringAttrExpr("s", promptSnippetAttributeKeys...),
		stringAttrExpr("s", responseSnippetAttributeKeys...),
		stringAttrExpr("s", serviceVersionAttributeKeys...),
		stringAttrExpr("s", environmentAttributeKeys...),
		numberAttrExpr("s", inputTokensAttributeKeys...),
		numberAttrExpr("s", outputTokensAttributeKeys...),
		mapContainsExpr("s", totalTokensAttributeKeys...),
		numberAttrExpr("s", totalTokensAttributeKeys...),
		numberAttrExpr("s", inputTokensAttributeKeys...),
		numberAttrExpr("s", outputTokensAttributeKeys...),
		numberAttrExpr("s", inputTokensAttributeKeys...),
		numberAttrExpr("s", outputTokensAttributeKeys...),
		numberAttrExpr("s", costAttributeKeys...),
		numberAttrExpr("s", ttftAttributeKeys...),
		numberAttrExpr("s", qualityScoreAttributeKeys...),
		numberAttrExpr("s", feedbackScoreAttributeKeys...),
		boolAttrExpr("s", cacheHitAttributeKeys...),
		qualityBucketExpr(numberAttrExpr("s", qualityScoreAttributeKeys...)),
		aiDetectionExpr("s"),
	)
}

func aiDetectionExpr(alias string) string {
	parts := make([]string, 0, len(aiDetectionAttributeKeys))
	for _, key := range aiDetectionAttributeKeys {
		parts = append(parts, fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key))
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func stringAttrExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "''"
	}
	parts := make([]string, 0, (len(keys)*2)+1)
	for _, key := range keys {
		parts = append(parts,
			fmt.Sprintf("mapGet(%s.attributes, '%s') != ''", alias, key),
			fmt.Sprintf("mapGet(%s.attributes, '%s')", alias, key),
		)
	}
	parts = append(parts, "''")
	return "multiIf(" + strings.Join(parts, ", ") + ")"
}

func numberAttrExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "0.0"
	}
	parts := make([]string, 0, (len(keys)*2)+1)
	for _, key := range keys {
		parts = append(parts,
			fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key),
			fmt.Sprintf("toFloat64OrZero(mapGet(%s.attributes, '%s'))", alias, key),
		)
	}
	parts = append(parts, "0.0")
	return "multiIf(" + strings.Join(parts, ", ") + ")"
}

func boolAttrExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "false"
	}
	parts := make([]string, 0, (len(keys)*2)+1)
	for _, key := range keys {
		parts = append(parts,
			fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key),
			fmt.Sprintf("lower(mapGet(%s.attributes, '%s')) IN ('1', 'true', 'yes')", alias, key),
		)
	}
	parts = append(parts, "false")
	return "multiIf(" + strings.Join(parts, ", ") + ")"
}

func mapContainsExpr(alias string, keys ...string) string {
	if len(keys) == 0 {
		return "false"
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key))
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func qualityBucketExpr(scoreExpr string) string {
	return fmt.Sprintf(
		"multiIf(%s >= 0.9, 'excellent', %s >= 0.75, 'good', %s > 0, 'review', 'unscored')",
		scoreExpr,
		scoreExpr,
		scoreExpr,
	)
}

func targetMatchesRun(target map[string]any, detail *AIRunDetail) bool {
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
