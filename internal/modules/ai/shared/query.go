package shared

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

// BuildWhereClause returns the WHERE fragment and named args for team/time
// filtering, optionally appending the caller's extra predicates.
func BuildWhereClause(q QueryContext) (string, []any) {
	where := "ai.team_id = @teamID AND ai.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ai.start_time BETWEEN @start AND @end"
	args := []any{
		clickhouse.Named("teamID", uint32(q.TeamID)),
		clickhouse.Named("bucketStart", utils.SpansBucketStart(q.Start/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(q.End/1000)),
		clickhouse.Named("start", time.UnixMilli(q.Start)),
		clickhouse.Named("end", time.UnixMilli(q.End)),
	}
	if strings.TrimSpace(q.Where) != "" {
		where += " AND " + q.Where
		args = append(args, q.Args...)
	}
	return where, args
}

// CompileAIQuery parses a user query string into a WHERE clause and args
// using the AI query schema.
func CompileAIQuery(raw string) (string, []any, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil, nil
	}
	node, err := queryparser.Parse(raw)
	if err != nil {
		return "", nil, fmt.Errorf("invalid query: %w", err)
	}
	if node == nil {
		return "", nil, nil
	}
	compiled, err := queryparser.Compile(node, queryparser.AISchema{})
	if err != nil {
		return "", nil, fmt.Errorf("query compilation error: %w", err)
	}
	return compiled.Where, compiled.Args, nil
}

// ResolveOrderBy maps a user-facing order column name to the ClickHouse expression.
func ResolveOrderBy(orderBy string) string {
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

// ResolveTimeBucket returns the ClickHouse time-bucketing expression for the
// given step size applied to ai.start_time.
func ResolveTimeBucket(step string) string {
	return utils.ByName(step).GetRawExpression("ai.start_time")
}

// ParseRunID splits "traceID:spanID" into its parts.
func ParseRunID(runID string) (string, string, error) {
	parts := strings.SplitN(runID, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid run id")
	}
	return parts[0], parts[1], nil
}

// SummarizeRuns returns an AIOverview aggregate for the given query context.
func SummarizeRuns(ctx context.Context, db *dbutil.NativeQuerier, q QueryContext) (AIOverview, error) {
	where, args := BuildWhereClause(q)
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
	`, AIRunsSubquery(), where)

	var summary AIOverview
	if err := db.QueryRow(ctx, &summary, query, args...); err != nil {
		return AIOverview{}, err
	}
	summary.ErrorRatePct = RatioPct(summary.ErrorRuns, summary.TotalRuns)
	return summary, nil
}

// TrendRuns returns time-bucketed trend data for the given query context.
func TrendRuns(ctx context.Context, db *dbutil.NativeQuerier, q QueryContext, step string) ([]AITrendPoint, error) {
	where, args := BuildWhereClause(q)
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
	`, bucket, AIRunsSubquery(), where)

	var rows []AITrendPoint
	if err := db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	for idx := range rows {
		rows[idx].ErrorRatePct = RatioPct(rows[idx].ErrorRuns, rows[idx].Requests)
	}
	return rows, nil
}
