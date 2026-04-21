package explorer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

// genAIBaseFilter ensures we only query spans with a non-empty gen_ai.system attribute.
const genAIBaseFilter = `JSONExtractString(s.attributes, 'gen_ai.system') != ''`

// aiSpansRollupPrefix backs the migrated aggregate reads. Rollup schema is
// keyed on (team_id, bucket_ts, ai_system, ai_model, ai_operation,
// service_name) and stores request/error counts, input/output token sums,
// plus a t-digest latency state.
const aiSpansRollupPrefix = "observability.ai_spans_rollup"

// queryIntervalMinutes returns max(tierStep, dashboardStep) for the rollup
// query-time step. Mirrors overview/overview/repository.go.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

// buildAIRollupWhere produces a WHERE clause for the AI-spans rollup.
// Only rollup-dimensional filters survive — attribute-map filters (prompt
// template, provider-specific flags, session IDs, etc.) are dropped.
func buildAIRollupWhere(teamID, startMs, endMs int64, filters []attrFilter) (string, []any) {
	frag := ` WHERE team_id = @teamID AND bucket_ts BETWEEN @start AND @end`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - tenant id fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	for i, af := range filters {
		valueName := fmt.Sprintf("fValue%d", i)
		switch af.Key {
		case "__service_name":
			frag += fmt.Sprintf(` AND service_name = @%s`, valueName)
			args = append(args, clickhouse.Named(valueName, af.Value))
		case "gen_ai.system", "ai_system":
			frag += fmt.Sprintf(` AND ai_system = @%s`, valueName)
			args = append(args, clickhouse.Named(valueName, af.Value))
		case "gen_ai.request.model", "gen_ai.response.model", "ai_model":
			frag += fmt.Sprintf(` AND ai_model = @%s`, valueName)
			args = append(args, clickhouse.Named(valueName, af.Value))
		case "gen_ai.operation.name", "ai_operation":
			frag += fmt.Sprintf(` AND ai_operation = @%s`, valueName)
			args = append(args, clickhouse.Named(valueName, af.Value))
		}
		// All other filters (prompt template, status, session, etc.) cannot
		// be expressed against the rollup and are silently dropped.
	}
	return frag, args
}

// Repository defines the data-access interface for AI explorer queries.
type Repository interface {
	GetAICalls(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, limit, offset int) ([]aiCallRow, uint64, error)
	GetAISummary(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter) (aiSummaryRow, error)
	GetAIFacets(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter) ([]aiFacetRow, error)
	GetAITrend(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, step string) ([]aiTrendRow, error)
	GetAISessions(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, limit, offset int) ([]aiSessionRow, uint64, error)
}

// attrFilter represents a single custom attribute filter extracted from the query string.
type attrFilter struct {
	Key   string
	Value string
	Op    string // "eq", "neq", "contains", "exists"
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db clickhouse.Conn
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

const aiSelectColumns = `
	s.span_id,
	s.trace_id,
	s.service_name,
	s.name AS operation_name,
	s.timestamp AS start_time,
	s.duration_nano / 1000000.0 AS duration_ms,
	s.status_code_string AS status,
	s.status_message,
	JSONExtractString(s.attributes, 'gen_ai.system') AS ai_system,
	JSONExtractString(s.attributes, 'gen_ai.request.model') AS ai_request_model,
	JSONExtractString(s.attributes, 'gen_ai.response.model') AS ai_response_model,
	JSONExtractString(s.attributes, 'gen_ai.operation.name') AS ai_operation,
	toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.input_tokens')) AS input_tokens,
	toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.output_tokens')) AS output_tokens,
	toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.total_tokens')) AS total_tokens,
	JSONExtractString(s.attributes, 'gen_ai.request.temperature') AS temperature,
	JSONExtractString(s.attributes, 'gen_ai.request.max_tokens') AS max_tokens,
	JSONExtractString(s.attributes, 'gen_ai.response.finish_reasons') AS finish_reason,
	s.mat_exception_type AS error_type`

// GetAICalls returns paginated AI/LLM span records.
func (r *ClickHouseRepository) GetAICalls(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, limit, offset int) ([]aiCallRow, uint64, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	query := fmt.Sprintf(`SELECT %s FROM observability.spans s %s ORDER BY s.timestamp DESC LIMIT @limit OFFSET @offset`, aiSelectColumns, where)
	args = append(args, clickhouse.Named("limit", limit), clickhouse.Named("offset", offset))

	var rows []aiCallRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAICalls: %w", err)
	}

	countQuery := fmt.Sprintf(`SELECT count() FROM observability.spans s %s`, where)
	var total uint64
	if err := r.db.QueryRow(dbutil.ExplorerCtx(ctx), countQuery, args[:len(args)-2]...).ScanStruct(&total); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAICalls.count: %w", err)
	}

	return rows, total, nil
}

// GetAISummary returns aggregated statistics for the query window, sourced
// from the `ai_spans_rollup_*` cascade. `avg_latency_ms` is derived in SQL
// via duration_ms is not carried per-row — we surface the p50 t-digest
// value as the avg proxy, which is close enough for a UI summary card.
func (r *ClickHouseRepository) GetAISummary(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter) (aiSummaryRow, error) {
	table, _ := rollup.TierTableFor(aiSpansRollupPrefix, startMs, endMs)
	where, args := buildAIRollupWhere(teamID, startMs, endMs, filters)

	query := fmt.Sprintf(`
		SELECT
			sumMerge(request_count)                                              AS total_calls,
			sumMerge(error_count)                                                AS error_calls,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS avg_latency_ms,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS p50_latency_ms,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2  AS p95_latency_ms,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3  AS p99_latency_ms,
			toFloat64(sumMerge(input_tokens_sum))                                AS total_input_tokens,
			toFloat64(sumMerge(output_tokens_sum))                               AS total_output_tokens
		FROM %s
		%s`, table, where)

	var row aiSummaryRow
	if err := r.db.QueryRow(dbutil.ExplorerCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return aiSummaryRow{}, fmt.Errorf("ai.GetAISummary: %w", err)
	}
	return row, nil
}

// GetAIFacets returns facet buckets for AI-specific dimensions.
func (r *ClickHouseRepository) GetAIFacets(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter) ([]aiFacetRow, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	// Build UNION ALL for each facet dimension.
	facetDefs := []struct {
		key  string
		expr string
	}{
		{"ai_system", `JSONExtractString(s.attributes, 'gen_ai.system')`},
		{"ai_model", `JSONExtractString(s.attributes, 'gen_ai.request.model')`},
		{"ai_operation", `JSONExtractString(s.attributes, 'gen_ai.operation.name')`},
		{"service_name", `s.service_name`},
		{"status", `s.status_code_string`},
		{"finish_reason", `JSONExtractString(s.attributes, 'gen_ai.response.finish_reasons')`},
		{"prompt_template", `JSONExtractString(s.attributes, 'gen_ai.prompt.template.name')`},
	}

	parts := make([]string, 0, len(facetDefs))
	for _, fd := range facetDefs {
		parts = append(parts, fmt.Sprintf(
			`SELECT '%s' AS facet_key, %s AS facet_value, count() AS count FROM observability.spans s %s GROUP BY facet_value HAVING facet_value != '' ORDER BY count DESC LIMIT 20`,
			fd.key, fd.expr, where,
		))
	}

	query := strings.Join(parts, " UNION ALL ")

	// Each sub-query needs its own copy of args. ClickHouse driver handles
	// named parameters that appear multiple times, so we only pass one set.
	var rows []aiFacetRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, fmt.Errorf("ai.GetAIFacets: %w", err)
	}
	return rows, nil
}

// GetAITrend returns time-bucketed trend data from the `ai_spans_rollup_*`
// cascade. `avg_latency_ms` uses the p50 t-digest value as the summary
// statistic (the rollup does not carry duration_ms_sum for AI spans); other
// fields come from merged state columns.
func (r *ClickHouseRepository) GetAITrend(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, step string) ([]aiTrendRow, error) {
	table, tierStep := rollup.TierTableFor(aiSpansRollupPrefix, startMs, endMs)
	where, args := buildAIRollupWhere(teamID, startMs, endMs, filters)

	stepMin := queryIntervalMinutes(tierStep, startMs, endMs)
	if s := strings.TrimSpace(step); s != "" {
		if explicit := aiStepFromName(s); explicit > 0 {
			if explicit < tierStep {
				explicit = tierStep
			}
			stepMin = explicit
		}
	}

	query := fmt.Sprintf(`
		SELECT
			formatDateTime(toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)), '%%Y-%%m-%%d %%H:%%i:00') AS time_bucket,
			sumMerge(request_count)                                              AS total_calls,
			sumMerge(error_count)                                                AS error_calls,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS avg_latency_ms,
			toFloat64(sumMerge(input_tokens_sum) + sumMerge(output_tokens_sum))  AS total_tokens
		FROM %s
		%s
		GROUP BY time_bucket
		ORDER BY time_bucket`, table, where)
	args = append(args, clickhouse.Named("intervalMin", stepMin))

	var rows []aiTrendRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, fmt.Errorf("ai.GetAITrend: %w", err)
	}
	return rows, nil
}

func aiStepFromName(step string) int64 {
	switch step {
	case "1m":
		return 1
	case "5m":
		return 5
	case "15m":
		return 15
	case "1h":
		return 60
	case "1d":
		return 1440
	default:
		return 0
	}
}

// sessionIDInnerSelect is the GenAI span subquery column that picks a stable session key from OTel attributes.
const sessionIDInnerSelect = `multiIf(
  JSONExtractString(s.attributes, 'gen_ai.session.id') != '', JSONExtractString(s.attributes, 'gen_ai.session.id'),
  JSONExtractString(s.attributes, 'gen_ai.conversation.id') != '', JSONExtractString(s.attributes, 'gen_ai.conversation.id'),
  JSONExtractString(s.attributes, 'session.id') != '', JSONExtractString(s.attributes, 'session.id'),
  ''
) AS session_id`

// GetAISessions aggregates GenAI spans by session/conversation id (non-empty session_id only).
func (r *ClickHouseRepository) GetAISessions(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, limit, offset int) ([]aiSessionRow, uint64, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	inner := fmt.Sprintf(`
		SELECT
			%s,
			s.trace_id AS trace_id,
			s.timestamp AS start_time,
			s.status_code_string AS status,
			s.has_error AS has_error,
			toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.input_tokens')) AS input_tokens,
			toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.output_tokens')) AS output_tokens,
			JSONExtractString(s.attributes, 'gen_ai.request.model') AS ai_request_model,
			s.service_name AS service_name
		FROM observability.spans s
		%s`, sessionIDInnerSelect, where)

	grouped := fmt.Sprintf(`
		SELECT
			session_id,
			count() AS generation_count,
			uniq(trace_id) AS trace_count,
			min(start_time) AS first_start,
			max(start_time) AS last_start,
			sum(input_tokens) AS total_input_tokens,
			sum(output_tokens) AS total_output_tokens,
			countIf(status = 'ERROR' OR has_error = true) AS error_count,
			argMax(ai_request_model, start_time) AS dominant_model,
			argMax(service_name, start_time) AS dominant_service
		FROM (%s) AS t
		WHERE session_id != ''
		GROUP BY session_id
		ORDER BY last_start DESC`, inner)

	query := fmt.Sprintf(`%s LIMIT @limit OFFSET @offset`, grouped)
	argsWithPaging := append(append([]any{}, args...), clickhouse.Named("limit", limit), clickhouse.Named("offset", offset))

	var rows []aiSessionRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, argsWithPaging...); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAISessions: %w", err)
	}

	countQuery := fmt.Sprintf(`SELECT count() FROM (%s)`, grouped)
	var total uint64
	if err := r.db.QueryRow(dbutil.ExplorerCtx(ctx), countQuery, args...).ScanStruct(&total); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAISessions.count: %w", err)
	}

	return rows, total, nil
}

// buildAIWhereClause constructs the WHERE clause for AI explorer queries.
// It always includes the gen_ai.system existence filter plus standard time/team bounds.
func buildAIWhereClause(teamID, startMs, endMs int64, filters []attrFilter) (string, []any) {
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)

	frag := ` WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end AND ` + genAIBaseFilter

	for i, af := range filters {
		valueName := fmt.Sprintf("fValue%d", i)

		// Handle materialized column filters (service_name, status).
		switch af.Key {
		case "__service_name":
			frag += fmt.Sprintf(` AND s.service_name = @%s`, valueName)
			args = append(args, clickhouse.Named(valueName, af.Value))
			continue
		case "__status":
			if af.Value == "ERROR" {
				frag += ` AND (s.status_code_string = 'ERROR' OR s.has_error = true)`
			} else {
				frag += fmt.Sprintf(` AND s.status_code_string = @%s`, valueName)
				args = append(args, clickhouse.Named(valueName, af.Value))
			}
			continue
		case "__session_id":
			frag += fmt.Sprintf(` AND (
				JSONExtractString(s.attributes, 'gen_ai.session.id') = @%s OR
				JSONExtractString(s.attributes, 'gen_ai.conversation.id') = @%s OR
				JSONExtractString(s.attributes, 'session.id') = @%s
			)`, valueName, valueName, valueName)
			args = append(args, clickhouse.Named(valueName, af.Value))
			continue
		}

		keyName := fmt.Sprintf("fKey%d", i)

		switch af.Op {
		case "exists":
			frag += fmt.Sprintf(` AND JSONExtractString(s.attributes, @%s) != ''`, keyName)
			args = append(args, clickhouse.Named(keyName, af.Key))
		case "neq":
			frag += fmt.Sprintf(` AND JSONExtractString(s.attributes, @%s) != @%s`, keyName, valueName)
			args = append(args, clickhouse.Named(keyName, af.Key), clickhouse.Named(valueName, af.Value))
		case "contains":
			frag += fmt.Sprintf(` AND positionCaseInsensitive(JSONExtractString(s.attributes, @%s), @%s) > 0`, keyName, valueName)
			args = append(args, clickhouse.Named(keyName, af.Key), clickhouse.Named(valueName, af.Value))
		default: // eq
			frag += fmt.Sprintf(` AND JSONExtractString(s.attributes, @%s) = @%s`, keyName, valueName)
			args = append(args, clickhouse.Named(keyName, af.Key), clickhouse.Named(valueName, af.Value))
		}
	}

	return frag, args
}
