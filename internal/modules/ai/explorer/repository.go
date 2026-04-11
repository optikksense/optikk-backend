package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// genAIBaseFilter ensures we only query spans with a non-empty gen_ai.system attribute.
const genAIBaseFilter = `JSONExtractString(s.attributes, 'gen_ai.system') != ''`

// Repository defines the data-access interface for AI explorer queries.
type Repository interface {
	GetAICalls(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, limit, offset int) ([]aiCallRow, uint64, error)
	GetAISummary(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter) (aiSummaryRow, error)
	GetAIFacets(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter) ([]aiFacetRow, error)
	GetAITrend(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, step string) ([]aiTrendRow, error)
}

// attrFilter represents a single custom attribute filter extracted from the query string.
type attrFilter struct {
	Key   string
	Value string
	Op    string // "eq", "neq", "contains", "exists"
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
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
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAICalls: %w", err)
	}

	countQuery := fmt.Sprintf(`SELECT count() FROM observability.spans s %s`, where)
	var total uint64
	if err := r.db.QueryRow(ctx, &total, countQuery, args[:len(args)-2]...); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAICalls.count: %w", err)
	}

	return rows, total, nil
}

// GetAISummary returns aggregated statistics for the query window.
func (r *ClickHouseRepository) GetAISummary(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter) (aiSummaryRow, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	query := fmt.Sprintf(`
		SELECT
			count() AS total_calls,
			countIf(s.status_code_string = 'ERROR' OR s.has_error = true) AS error_calls,
			avg(s.duration_nano / 1000000.0) AS avg_latency_ms,
			quantile(0.50)(s.duration_nano / 1000000.0) AS p50_latency_ms,
			quantile(0.95)(s.duration_nano / 1000000.0) AS p95_latency_ms,
			quantile(0.99)(s.duration_nano / 1000000.0) AS p99_latency_ms,
			sum(toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.input_tokens'))) AS total_input_tokens,
			sum(toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.output_tokens'))) AS total_output_tokens
		FROM observability.spans s
		%s`, where)

	var row aiSummaryRow
	if err := r.db.QueryRow(ctx, &row, query, args...); err != nil {
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
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("ai.GetAIFacets: %w", err)
	}
	return rows, nil
}

// GetAITrend returns time-bucketed trend data.
func (r *ClickHouseRepository) GetAITrend(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, step string) ([]aiTrendRow, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	bucketExpr := timebucket.Expression(startMs, endMs)
	if step != "" {
		bucketExpr = timebucket.ByName(step).GetBucketExpression()
	}

	query := fmt.Sprintf(`
		SELECT
			%s AS time_bucket,
			count() AS total_calls,
			countIf(s.status_code_string = 'ERROR' OR s.has_error = true) AS error_calls,
			avg(s.duration_nano / 1000000.0) AS avg_latency_ms,
			sum(toFloat64OrZero(JSONExtractString(s.attributes, 'gen_ai.usage.total_tokens'))) AS total_tokens
		FROM observability.spans s
		%s
		GROUP BY time_bucket
		ORDER BY time_bucket`, bucketExpr, where)

	var rows []aiTrendRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("ai.GetAITrend: %w", err)
	}
	return rows, nil
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
