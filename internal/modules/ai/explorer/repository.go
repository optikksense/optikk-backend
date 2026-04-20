package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
)

// genAIBaseFilter ensures we only query spans with a non-empty gen_ai.system attribute.
const genAIBaseFilter = `JSONExtractString(s.attributes, 'gen_ai.system') != ''`

// errorPredicate identifies GenAI error spans. Lives in the base WHERE on the
// error-leg scans so no combinator is ever needed in SELECT.
const errorPredicate = `(s.status_code_string = 'ERROR' OR s.has_error = true)`

// Repository defines the data-access interface for AI explorer queries.
type Repository interface {
	GetAICalls(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, limit, offset int) ([]aiCallRow, uint64, error)
	GetAISummary(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter) (aiSummaryRow, error)
	GetAIFacets(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter) ([]aiFacetRow, error)
	GetAITrend(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, step string) ([]aiTrendRow, error)
	GetAISessions(ctx context.Context, teamID, startMs, endMs int64, attrFilters []attrFilter, limit, offset int) ([]aiSessionRawRow, error)
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

// aiSelectColumns projects the row-level GenAI span columns. Token fields
// come back as raw strings; the service parses them to float64 Go-side so
// no float cast is needed in SQL.
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
	JSONExtractString(s.attributes, 'gen_ai.usage.input_tokens') AS input_tokens_raw,
	JSONExtractString(s.attributes, 'gen_ai.usage.output_tokens') AS output_tokens_raw,
	JSONExtractString(s.attributes, 'gen_ai.usage.total_tokens') AS total_tokens_raw,
	JSONExtractString(s.attributes, 'gen_ai.request.temperature') AS temperature,
	JSONExtractString(s.attributes, 'gen_ai.request.max_tokens') AS max_tokens,
	JSONExtractString(s.attributes, 'gen_ai.response.finish_reasons') AS finish_reason,
	s.mat_exception_type AS error_type`

// GetAICalls returns paginated AI/LLM span records.
func (r *ClickHouseRepository) GetAICalls(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, limit, offset int) ([]aiCallRow, uint64, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	query := fmt.Sprintf(`SELECT %s FROM observability.spans s %s ORDER BY s.timestamp DESC LIMIT @limit OFFSET @offset`, aiSelectColumns, where)
	pagedArgs := append(append([]any{}, args...), clickhouse.Named("limit", limit), clickhouse.Named("offset", offset))

	var rows []aiCallRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, pagedArgs...); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAICalls: %w", err)
	}

	countQuery := fmt.Sprintf(`SELECT count() AS total FROM observability.spans s %s`, where)
	var cnt aiCountRow
	if err := r.db.QueryRow(dbutil.ExplorerCtx(ctx), countQuery, args...).ScanStruct(&cnt); err != nil {
		return nil, 0, fmt.Errorf("ai.GetAICalls.count: %w", err)
	}

	return rows, cnt.Total, nil
}

// GetAISummary returns aggregated statistics for the query window.
//
// Percentiles come from sketch.Querier (SpanLatencyService) — see service.go.
// Combinators (conditional aggregators / distinct-array builders) are
// replaced with three narrow-WHERE scans run in parallel: totals
// (count/latency-sum/token-sums), errors (error-only count), and services
// (distinct service_name list).
//
// avg_latency_ms is computed Go-side from sum / count (no SQL-side average).
func (r *ClickHouseRepository) GetAISummary(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter) (aiSummaryRow, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	var (
		totals   aiSummaryTotalsRow
		errCount aiCountRow
		svcRows  []aiServiceRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT
				count() AS total_calls,
				sum(s.duration_nano / 1000000.0) AS latency_ms_sum,
				sum(JSONExtractFloat(s.attributes, 'gen_ai.usage.input_tokens')) AS total_input_tokens,
				sum(JSONExtractFloat(s.attributes, 'gen_ai.usage.output_tokens')) AS total_output_tokens
			FROM observability.spans s
			%s`, where)
		return r.db.QueryRow(dbutil.ExplorerCtx(gctx), q, args...).ScanStruct(&totals)
	})

	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT count() AS total
			FROM observability.spans s
			%s AND `+errorPredicate, where)
		return r.db.QueryRow(dbutil.ExplorerCtx(gctx), q, args...).ScanStruct(&errCount)
	})

	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT s.service_name AS service_name
			FROM observability.spans s
			%s
			GROUP BY s.service_name`, where)
		return r.db.Select(dbutil.ExplorerCtx(gctx), &svcRows, q, args...)
	})

	if err := g.Wait(); err != nil {
		return aiSummaryRow{}, fmt.Errorf("ai.GetAISummary: %w", err)
	}

	services := make([]string, 0, len(svcRows))
	for _, s := range svcRows {
		if s.ServiceName != "" {
			services = append(services, s.ServiceName)
		}
	}

	return aiSummaryRow{
		TotalCalls:        totals.TotalCalls,
		ErrorCalls:        errCount.Total,
		LatencyMsSum:      totals.LatencyMsSum,
		LatencyMsCount:    totals.TotalCalls,
		Services:          services,
		TotalInputTokens:  totals.TotalInputTokens,
		TotalOutputTokens: totals.TotalOutputTokens,
	}, nil
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

// GetAITrend returns time-bucketed trend data.
//
// Totals + errors are fetched via two parallel narrow-WHERE scans and merged
// per-bucket Go-side. avg_latency_ms is computed Go-side from sum/count.
func (r *ClickHouseRepository) GetAITrend(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, step string) ([]aiTrendRow, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	bucketExpr := timebucket.Expression(startMs, endMs)
	if step != "" {
		bucketExpr = timebucket.ByName(step).GetBucketExpression()
	}

	var (
		totals []aiTrendTotalsRow
		errs   []aiTrendErrorRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT
				%s AS time_bucket,
				count() AS total_calls,
				sum(s.duration_nano / 1000000.0) AS latency_ms_sum,
				sum(JSONExtractFloat(s.attributes, 'gen_ai.usage.total_tokens')) AS total_tokens
			FROM observability.spans s
			%s
			GROUP BY time_bucket
			ORDER BY time_bucket`, bucketExpr, where)
		return r.db.Select(dbutil.ExplorerCtx(gctx), &totals, q, args...)
	})
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT
				%s AS time_bucket,
				count() AS error_calls
			FROM observability.spans s
			%s AND `+errorPredicate+`
			GROUP BY time_bucket`, bucketExpr, where)
		return r.db.Select(dbutil.ExplorerCtx(gctx), &errs, q, args...)
	})
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("ai.GetAITrend: %w", err)
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.TimeBucket] = e.ErrorCalls
	}
	out := make([]aiTrendRow, 0, len(totals))
	for _, t := range totals {
		out = append(out, aiTrendRow{
			TimeBucket:     t.TimeBucket,
			TotalCalls:     t.TotalCalls,
			ErrorCalls:     errIdx[t.TimeBucket],
			LatencyMsSum:   t.LatencyMsSum,
			LatencyMsCount: t.TotalCalls,
			TotalTokens:    t.TotalTokens,
		})
	}
	return out, nil
}

// GetAISessions returns raw per-span rows scoped to GenAI spans. Session
// aggregation + pagination happens Go-side (service.go) so the CH SELECT
// stays free of branching / conditional-aggregator / argMax combinators.
//
// Note: limit/offset are applied in Go post-grouping; this repo method
// bounds the scan with the input limit ceiling to keep memory sane.
func (r *ClickHouseRepository) GetAISessions(ctx context.Context, teamID, startMs, endMs int64, filters []attrFilter, limit, offset int) ([]aiSessionRawRow, error) {
	where, args := buildAIWhereClause(teamID, startMs, endMs, filters)

	// Hard row-cap for session aggregation scans; tune if/when we hit real
	// tenants with high-volume GenAI traffic. Matches the logs/traces
	// keyset-style safety cap.
	const sessionScanCap = 100_000
	_ = limit
	_ = offset

	query := fmt.Sprintf(`
		SELECT
			JSONExtractString(s.attributes, 'gen_ai.session.id') AS session_id_primary,
			JSONExtractString(s.attributes, 'gen_ai.conversation.id') AS session_id_secondary,
			JSONExtractString(s.attributes, 'session.id') AS session_id_tertiary,
			s.trace_id AS trace_id,
			s.timestamp AS start_time,
			s.status_code_string AS status,
			s.has_error AS has_error,
			JSONExtractString(s.attributes, 'gen_ai.usage.input_tokens') AS input_tokens_raw,
			JSONExtractString(s.attributes, 'gen_ai.usage.output_tokens') AS output_tokens_raw,
			JSONExtractString(s.attributes, 'gen_ai.request.model') AS ai_request_model,
			s.service_name AS service_name
		FROM observability.spans s
		%s
		ORDER BY s.timestamp DESC
		LIMIT %d`, where, sessionScanCap)

	var rows []aiSessionRawRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, fmt.Errorf("ai.GetAISessions: %w", err)
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
