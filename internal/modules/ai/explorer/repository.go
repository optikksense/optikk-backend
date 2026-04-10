package explorer

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const genAIFilter = `(attributes.'gen_ai.system'::String != '' OR attributes.'gen_ai.request.model'::String != '')`

// Repository defines data-access for the AI explorer module.
type Repository interface {
	GetSpans(ctx context.Context, f ExplorerFilter) ([]spanRow, error)
	GetFacetValues(ctx context.Context, f ExplorerFilter, facetExpr string) ([]facetRow, error)
	GetSummary(ctx context.Context, f ExplorerFilter) (explorerSummaryRow, error)
	GetHistogram(ctx context.Context, f ExplorerFilter) ([]histogramRow, error)
}

// ClickHouseRepository implements Repository.
type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func baseArgs(f ExplorerFilter) []any {
	return []any{
		clickhouse.Named("teamID", uint32(f.TeamID)),                                //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(f.EndMs/1000)),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}
}

func optionalFilters(f ExplorerFilter) (string, []any) {
	clause := ""
	var args []any
	if f.Service != "" {
		clause += " AND s.service_name = @filterService"
		args = append(args, clickhouse.Named("filterService", f.Service))
	}
	if f.Model != "" {
		clause += " AND attributes.'gen_ai.request.model'::String = @filterModel"
		args = append(args, clickhouse.Named("filterModel", f.Model))
	}
	if f.Provider != "" {
		clause += " AND attributes.'gen_ai.system'::String = @filterProvider"
		args = append(args, clickhouse.Named("filterProvider", f.Provider))
	}
	if f.Operation != "" {
		clause += " AND attributes.'gen_ai.operation.name'::String = @filterOp"
		args = append(args, clickhouse.Named("filterOp", f.Operation))
	}
	if f.Status == "error" {
		clause += " AND s.has_error = true"
	} else if f.Status == "ok" {
		clause += " AND s.has_error = false"
	}
	if f.FinishReason != "" {
		clause += " AND attributes.'gen_ai.response.finish_reasons'::String = @filterFinish"
		args = append(args, clickhouse.Named("filterFinish", f.FinishReason))
	}
	if f.MinDurationMs > 0 {
		clause += fmt.Sprintf(" AND duration_nano / 1000000.0 >= %f", f.MinDurationMs)
	}
	if f.MaxDurationMs > 0 {
		clause += fmt.Sprintf(" AND duration_nano / 1000000.0 <= %f", f.MaxDurationMs)
	}
	if f.TraceID != "" {
		clause += " AND s.trace_id = @filterTraceID"
		args = append(args, clickhouse.Named("filterTraceID", f.TraceID))
	}
	return clause, args
}

func allArgs(f ExplorerFilter) (string, []any) {
	extra, extraArgs := optionalFilters(f)
	args := baseArgs(f)
	args = append(args, extraArgs...)
	return extra, args
}

func sortClause(f ExplorerFilter) string {
	dir := "DESC"
	if f.SortDir == "asc" {
		dir = "ASC"
	}
	switch f.Sort {
	case "duration":
		return "ORDER BY duration_ms " + dir
	case "tokens":
		return "ORDER BY (input_tokens + output_tokens) " + dir
	default:
		return "ORDER BY s.timestamp " + dir
	}
}

// ---- spans ----

func (r *ClickHouseRepository) GetSpans(ctx context.Context, f ExplorerFilter) ([]spanRow, error) {
	extra, args := allArgs(f)
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, clickhouse.Named("limit", limit), clickhouse.Named("offset", offset))
	var rows []spanRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id                                                AS span_id,
		       s.trace_id                                               AS trace_id,
		       s.parent_span_id                                         AS parent_span_id,
		       s.service_name                                           AS service_name,
		       s.name                                                   AS operation_name,
		       attributes.'gen_ai.request.model'::String                AS model,
		       attributes.'gen_ai.system'::String                       AS provider,
		       attributes.'gen_ai.operation.name'::String               AS operation_type,
		       s.timestamp                                              AS timestamp,
		       duration_nano / 1000000.0                                AS duration_ms,
		       attributes.'gen_ai.usage.input_tokens'::Int64            AS input_tokens,
		       attributes.'gen_ai.usage.output_tokens'::Int64           AS output_tokens,
		       s.has_error                                              AS has_error,
		       s.status_message                                         AS status_message,
		       attributes.'gen_ai.response.finish_reasons'::String      AS finish_reason,
		       attributes.'gen_ai.request.temperature'::Float64         AS temperature
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		`+sortClause(f)+`
		LIMIT @limit OFFSET @offset
	`, args...)
	return rows, err
}

// ---- facet values ----

func (r *ClickHouseRepository) GetFacetValues(ctx context.Context, f ExplorerFilter, facetExpr string) ([]facetRow, error) {
	extra, args := allArgs(f)
	var rows []facetRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS value, toInt64(count()) AS count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND value != ''
		GROUP BY value
		ORDER BY count DESC
		LIMIT 100
	`, facetExpr), args...)
	return rows, err
}

// ---- summary ----

func (r *ClickHouseRepository) GetSummary(ctx context.Context, f ExplorerFilter) (explorerSummaryRow, error) {
	extra, args := allArgs(f)
	var rows []explorerSummaryRow
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(count())                                            AS total_spans,
		       toInt64(countIf(has_error = true))                          AS error_count,
		       avg(duration_nano / 1000000.0)                              AS avg_latency_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0)              AS p95_ms,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64) +
		              sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS total_tokens,
		       toInt64(uniqExact(attributes.'gen_ai.request.model'::String)) AS unique_models
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra, args...)
	if err != nil || len(rows) == 0 {
		return explorerSummaryRow{}, err
	}
	return rows[0], nil
}

// ---- histogram ----

func (r *ClickHouseRepository) GetHistogram(ctx context.Context, f ExplorerFilter) ([]histogramRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []histogramRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp, toInt64(count()) AS count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}
