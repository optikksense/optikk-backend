package overview

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// genAIFilter is the WHERE fragment that scopes to AI/LLM spans.
const genAIFilter = `(attributes.'gen_ai.system'::String != '' OR attributes.'gen_ai.request.model'::String != '')`

// Repository defines the data-access contract for the AI overview module.
type Repository interface {
	GetSummary(ctx context.Context, f OverviewFilter) (summaryRow, error)
	GetModels(ctx context.Context, f OverviewFilter) ([]modelRow, error)
	GetOperations(ctx context.Context, f OverviewFilter) ([]operationRow, error)
	GetServices(ctx context.Context, f OverviewFilter) ([]serviceRow, error)
	GetModelHealth(ctx context.Context, f OverviewFilter) ([]modelHealthRow, error)
	GetTopSlow(ctx context.Context, f OverviewFilter, limit int) ([]topSlowRow, error)
	GetTopErrors(ctx context.Context, f OverviewFilter, limit int) ([]topErrorRow, error)
	GetFinishReasons(ctx context.Context, f OverviewFilter) ([]finishReasonRow, error)
	GetTimeseriesRequests(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error)
	GetTimeseriesLatency(ctx context.Context, f OverviewFilter) ([]timeseriesDualRow, error)
	GetTimeseriesErrors(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error)
	GetTimeseriesTokens(ctx context.Context, f OverviewFilter) ([]timeseriesDualRow, error)
	GetTimeseriesThroughput(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error)
	GetTimeseriesCost(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error)
}

// ClickHouseRepository implements Repository against the observability.spans table.
type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// --- helpers ---

func baseArgs(f OverviewFilter) []any {
	return []any{
		clickhouse.Named("teamID", uint32(f.TeamID)),                                //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(f.EndMs/1000)),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}
}

func optionalFilters(f OverviewFilter) (string, []any) {
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
	if f.Operation != "" {
		clause += " AND attributes.'gen_ai.operation.name'::String = @filterOp"
		args = append(args, clickhouse.Named("filterOp", f.Operation))
	}
	if f.Provider != "" {
		clause += " AND attributes.'gen_ai.system'::String = @filterProvider"
		args = append(args, clickhouse.Named("filterProvider", f.Provider))
	}
	return clause, args
}

func allArgs(f OverviewFilter) (string, []any) {
	extra, extraArgs := optionalFilters(f)
	args := baseArgs(f)
	args = append(args, extraArgs...)
	return extra, args
}

// ---- summary ----

func (r *ClickHouseRepository) GetSummary(ctx context.Context, f OverviewFilter) (summaryRow, error) {
	extra, args := allArgs(f)
	var rows []summaryRow
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(count())                                            AS total_count,
		       toInt64(countIf(has_error = true))                          AS error_count,
		       avg(duration_nano / 1000000.0)                              AS avg_latency_ms,
		       quantileExact(0.50)(duration_nano / 1000000.0)              AS p50_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0)              AS p95_ms,
		       quantileExact(0.99)(duration_nano / 1000000.0)              AS p99_ms,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64)) AS total_input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64))AS total_output_tokens,
		       toInt64(uniqExact(attributes.'gen_ai.request.model'::String)) AS unique_models,
		       toInt64(uniqExact(attributes.'gen_ai.operation.name'::String))AS unique_operations,
		       toInt64(uniqExact(service_name))                              AS unique_services
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra, args...)
	if err != nil {
		return summaryRow{}, err
	}
	if len(rows) == 0 {
		return summaryRow{}, nil
	}
	return rows[0], nil
}

// ---- models ----

func (r *ClickHouseRepository) GetModels(ctx context.Context, f OverviewFilter) ([]modelRow, error) {
	extra, args := allArgs(f)
	var rows []modelRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String         AS model,
		       any(attributes.'gen_ai.system'::String)           AS provider,
		       toInt64(count())                                  AS request_count,
		       avg(duration_nano / 1000000.0)                    AS avg_latency_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0)    AS p95_ms,
		       toInt64(countIf(has_error = true))                AS error_count,
		       error_count * 100.0 / request_count               AS error_rate,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS output_tokens
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY model
		ORDER BY request_count DESC
	`, args...)
	return rows, err
}

// ---- operations ----

func (r *ClickHouseRepository) GetOperations(ctx context.Context, f OverviewFilter) ([]operationRow, error) {
	extra, args := allArgs(f)
	var rows []operationRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.operation.name'::String        AS operation,
		       toInt64(count())                                  AS request_count,
		       avg(duration_nano / 1000000.0)                    AS avg_latency_ms,
		       toInt64(countIf(has_error = true))                AS error_count,
		       error_count * 100.0 / request_count               AS error_rate,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS output_tokens
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY operation
		ORDER BY request_count DESC
	`, args...)
	return rows, err
}

// ---- services ----

func (r *ClickHouseRepository) GetServices(ctx context.Context, f OverviewFilter) ([]serviceRow, error) {
	extra, args := allArgs(f)
	var rows []serviceRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       toInt64(count()) AS request_count,
		       arrayStringConcat(groupUniqArray(attributes.'gen_ai.request.model'::String), ', ') AS models,
		       toInt64(countIf(has_error = true)) AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY service_name
		ORDER BY request_count DESC
	`, args...)
	return rows, err
}

// ---- model health ----

func (r *ClickHouseRepository) GetModelHealth(ctx context.Context, f OverviewFilter) ([]modelHealthRow, error) {
	extra, args := allArgs(f)
	var rows []modelHealthRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String      AS model,
		       any(attributes.'gen_ai.system'::String)        AS provider,
		       toInt64(count())                               AS request_count,
		       avg(duration_nano / 1000000.0)                 AS avg_latency_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0) AS p95_ms,
		       countIf(has_error = true) * 100.0 / count()    AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY model
		ORDER BY request_count DESC
	`, args...)
	return rows, err
}

// ---- top slow ----

func (r *ClickHouseRepository) GetTopSlow(ctx context.Context, f OverviewFilter, limit int) ([]topSlowRow, error) {
	extra, args := allArgs(f)
	args = append(args, clickhouse.Named("limit", limit))
	var rows []topSlowRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String      AS model,
		       attributes.'gen_ai.operation.name'::String     AS operation,
		       quantileExact(0.95)(duration_nano / 1000000.0) AS p95_ms,
		       toInt64(count())                               AS request_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY model, operation
		ORDER BY p95_ms DESC
		LIMIT @limit
	`, args...)
	return rows, err
}

// ---- top errors ----

func (r *ClickHouseRepository) GetTopErrors(ctx context.Context, f OverviewFilter, limit int) ([]topErrorRow, error) {
	extra, args := allArgs(f)
	args = append(args, clickhouse.Named("limit", limit))
	var rows []topErrorRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String AS model,
		       attributes.'gen_ai.operation.name'::String AS operation,
		       toInt64(countIf(has_error = true))         AS error_count,
		       error_count * 100.0 / count()              AS error_rate,
		       toInt64(count())                           AS request_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND has_error = true
		GROUP BY model, operation
		ORDER BY error_count DESC
		LIMIT @limit
	`, args...)
	return rows, err
}

// ---- finish reasons ----

func (r *ClickHouseRepository) GetFinishReasons(ctx context.Context, f OverviewFilter) ([]finishReasonRow, error) {
	extra, args := allArgs(f)
	var rows []finishReasonRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.response.finish_reasons'::String AS finish_reason,
		       toInt64(count()) AS count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND finish_reason != ''
		GROUP BY finish_reason
		ORDER BY count DESC
	`, args...)
	return rows, err
}

// ---- timeseries: requests ----

func (r *ClickHouseRepository) GetTimeseriesRequests(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []timeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String AS series,
		       toFloat64(count()) AS value
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, series
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- timeseries: latency ----

func (r *ClickHouseRepository) GetTimeseriesLatency(ctx context.Context, f OverviewFilter) ([]timeseriesDualRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []timeseriesDualRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String              AS series,
		       avg(duration_nano / 1000000.0)                          AS value1,
		       quantileExact(0.95)(duration_nano / 1000000.0)          AS value2
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, series
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- timeseries: errors ----

func (r *ClickHouseRepository) GetTimeseriesErrors(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []timeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String      AS series,
		       countIf(has_error = true) * 100.0 / count()    AS value
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, series
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- timeseries: tokens ----

func (r *ClickHouseRepository) GetTimeseriesTokens(ctx context.Context, f OverviewFilter) ([]timeseriesDualRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []timeseriesDualRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String                    AS series,
		       toFloat64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS value1,
		       toFloat64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS value2
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, series
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- timeseries: throughput ----

func (r *ClickHouseRepository) GetTimeseriesThroughput(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []timeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String AS series,
		       toFloat64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) /
		         greatest(sum(duration_nano) / 1000000000.0, 0.001) AS value
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, series
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- timeseries: cost ----

func (r *ClickHouseRepository) GetTimeseriesCost(ctx context.Context, f OverviewFilter) ([]timeseriesRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []timeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String AS series,
		       toFloat64(sum(attributes.'gen_ai.usage.input_tokens'::Int64) +
		                 sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS value
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, series
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}
