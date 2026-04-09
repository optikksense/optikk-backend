package query

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

type Repository interface {
	GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]traceRow, traceSummaryRow, bool, error)
	GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]traceRow, uint64, traceSummaryRow, error)
	GetTraceFacets(ctx context.Context, f TraceFilters) ([]traceFacetRow, error)
	GetTraceTrend(ctx context.Context, f TraceFilters, step string) ([]traceTrendRow, error)
	GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]spanRow, error)
	GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]spanRow, error)
	GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error)
	GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorTimeSeriesRow, error)
	GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]latencyHistogramRow, error)
	GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyHeatmapRow, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]traceRow, traceSummaryRow, bool, error) {
	query, args := buildTracesQuery(f, true, limit, cursor, 0)
	var rows []traceRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, traceSummaryRow{}, false, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	summary, err := r.getTraceSummary(ctx, f)
	return rows, summary, hasMore, err
}

func (r *ClickHouseRepository) GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]traceRow, uint64, traceSummaryRow, error) {
	query, args := buildTracesQuery(f, false, limit, TraceCursor{}, offset)
	var rows []traceRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, 0, traceSummaryRow{}, err
	}

	countQuery, countArgs := buildTracesCountQuery(f)
	var total uint64
	if err := r.db.QueryRow(ctx, &total, countQuery, countArgs...); err != nil {
		return nil, 0, traceSummaryRow{}, err
	}

	summary, err := r.getTraceSummary(ctx, f)
	return rows, total, summary, err
}

func (r *ClickHouseRepository) getTraceSummary(ctx context.Context, f TraceFilters) (traceSummaryRow, error) {
	query, args := buildTracesSummaryQuery(f)
	var res traceSummaryRow
	err := r.db.QueryRow(ctx, &res, query, args...)
	return res, err
}

func (r *ClickHouseRepository) GetTraceFacets(ctx context.Context, f TraceFilters) ([]traceFacetRow, error) {
	query, args := buildTraceFacetsQuery(f)
	var rows []traceFacetRow
	err := r.db.Select(ctx, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetTraceTrend(ctx context.Context, f TraceFilters, step string) ([]traceTrendRow, error) {
	query, args := buildTraceTrendQuery(f, step)
	var rows []traceTrendRow
	err := r.db.Select(ctx, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]spanRow, error) {
	var rows []spanRow
	err := r.db.Select(ctx, &rows, `
		SELECT span_id, parent_span_id, trace_id, name AS operation_name, service_name, kind_string AS span_kind,
		       timestamp AS start_time, toInt64(duration_nano) AS duration_nano, (duration_nano / 1000000.0) AS duration_ms, 
		       status_code_string AS status, status_message, toJSONString(attributes) AS attributes,
		       http_method, http_url, toUInt16OrZero(response_status_code) AS http_status_code,
		       mat_host_name AS host, mat_k8s_pod_name AS pod
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 10000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]spanRow, error) {
	var traceID string
	err := r.db.QueryRow(ctx, &traceID, "SELECT trace_id FROM observability.spans WHERE team_id = @teamID AND span_id = @spanID LIMIT 1",
		clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("spanID", spanID)) //nolint:gosec // G115
	if err != nil {
		return nil, err
	}
	return r.GetTraceSpans(ctx, teamID, traceID)
}

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	var rows []errorGroupRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       name                                           AS operation_name,
		       status_message,
		       toUInt16OrZero(response_status_code)           AS http_status_code,
		       count()                                        AS error_count,
		       max(timestamp)                                 AS last_occurrence,
		       min(timestamp)                                 AS first_occurrence,
		       argMax(trace_id, timestamp)                    AS sample_trace_id
		FROM observability.spans
		WHERE team_id = @teamID
		  AND (has_error = true OR toUInt16OrZero(response_status_code) >= 400)
		  AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND service_name = if(@serviceName = '', service_name, @serviceName)
		GROUP BY service_name, operation_name, status_message, http_status_code
		ORDER BY error_count DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorTimeSeriesRow, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       service_name,
		       count()                              AS total_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) AS error_count,
		       error_count * 100.0 / total_count     AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = if(@serviceName = '', s.service_name, @serviceName)
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC`, bucket)

	var rows []errorTimeSeriesRow
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]latencyHistogramRow, error) {
	var rows []latencyHistogramRow
	err := r.db.Select(ctx, &rows, `
		SELECT
		    concat(toString(floor(log10(duration_nano/1000000.0) * 10) / 10), ' - ', toString(floor(log10(duration_nano/1000000.0) * 10) / 10 + 0.1)) AS bucket_label,
		    floor(log10(duration_nano/1000000.0) * 10) / 10 AS bucket_min,
		    count() AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @serviceName AND s.name = @operationName
		GROUP BY bucket_label, bucket_min
		ORDER BY bucket_min ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyHeatmapRow, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT time_bucket,
		       latency_bucket,
		       count() AS span_count
		FROM (
			SELECT %s                            AS time_bucket,
			       toString(floor(log10(duration_nano/1000000.0) * 10) / 10) AS latency_bucket
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+rootspan.Condition("s")+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(`) GROUP BY time_bucket, latency_bucket
                   ORDER BY time_bucket ASC, latency_bucket ASC`)

	var rows []latencyHeatmapRow
	return rows, r.db.Select(ctx, &rows, query, args...)
}

// SQL Query Builders follow ...

func buildTracesQuery(f TraceFilters, useKeyset bool, limit int, cursor TraceCursor, offset int) (string, []any) {
	where, args := buildWhereClause(f)
	columns := traceSelectColumns(f.SearchMode)

	query := fmt.Sprintf("SELECT %s FROM observability.spans s %s", columns, where)

	if useKeyset && cursor.Timestamp.UnixMilli() > 0 {
		query += " AND (s.timestamp < @cursorTime OR (s.timestamp = @cursorTime AND s.span_id < @cursorSpanID))"
		args = append(args, clickhouse.Named("cursorTime", cursor.Timestamp), clickhouse.Named("cursorSpanID", cursor.SpanID))
	}

	query += " ORDER BY s.timestamp DESC, s.span_id DESC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit+1)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	return query, args
}

func buildTracesCountQuery(f TraceFilters) (string, []any) {
	where, args := buildWhereClause(f)
	return fmt.Sprintf("SELECT count() AS total FROM observability.spans s %s", where), args
}

func buildTracesSummaryQuery(f TraceFilters) (string, []any) {
	where, args := buildWhereClause(f)
	query := fmt.Sprintf(`
		SELECT count() AS total_traces,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) AS error_traces,
		       avg(duration_nano / 1000000.0) AS avg_duration,
		       quantile(0.5)(duration_nano / 1000000.0) AS p50_duration,
		       quantile(0.95)(duration_nano / 1000000.0) AS p95_duration,
		       quantile(0.99)(duration_nano / 1000000.0) AS p99_duration
		FROM observability.spans s %s`, where)
	return query, args
}

func buildTraceFacetsQuery(f TraceFilters) (string, []any) {
	where, args := buildWhereClause(f)
	// Example: just service name facets for now
	query := fmt.Sprintf(`
		SELECT 'service_name' AS facet_key, service_name AS facet_value, count() AS count
		FROM observability.spans s %s
		GROUP BY service_name
		ORDER BY count DESC LIMIT 10`, where)
	return query, args
}

func buildTraceTrendQuery(f TraceFilters, step string) (string, []any) {
	where, args := buildWhereClause(f)
	bucket := timebucket.ByName(step).GetRawExpression("s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       count() AS total_traces,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) AS error_traces,
		       quantile(0.95)(duration_nano / 1000000.0) AS p95_duration
		FROM observability.spans s %s
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`, bucket, where)
	return query, args
}

func traceSelectColumns(searchMode string) string {
	base := `s.span_id, s.trace_id, s.service_name AS service_name, s.name as operation_name,
	               s.timestamp as start_time, toInt64(s.duration_nano) as duration_nano,
	               s.duration_nano / 1000000.0 as duration_ms,
	               s.status_code_string as status, s.http_method, toUInt16OrZero(s.response_status_code) as http_status_code,
	               s.status_message`
	if searchMode == SearchModeAll {
		base += `, s.parent_span_id, s.kind_string as span_kind`
	}
	return base
}
