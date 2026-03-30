package query

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func rawTimeBucketExpr(startMs, endMs int64, column string) string {
	return timebucket.ExprForColumnTime(startMs, endMs, column)
}

// GetTracesKeyset returns a page of traces using keyset (cursor) pagination.
func (r *ClickHouseRepository) GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]traceRow, traceSummaryRow, bool, error) {
	queryFrag, args := buildWhereClause(f)

	if !cursor.Timestamp.IsZero() && cursor.SpanID != "" {
		queryFrag += ` AND (s.timestamp < @cursorTimestamp OR (s.timestamp = @cursorTimestamp AND s.span_id < @cursorSpanID))`
		args = append(args,
			clickhouse.Named("cursorTimestamp", cursor.Timestamp),
			clickhouse.Named("cursorSpanID", cursor.SpanID),
		)
	}

	selectCols := traceSelectColumns(f.SearchMode)
	query := `SELECT ` + selectCols + ` FROM observability.spans s` + queryFrag + ` ORDER BY s.timestamp DESC, s.span_id DESC LIMIT @limit`
	args = append(args, clickhouse.Named("limit", limit+1))
	var rows []traceRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, traceSummaryRow{}, false, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	summaryFrag, summaryArgs := buildWhereClause(f)
	var summary traceSummaryRow
	if err := r.db.QueryRow(ctx, &summary, `
		SELECT toInt64(COUNT(*)) as total_traces,
		       toInt64(sum(if(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400, 1, 0))) as error_traces,
		       AVG(s.duration_nano / 1000000.0) as avg_duration,
		       quantile(0.5)(s.duration_nano / 1000000.0) as p50_duration,
		       quantile(0.95)(s.duration_nano / 1000000.0) as p95_duration,
		       quantile(0.99)(s.duration_nano / 1000000.0) as p99_duration
		FROM observability.spans s`+summaryFrag, summaryArgs...); err != nil {
		//nolint:nilerr // summary is supplementary; return traces with empty summary on failure
		return rows, traceSummaryRow{}, hasMore, nil
	}

	return rows, summary, hasMore, nil
}

// GetTraces returns traces with legacy OFFSET-based pagination.
func (r *ClickHouseRepository) GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]traceRow, int64, traceSummaryRow, error) {
	queryFrag, args := buildWhereClause(f)

	selectCols := traceSelectColumns(f.SearchMode)
	query := `SELECT ` + selectCols + ` FROM observability.spans s` + queryFrag + ` ORDER BY s.timestamp DESC LIMIT @limit OFFSET @offset`
	args = append(args,
		clickhouse.Named("limit", limit),
		clickhouse.Named("offset", offset),
	)
	var rows []traceRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, 0, traceSummaryRow{}, err
	}

	countFrag, countArgs := buildWhereClause(f)
	var countRow traceCountRow
	total := int64(0)
	if err := r.db.QueryRow(ctx, &countRow, `SELECT toInt64(COUNT(*)) as total FROM observability.spans s`+countFrag, countArgs...); err == nil {
		total = countRow.Total
	}

	summaryFrag, summaryArgs := buildWhereClause(f)
	var summary traceSummaryRow
	if err := r.db.QueryRow(ctx, &summary, `
		SELECT toInt64(COUNT(*)) as total_traces,
		       toInt64(sum(if(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400, 1, 0))) as error_traces,
		       AVG(s.duration_nano / 1000000.0) as avg_duration,
		       quantile(0.5)(s.duration_nano / 1000000.0) as p50_duration,
		       quantile(0.95)(s.duration_nano / 1000000.0) as p95_duration,
		       quantile(0.99)(s.duration_nano / 1000000.0) as p99_duration
		FROM observability.spans s`+summaryFrag, summaryArgs...); err != nil {
		return rows, total, traceSummaryRow{}, err
	}

	return rows, total, summary, nil
}

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]spanRow, error) {
	var rows []spanRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id, s.trace_id, s.name as operation_name, s.service_name AS service_name, s.kind_string as span_kind,
		       s.timestamp as start_time,
		       toInt64(s.duration_nano) as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.status_message,
		       s.http_method, s.http_url, toUInt16OrZero(s.response_status_code) as http_status_code,
		       s.mat_host_name as host, s.mat_k8s_pod_name as pod, toJSONString(s.attributes) as attributes
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY s.timestamp ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]spanRow, error) {
	var rows []spanRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id, s.trace_id, s.name as operation_name, s.service_name AS service_name, s.kind_string as span_kind,
		       s.timestamp as start_time,
		       toInt64(s.duration_nano) as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.status_message,
		       s.http_method, s.http_url, toUInt16OrZero(s.response_status_code) as http_status_code,
		       s.mat_host_name as host, s.mat_k8s_pod_name as pod, toJSONString(s.attributes) as attributes
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.trace_id = (
		      SELECT trace_id FROM observability.spans WHERE team_id = @teamID AND span_id = @spanID LIMIT 1
		  )
		ORDER BY s.timestamp ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("spanID", spanID)) //nolint:gosec // G115
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetServiceDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceDependencyRow, error) {
	var rows []serviceDependencyRow
	err := r.db.Select(ctx, &rows, `
		SELECT s1.service_name AS source,
		       s2.service_name AS target,
		       toInt64(count()) AS call_count
		FROM observability.spans s1
		JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
			AND s2.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s2.timestamp BETWEEN @start AND @end
		WHERE s1.team_id = @teamID AND s1.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s1.kind = 3 AND s1.timestamp BETWEEN @start AND @end
		  AND s1.service_name != s2.service_name
		GROUP BY s1.service_name, s2.service_name
		ORDER BY call_count DESC
		LIMIT 100
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

// traceSelectColumns returns the SELECT column list.
// In "all" mode (span-level search), include parent_span_id and span_kind.
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
