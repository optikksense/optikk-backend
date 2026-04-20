package query

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
	"golang.org/x/sync/errgroup"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

// errorPredicate matches error spans without combinators. The materialized
// http_status_code alias is numeric and avoids any explicit cast.
const errorPredicate = `(s.has_error = true OR s.http_status_code >= 400)`

type Repository interface {
	GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]traceRow, traceSummaryRow, bool, error)
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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]traceRow, traceSummaryRow, bool, error) {
	query, args := buildTracesQuery(f, limit, cursor)
	var rows []traceRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, traceSummaryRow{}, false, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	summary, err := r.getTraceSummary(ctx, f)
	return rows, summary, hasMore, err
}

// getTraceSummary splits the old combinator-driven summary into two
// parallel scans: totals (count + latency sum) and errors (error-only
// count). AvgDuration is derived Go-side from sum/count.
func (r *ClickHouseRepository) getTraceSummary(ctx context.Context, f TraceFilters) (traceSummaryRow, error) {
	totalsQ, totalsArgs := buildTracesSummaryTotalsQuery(f)
	errsQ, errsArgs := buildTracesSummaryErrorsQuery(f)

	var (
		totals traceSummaryTotalsRow
		errs   traceCountRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), totalsQ, totalsArgs...).ScanStruct(&totals)
	})
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), errsQ, errsArgs...).ScanStruct(&errs)
	})
	if err := g.Wait(); err != nil {
		return traceSummaryRow{}, err
	}
	return traceSummaryRow{
		TotalTraces:     totals.TotalTraces,
		ErrorTraces:     errs.Total,
		DurationMsSum:   totals.DurationMsSum,
		DurationMsCount: totals.TotalTraces,
	}, nil
}

func (r *ClickHouseRepository) GetTraceFacets(ctx context.Context, f TraceFilters) ([]traceFacetRow, error) {
	query, args := buildTraceFacetsQuery(f)
	var rows []traceFacetRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
	return rows, err
}

// GetTraceTrend runs totals and errors in parallel and merges per-bucket.
// p95_duration is zero-filled here; the service attaches it from sketches.
func (r *ClickHouseRepository) GetTraceTrend(ctx context.Context, f TraceFilters, step string) ([]traceTrendRow, error) {
	totalsQ, totalsArgs, errsQ, errsArgs := buildTraceTrendQueries(f, step)

	var (
		totals []traceTrendTotalsRow
		errs   []traceTrendErrorRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, totalsQ, totalsArgs...)
	})
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, errsQ, errsArgs...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.TimeBucket] = e.ErrorTraces
	}
	out := make([]traceTrendRow, 0, len(totals))
	for _, t := range totals {
		out = append(out, traceTrendRow{
			TimeBucket:  t.TimeBucket,
			TotalTraces: t.TotalTraces,
			ErrorTraces: errIdx[t.TimeBucket],
		})
	}
	return out, nil
}

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]spanRow, error) {
	var rows []spanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT span_id, parent_span_id, trace_id, name AS operation_name, service_name, kind_string AS span_kind,
		       timestamp AS start_time, duration_nano AS duration_nano, (duration_nano / 1000000.0) AS duration_ms,
		       status_code_string AS status, status_message, toJSONString(attributes) AS attributes,
		       http_method, http_url, http_status_code AS http_status_code,
		       mat_host_name AS host, mat_k8s_pod_name AS pod
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 10000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]spanRow, error) {
	var lookup spanTraceIDRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), `SELECT trace_id FROM observability.spans WHERE team_id = @teamID AND span_id = @spanID LIMIT 1`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("spanID", spanID)).ScanStruct(&lookup) //nolint:gosec // G115
	if err != nil {
		return nil, err
	}
	return r.GetTraceSpans(ctx, teamID, lookup.TraceID)
}

// GetErrorGroups uses the numeric http_status_code alias directly and
// appends the service-name filter Go-side rather than an SQL branch.
func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	query := `
		SELECT service_name,
		       name                                           AS operation_name,
		       status_message,
		       http_status_code                               AS http_status_code,
		       count()                                        AS error_count,
		       max(timestamp)                                 AS last_occurrence,
		       min(timestamp)                                 AS first_occurrence,
		       argMax(trace_id, timestamp)                    AS sample_trace_id
		FROM observability.spans s
		WHERE team_id = @teamID
		  AND (has_error = true OR http_status_code >= 400)
		  AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end`
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
	query += `
		GROUP BY service_name, operation_name, status_message, http_status_code
		ORDER BY error_count DESC
		LIMIT @limit`
	args = append(args, clickhouse.Named("limit", limit))

	var rows []errorGroupRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
	return rows, err
}

// GetErrorTimeSeries returns per-(bucket, service) totals and error counts
// via two parallel narrow-WHERE scans. error_rate is derived Go-side.
func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorTimeSeriesRow, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	baseArgs := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	whereTail := ` s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`

	totalsArgs := append([]any{}, baseArgs...)
	errsArgs := append([]any{}, baseArgs...)
	if serviceName != "" {
		whereTail += serviceNameFilter
		totalsArgs = append(totalsArgs, clickhouse.Named("serviceName", serviceName))
		errsArgs = append(errsArgs, clickhouse.Named("serviceName", serviceName))
	}

	totalsQ := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name AS service_name,
		       count() AS total_count
		FROM observability.spans s
		WHERE%s
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC`, bucket, whereTail)

	errsQ := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name AS service_name,
		       count() AS error_count
		FROM observability.spans s
		WHERE%s AND `+errorPredicate+`
		GROUP BY timestamp, s.service_name`, bucket, whereTail)

	var (
		totals []errorTimeSeriesTotalsRow
		errs   []errorTimeSeriesErrorRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, totalsQ, totalsArgs...)
	})
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, errsQ, errsArgs...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[errTimeSeriesKey(e.ServiceName, e.Timestamp)] = e.ErrorCount
	}
	out := make([]errorTimeSeriesRow, 0, len(totals))
	for _, t := range totals {
		errCount := errIdx[errTimeSeriesKey(t.ServiceName, t.Timestamp)]
		rate := 0.0
		if t.TotalCount > 0 {
			rate = float64(errCount) * 100.0 / float64(t.TotalCount)
		}
		out = append(out, errorTimeSeriesRow{
			ServiceName: t.ServiceName,
			Timestamp:   t.Timestamp,
			TotalCount:  t.TotalCount,
			ErrorCount:  errCount,
			ErrorRate:   rate,
		})
	}
	return out, nil
}

func errTimeSeriesKey(svc string, ts time.Time) string {
	return fmt.Sprintf("%s|%d", svc, ts.UnixNano())
}

// GetLatencyHistogram returns only the numeric bucket_min; the service
// formats the label string so no string cast is needed in SQL.
func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]latencyHistogramRow, error) {
	var rows []latencyHistogramRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT
		    floor(log10(duration_nano/1000000.0) * 10) / 10 AS bucket_min,
		    count() AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @serviceName AND s.name = @operationName
		GROUP BY bucket_min
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

// GetLatencyHeatmap returns the numeric bucket_min; the service formats the
// string label so the SQL stays cast-free.
func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyHeatmapRow, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT time_bucket,
		       latency_bucket,
		       count() AS span_count
		FROM (
			SELECT %s                                                AS time_bucket,
			       floor(log10(duration_nano/1000000.0) * 10) / 10   AS latency_bucket
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
	query += `) GROUP BY time_bucket, latency_bucket
	                   ORDER BY time_bucket ASC, latency_bucket ASC`

	var rows []latencyHeatmapRow
	return rows, r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
}

// SQL Query Builders follow ...

func buildTracesQuery(f TraceFilters, limit int, cursor TraceCursor) (string, []any) {
	where, args := buildWhereClause(f)
	columns := traceSelectColumns(f.SearchMode)

	query := fmt.Sprintf("SELECT %s FROM observability.spans s %s", columns, where)

	if cursor.Timestamp.UnixMilli() > 0 {
		query += " AND (s.timestamp < @cursorTime OR (s.timestamp = @cursorTime AND s.span_id < @cursorSpanID))"
		args = append(args, clickhouse.Named("cursorTime", cursor.Timestamp), clickhouse.Named("cursorSpanID", cursor.SpanID))
	}

	query += " ORDER BY s.timestamp DESC, s.span_id DESC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit+1)
	}

	return query, args
}

// buildTracesSummaryTotalsQuery produces the totals leg (count +
// duration-sum). DurationMsCount == TotalTraces so we don't project it.
func buildTracesSummaryTotalsQuery(f TraceFilters) (string, []any) {
	where, args := buildWhereClause(f)
	query := fmt.Sprintf(`
		SELECT count() AS total_traces,
		       sum(duration_nano / 1000000.0) AS duration_ms_sum
		FROM observability.spans s %s`, where)
	return query, args
}

// buildTracesSummaryErrorsQuery produces the error-only count leg.
func buildTracesSummaryErrorsQuery(f TraceFilters) (string, []any) {
	where, args := buildWhereClause(f)
	query := fmt.Sprintf(`
		SELECT count() AS total
		FROM observability.spans s %s AND `+errorPredicate, where)
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

// buildTraceTrendQueries returns the totals + errors SQL+args pair for the
// trend two-scan errgroup pattern. P95 is filled by the service from
// sketch.Querier.
func buildTraceTrendQueries(f TraceFilters, step string) (string, []any, string, []any) {
	where, args := buildWhereClause(f)
	bucket := timebucket.ByName(step).GetRawExpression("s.timestamp")
	totalsQ := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       count() AS total_traces
		FROM observability.spans s %s
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`, bucket, where)
	errsQ := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       count() AS error_traces
		FROM observability.spans s %s AND `+errorPredicate+`
		GROUP BY time_bucket`, bucket, where)
	return totalsQ, args, errsQ, append([]any{}, args...)
}

func traceSelectColumns(searchMode string) string {
	base := `s.span_id, s.trace_id, s.service_name AS service_name, s.name as operation_name,
	               s.timestamp as start_time, s.duration_nano as duration_nano,
	               s.duration_nano / 1000000.0 as duration_ms,
	               s.status_code_string as status, s.http_method, s.http_status_code as http_status_code,
	               s.status_message`
	if searchMode == SearchModeAll {
		base += `, s.parent_span_id, s.kind_string as span_kind`
	}
	return base
}
