// Package query backs /v1/traces/* trace-list and trace-structure endpoints.
//
// Raw-read rationale: aggregate methods (trace-summary counters, error timeseries,
// latency histograms) read rollups. Drill-down methods (GetTraceSpans,
// GetSpanTree, GetErrorGroups details, GetLatencyHeatmap) stay on raw
// `observability.spans` because they need per-span structure — span_id,
// parent_span_id, status_message, attributes, body — that rollup aggregation
// collapses away. Every drill-down is bounded by trace_id (hits `idx_trace_id`
// bloom-filter, GRAN 4) or a narrow service+name window, so raw reads stay
// cheap.
package query

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

const (
	serviceNameFilter = " AND s.service_name = @serviceName"
	spansRollupPrefix = "observability.spans_rollup"
)

// queryIntervalMinutes returns the step (in minutes) for the query-time
// `toStartOfInterval` group-by. Returns max(tierStep, dashboardStep) so the
// step is never finer than the tier's native resolution. Copied from
// overview/overview/repository.go.
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

// buildRollupWhere produces a minimal WHERE clause for reads from
// `observability.spans_rollup_*`. Only rollup-dimensional filters are
// honoured (team_id, bucket_ts, service_name, http_method, operation_name).
// Filters that require raw span columns (TraceID, SearchText, attributes,
// MinDuration, SpanKind…) are silently ignored — the rollup can't express
// them. Keyset + explorer drill-down paths that need those filters continue
// to read from `observability.spans` directly.
func buildRollupWhere(f TraceFilters) (frag string, args []any) {
	startMs, endMs := normalizeTimeRange(f.StartMs, f.EndMs)

	frag = ` WHERE team_id = @teamID AND bucket_ts BETWEEN @start AND @end`
	args = []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	if len(f.Services) > 0 {
		frag += ` AND service_name IN @services`
		args = append(args, clickhouse.Named("services", f.Services))
	}
	if f.SpanName != "" {
		frag += ` AND operation_name = @spanName`
		args = append(args, clickhouse.Named("spanName", f.SpanName))
	}
	if f.HTTPMethod != "" {
		frag += ` AND upper(http_method) = upper(@httpMethod)`
		args = append(args, clickhouse.Named("httpMethod", f.HTTPMethod))
	}
	return frag, args
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

// getTraceSummary returns aggregate stats (total / error counts, latency
// percentiles) for the requested window. Migrated to the `spans_rollup_*`
// cascade; rollup-incompatible filters on f are dropped — see
// buildRollupWhere.
func (r *ClickHouseRepository) getTraceSummary(ctx context.Context, f TraceFilters) (traceSummaryRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, f.StartMs, f.EndMs)
	where, args := buildRollupWhere(f)
	query := fmt.Sprintf(`
		SELECT sumMerge(request_count)                                              AS total_traces,
		       sumMerge(error_count)                                                AS error_traces,
		       if(sumMerge(request_count) = 0, 0,
		          sumMerge(duration_ms_sum) / sumMerge(request_count))              AS avg_duration,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS p50_duration,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2  AS p95_duration,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3  AS p99_duration
		FROM %s %s`, table, where)

	var res traceSummaryRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&res)
	return res, err
}

// GetTraceFacets returns service-name facet counts. Migrated to
// `spans_rollup_*` cascade.
func (r *ClickHouseRepository) GetTraceFacets(ctx context.Context, f TraceFilters) ([]traceFacetRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, f.StartMs, f.EndMs)
	where, args := buildRollupWhere(f)
	query := fmt.Sprintf(`
		SELECT 'service_name'         AS facet_key,
		       service_name           AS facet_value,
		       sumMerge(request_count) AS count
		FROM %s %s
		GROUP BY service_name
		ORDER BY count DESC
		LIMIT 10`, table, where)

	var rows []traceFacetRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
	return rows, err
}

// GetTraceTrend returns time-bucketed trend data (total/error counts + p95)
// from the `spans_rollup_*` cascade. `step` is honoured as the query-time
// bucket width (clamped to the tier's native step).
func (r *ClickHouseRepository) GetTraceTrend(ctx context.Context, f TraceFilters, step string) ([]traceTrendRow, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, f.StartMs, f.EndMs)
	where, args := buildRollupWhere(f)

	stepMin := queryIntervalMinutes(tierStep, f.StartMs, f.EndMs)
	if step != "" {
		// Explicit step from caller (e.g. "5m"/"1h") — parse conservatively.
		if s := stepFromName(step); s > 0 {
			if s < tierStep {
				s = tierStep
			}
			stepMin = s
		}
	}

	query := fmt.Sprintf(`
		SELECT formatDateTime(toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)), '%%Y-%%m-%%d %%H:%%i:00') AS time_bucket,
		       sumMerge(request_count)                                             AS total_traces,
		       sumMerge(error_count)                                               AS error_traces,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_duration
		FROM %s %s
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`, table, where)
	args = append(args, clickhouse.Named("intervalMin", stepMin))

	var rows []traceTrendRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
	return rows, err
}

// stepFromName maps explicit step tokens to minute widths. Unknown tokens
// return 0 which signals "use the default (queryIntervalMinutes)".
func stepFromName(step string) int64 {
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

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]spanRow, error) {
	var rows []spanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
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
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), "SELECT trace_id FROM observability.spans WHERE team_id = @teamID AND span_id = @spanID LIMIT 1", clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("spanID", spanID)).ScanStruct(&traceID) //nolint:gosec // G115
	if err != nil {
		return nil, err
	}
	return r.GetTraceSpans(ctx, teamID, traceID)
}

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	var rows []errorGroupRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
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

// GetErrorTimeSeries returns per-service error/total counts time-bucketed
// from the `spans_rollup_*` cascade. `error_rate` is computed in SQL from
// the merged state columns.
func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorTimeSeriesRow, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))      AS timestamp,
		       service_name,
		       sumMerge(request_count)                                           AS total_count,
		       sumMerge(error_count)                                             AS error_count,
		       if(sumMerge(request_count) = 0, 0,
		          sumMerge(error_count) * 100.0 / sumMerge(request_count))      AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_name = if(@serviceName = '', service_name, @serviceName)
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC`, table)

	var rows []errorTimeSeriesRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	return rows, err
}

// GetLatencyHistogram returns log10-width histogram buckets derived from the
// `spans_rollup_*` t-digest. The rollup does not retain per-span durations,
// so buckets are approximated from 10 equal-mass quantile samples and
// `span_count` is distributed evenly. Shape + column semantics match the
// legacy raw-scan contract.
func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]latencyHistogramRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	// `quantilesTDigestWeightedMerge(q0,q1,...)` returns Array(Float64); we
	// pull p0..p1 in 0.1 steps (11 boundaries ⇒ 10 equal-mass buckets).
	type histSampleRow struct {
		Total     uint64    `ch:"total"`
		Quantiles []float64 `ch:"quantiles"`
	}
	query := fmt.Sprintf(`
		SELECT sumMerge(request_count) AS total,
		       quantilesTDigestWeightedMerge(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(latency_ms_digest) AS quantiles
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_name = @serviceName
		  AND operation_name = @operationName`, table)

	var sample histSampleRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	).ScanStruct(&sample); err != nil {
		return nil, err
	}
	if sample.Total == 0 || len(sample.Quantiles) < 11 {
		return []latencyHistogramRow{}, nil
	}

	perBucket := sample.Total / 10 //nolint:gosec // domain-bounded
	rows := make([]latencyHistogramRow, 0, 10)
	for i := 0; i < 10; i++ {
		lo := sample.Quantiles[i]
		hi := sample.Quantiles[i+1]
		bucketMin := int64(0)
		if lo > 0 {
			// Match the legacy SQL shape: floor(log10(ms)*10)/10 stored as
			// the integer-scaled bucket_min.
			bucketMin = int64(math.Floor(math.Log10(lo) * 10))
		}
		rows = append(rows, latencyHistogramRow{
			BucketLabel: fmt.Sprintf("%.1f - %.1f", lo, hi),
			BucketMin:   bucketMin,
			SpanCount:   perBucket,
		})
	}
	return rows, nil
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
