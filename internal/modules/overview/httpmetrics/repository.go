package httpmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

const (
	spansRollupPrefix             = "observability.spans_rollup"
	spansPeerRollupPrefix         = "observability.spans_peer_rollup"
	metricsHistRollupPrefix       = "observability.metrics_histograms_rollup"
	metricsGaugesRollupPrefix     = "observability.metrics_gauges_rollup"
	metricsGaugesByStatusPrefix   = "observability.metrics_gauges_by_status_rollup"
)

// Histogram-metric queries target `observability.metrics_histograms_rollup_1m`.
// Span-based route/host queries target `observability.spans_rollup_1m` where
// the `endpoint` dim (coalesced http_route/http_target/name at MV time)
// substitutes for the previous `mat_http_route` scan. Distribution + active-
// request + status-code breakdown queries stay on raw tables because the
// needed dimensions aren't in the rollup.

type Repository interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusCodeBucketDTO, error)
	GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error)
	GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeTimeseriesPointDTO, error)
	GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusGroupBucketDTO, error)
	GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorTimeseriesPointDTO, error)
	GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
	GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
	GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

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

func histRollupParams(teamID int64, startMs, endMs int64, metricName string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
	}
}

func spanRollupParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

type histogramSummaryRawRow struct {
	P50     float64 `ch:"p50"`
	P95     float64 `ch:"p95"`
	P99     float64 `ch:"p99"`
	HistSum float64 `ch:"hist_sum"`
	HistCnt uint64  `ch:"hist_count"`
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramSummary, error) {
	table, _ := rollup.TierTableFor(metricsHistRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) AS p99,
		    sumMerge(hist_sum)                                                  AS hist_sum,
		    sumMerge(hist_count)                                                AS hist_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName`, table)

	var raw histogramSummaryRawRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, histRollupParams(teamID, startMs, endMs, metricName)...).ScanStruct(&raw); err != nil {
		return HistogramSummary{}, err
	}
	avg := 0.0
	if raw.HistCnt > 0 {
		avg = raw.HistSum / float64(raw.HistCnt)
	}
	return HistogramSummary{P50: raw.P50, P95: raw.P95, P99: raw.P99, Avg: avg}, nil
}

// GetRequestRate reads the Phase-7 `metrics_gauges_by_status_rollup` which
// pre-aggregates `http.server.request.duration` samples by status_code. The MV
// extracts `attributes.http.status_code` at ingest so we read a single key'd
// rollup row per bucket × status.
func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesByStatusPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       http_status_code    AS status_code,
		       sumMerge(sample_count) AS req_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket, status_code
		ORDER BY time_bucket, status_code`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", MetricHTTPServerRequestDuration),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp  time.Time `ch:"time_bucket"`
		StatusCode string    `ch:"status_code"`
		ReqCount   uint64    `ch:"req_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]StatusCodeBucket, len(raw))
	for i, row := range raw {
		rows[i] = StatusCodeBucket{
			Timestamp:  row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			StatusCode: row.StatusCode,
			Count:      int64(row.ReqCount), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestDuration)
}

// GetActiveRequests reads the Phase-7 gauges rollup. MV entry emits a `Gauge`
// metric row with no state_dim, so we just sum avg_num / count across all
// host+pod rows for the time bucket.
func (r *ClickHouseRepository) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(value_avg_num) AS value_num,
		       sumMerge(sample_count)  AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket
		ORDER BY time_bucket`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", MetricHTTPServerActiveRequests),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		ValueNum  float64   `ch:"value_num"`
		ValueCnt  uint64    `ch:"value_cnt"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]TimeBucket, len(raw))
	for i, row := range raw {
		var valPtr *float64
		if row.ValueCnt > 0 {
			v := row.ValueNum / float64(row.ValueCnt)
			valPtr = &v
		}
		rows[i] = TimeBucket{
			Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			Value:     valPtr,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestBodySize)
}

func (r *ClickHouseRepository) GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerResponseBodySize)
}

func (r *ClickHouseRepository) GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPClientRequestDuration)
}

func (r *ClickHouseRepository) GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricDNSLookupDuration)
}

func (r *ClickHouseRepository) GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricTLSConnectDuration)
}

// Span-backed route queries now read `spans_rollup_1m`, where `endpoint` is
// coalesce(http_route, http_target, name) captured at MV time. Rollup is
// root-only; HTTP server spans are root-scope, so coverage matches.

func (r *ClickHouseRepository) GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT endpoint AS route,
		       sumMerge(request_count) AS req_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''
		GROUP BY route
		ORDER BY req_count DESC
		LIMIT 20`, table)

	var raw []struct {
		Route    string `ch:"route"`
		ReqCount uint64 `ch:"req_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, spanRollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]RouteMetric, len(raw))
	for i, row := range raw {
		rows[i] = RouteMetric{
			Route:    row.Route,
			ReqCount: int64(row.ReqCount), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT endpoint AS route,
		       sumMerge(request_count) AS req_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''
		GROUP BY route
		ORDER BY p95_ms DESC
		LIMIT 20`, table)

	var raw []struct {
		Route    string  `ch:"route"`
		ReqCount uint64  `ch:"req_count"`
		P95Ms    float64 `ch:"p95_ms"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, spanRollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]RouteMetric, len(raw))
	for i, row := range raw {
		rows[i] = RouteMetric{
			Route:    row.Route,
			ReqCount: int64(row.ReqCount), //nolint:gosec // domain-bounded
			P95Ms:    row.P95Ms,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT endpoint AS route,
		       sumMerge(request_count) AS req_count,
		       sumMerge(error_count)   AS err_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''
		GROUP BY route
		ORDER BY err_count DESC
		LIMIT 20`, table)

	var raw []struct {
		Route    string `ch:"route"`
		ReqCount uint64 `ch:"req_count"`
		ErrCount uint64 `ch:"err_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, spanRollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]RouteMetric, 0, len(raw))
	for _, row := range raw {
		total := int64(row.ReqCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrCount)  //nolint:gosec // domain-bounded
		pct := 0.0
		if total > 0 {
			pct = float64(errs) * 100.0 / float64(total)
		}
		rows = append(rows, RouteMetric{
			Route:    row.Route,
			ReqCount: total,
			ErrorPct: pct,
		})
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       endpoint                AS http_route,
		       sumMerge(request_count) AS req_count,
		       sumMerge(error_count)   AS err_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''
		GROUP BY time_bucket, http_route
		ORDER BY time_bucket ASC, err_count DESC`, table)
	args := append(spanRollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		HttpRoute string    `ch:"http_route"`
		ReqCount  uint64    `ch:"req_count"`
		ErrCount  uint64    `ch:"err_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]RouteTimeseriesPoint, len(raw))
	for i, row := range raw {
		total := int64(row.ReqCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrCount)  //nolint:gosec // domain-bounded
		rate := 0.0
		if total > 0 {
			rate = float64(errs) * 100.0 / float64(total)
		}
		rows[i] = RouteTimeseriesPoint{
			Timestamp:  row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			HttpRoute:  row.HttpRoute,
			ReqCount:   total,
			ErrorCount: errs,
			ErrorRate:  rate,
		}
	}
	return rows, nil
}

// GetStatusDistribution counts CLIENT spans per HTTP-status bucket via
// `spans_peer_rollup` (http_status_bucket is a rollup key). Server-side spans
// contribute via the root-level `spans_error_fingerprint` which also buckets
// status_code — but the existing panel is client-call distribution so peer
// rollup is the right source.
func (r *ClickHouseRepository) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	table, _ := rollup.TierTableFor(spansPeerRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT http_status_bucket              AS status_group,
		       toInt64(sumMerge(request_count)) AS count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND http_status_bucket != 'other'
		GROUP BY status_group
		ORDER BY status_group ASC`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []StatusGroupBucket
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(request_count) AS req_count,
		       sumMerge(error_count)   AS err_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`, table)
	args := append(spanRollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		ReqCount  uint64    `ch:"req_count"`
		ErrCount  uint64    `ch:"err_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]ErrorTimeseriesPoint, len(raw))
	for i, row := range raw {
		total := int64(row.ReqCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrCount)  //nolint:gosec // domain-bounded
		rate := 0.0
		if total > 0 {
			rate = float64(errs) * 100.0 / float64(total)
		}
		rows[i] = ErrorTimeseriesPoint{
			Timestamp:  row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			ReqCount:   total,
			ErrorCount: errs,
			ErrorRate:  rate,
		}
	}
	return rows, nil
}

// External host (CLIENT-kind span) queries read `spans_peer_rollup` — Phase-9
// addition keyed on (service, peer_service, host_name, http_status_bucket).
// MV filters `kind = 3` so only CLIENT spans contribute.

func (r *ClickHouseRepository) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	table, _ := rollup.TierTableFor(spansPeerRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT host_name                        AS host,
		       toInt64(sumMerge(request_count)) AS req_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND host_name != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	table, _ := rollup.TierTableFor(spansPeerRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT host_name                                                                   AS host,
		       toInt64(sumMerge(request_count))                                            AS req_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])         AS p95_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND host_name != ''
		GROUP BY host
		ORDER BY p95_ms DESC
		LIMIT 20`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	table, _ := rollup.TierTableFor(spansPeerRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT host_name                                                                   AS host,
		       toInt64(sumMerge(request_count))                                            AS req_count,
		       toFloat64(sumMerge(error_count)) * 100.0 / nullIf(toFloat64(sumMerge(request_count)), 0) AS error_pct
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND host_name != ''
		GROUP BY host
		ORDER BY error_pct DESC
		LIMIT 20`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}
