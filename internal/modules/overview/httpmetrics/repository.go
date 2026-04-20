package httpmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const (
	spansRollupPrefix       = "observability.spans_rollup"
	metricsHistRollupPrefix = "observability.metrics_histograms_rollup"
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
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99,
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

// GetRequestRate groups by status_code attribute — not a rollup dimension.
// Stays on raw metrics; `attrString` pulls the label from JSON attrs.
func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	statusAttr := attrString(AttrHTTPStatusCode)

	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       %s AS status_code,
		       count() AS req_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket, status_code
		ORDER BY time_bucket, status_code`,
		bucket, statusAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerRequestDuration,
	)
	var raw []struct {
		Timestamp  string `ch:"time_bucket"`
		StatusCode string `ch:"status_code"`
		ReqCount   uint64 `ch:"req_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]StatusCodeBucket, len(raw))
	for i, row := range raw {
		rows[i] = StatusCodeBucket{
			Timestamp:  row.Timestamp,
			StatusCode: row.StatusCode,
			Count:      int64(row.ReqCount), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestDuration)
}

// GetActiveRequests is a gauge metric — not in histogram rollup. Stays on raw.
func (r *ClickHouseRepository) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       avg(value) AS val
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerActiveRequests,
	)
	var rows []TimeBucket
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
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
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_ms
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

// Status-code group bucketing needs the raw response_status_code column — not
// a rollup dim. Stays on raw spans.
func (r *ClickHouseRepository) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	query := fmt.Sprintf(`
		SELECT multiIf(
			toUInt16OrZero(response_status_code) BETWEEN 200 AND 299, '2xx',
			toUInt16OrZero(response_status_code) BETWEEN 300 AND 399, '3xx',
			toUInt16OrZero(response_status_code) BETWEEN 400 AND 499, '4xx',
			toUInt16OrZero(response_status_code) >= 500, '5xx',
			'other'
		) AS status_group,
		toInt64(count()) AS count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND response_status_code != ''
		GROUP BY status_group
		ORDER BY status_group ASC`, TableSpans)
	var rows []StatusGroupBucket
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
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

// External host (kind=3 CLIENT span) queries need `http_host` + non-root
// spans. Rollup is root-only and doesn't carry host. Stays on raw spans.

func (r *ClickHouseRepository) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host, toInt64(count()) AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20`, TableSpans)
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       toInt64(count()) AS req_count,
		       quantileTDigest(0.95)(duration_nano / 1000000.0) AS p95_ms
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY p95_ms DESC
		LIMIT 20`, TableSpans)
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       toInt64(count()) AS req_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_pct
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY error_pct DESC
		LIMIT 20`, TableSpans)
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}
