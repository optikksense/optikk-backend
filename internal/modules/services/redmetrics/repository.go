package redmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error)
	GetApdexByService(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error)
	GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error)
	GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error)
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]requestRateRawRow, error)
	GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]p95LatencyRawRow, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]spanKindRawRow, error)
	GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorByRouteRawRow, error)
	GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                                              AS service,
		       sum(request_count)                                                   AS total_count,
		       sum(error_count)                                                     AS error_count,
		       quantileTimingMerge(0.5)(latency_state)                              AS p50_ms,
		       quantileTimingMerge(0.95)(latency_state)                             AS p95_ms,
		       quantileTimingMerge(0.99)(latency_state)                             AS p99_ms
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service`
	var rows []redSummaryServiceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetSummary",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                                              AS service,
		       count()                                                              AS total_count,
		       countIf(duration_nano <= @satisfiedNs)                               AS satisfied,
		       countIf(duration_nano > @satisfiedNs AND duration_nano <= @toleratingNs) AS tolerating
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service
		ORDER BY total_count DESC`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("satisfiedNs", uint64(satisfiedMs*1_000_000)),
		clickhouse.Named("toleratingNs", uint64(toleratingMs*1_000_000)),
	)
	var rows []apdexRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetApdex",
		&rows, query, args...)
}

func (r *ClickHouseRepository) GetApdexByService(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT service                                                              AS service,
		       count()                                                              AS total_count,
		       countIf(duration_nano <= @satisfiedNs)                               AS satisfied,
		       countIf(duration_nano > @satisfiedNs AND duration_nano <= @toleratingNs) AS tolerating
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service   = @serviceName
		GROUP BY service
		ORDER BY total_count DESC`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("satisfiedNs", uint64(satisfiedMs*1_000_000)),
		clickhouse.Named("toleratingNs", uint64(toleratingMs*1_000_000)),
	)
	var rows []apdexRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetApdexByService",
		&rows, query, args...)
}

// slowOpsCandidatePoolMultiplier oversamples the candidate set so the outer
// quantileTiming pass sees enough operations to find the true top-N by p95
// without computing percentiles for every cardinality group.
const slowOpsCandidatePoolMultiplier = 20

func (r *ClickHouseRepository) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		),
		candidates AS (
		    SELECT service, name AS operation_name
		    FROM observability.spans_1m
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		    GROUP BY service, name
		    ORDER BY sum(request_count) DESC
		    LIMIT @candidateLimit
		)
		SELECT service                                                              AS service,
		       name                                                                 AS operation_name,
		       sum(request_count)                                                   AS span_count,
		       quantileTimingMerge(0.5)(latency_state)                              AS p50_ms,
		       quantileTimingMerge(0.95)(latency_state)                             AS p95_ms,
		       quantileTimingMerge(0.99)(latency_state)                             AS p99_ms
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND (service, name) IN (SELECT service, operation_name FROM candidates)
		GROUP BY service, name
		ORDER BY p95_ms DESC
		LIMIT @limit`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("limit", limit),
		clickhouse.Named("candidateLimit", limit*slowOpsCandidatePoolMultiplier),
	)
	var rows []slowOperationRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetTopSlowOperations",
		&rows, query, args...)
}

func (r *ClickHouseRepository) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                  AS service,
		       name                     AS operation_name,
		       sum(request_count)       AS total_count,
		       sum(error_count)         AS error_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service, name
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT @limit`
	args := append(spanArgs(teamID, startMs, endMs), clickhouse.Named("limit", limit))
	var rows []errorOperationRow
	return rows, dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "redmetrics.GetTopErrorOperations",
		&rows, query, args...)
}

type requestRateRawRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	ServiceName  string    `ch:"service"`
	RequestCount uint64    `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]requestRateRawRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket) AS timestamp,
		       service               AS service,
		       sum(request_count)    AS request_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY ts_bucket, service
		ORDER BY timestamp ASC`
	var rows []requestRateRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetRequestRateTimeSeries",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

type p95LatencyRawRow struct {
	Timestamp   time.Time `ch:"timestamp"`
	ServiceName string    `ch:"service"`
	P95Ms       float32   `ch:"p95_ms"`
}

func (r *ClickHouseRepository) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]p95LatencyRawRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket)                                            AS timestamp,
		       service                                                          AS service,
		       quantileTimingMerge(0.95)(latency_state)                         AS p95_ms
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY ts_bucket, service
		ORDER BY timestamp ASC`
	var rows []p95LatencyRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetP95LatencyTimeSeries",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

type spanKindRawRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	KindString string    `ch:"kind_string"`
	SpanCount  uint64    `ch:"span_count"`
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]spanKindRawRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket) AS timestamp,
		       kind_string           AS kind_string,
		       sum(request_count)    AS span_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY ts_bucket, kind_string
		ORDER BY timestamp ASC`
	var rows []spanKindRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetSpanKindBreakdown",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

type errorByRouteRawRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	HTTPRoute    string    `ch:"http_route"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorByRouteRawRow, error) {
	// Prefer the OTel canonical http.route; fall back to operation_name when
	// the attribute is empty (older instrumentations that didn't set it).
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket)                                AS timestamp,
		       if(http_route != '', http_route, name)               AS http_route,
		       sum(request_count)                                   AS request_count,
		       sum(error_count)                                     AS error_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND (http_route != '' OR name != '')
		GROUP BY ts_bucket, http_route
		ORDER BY timestamp ASC, error_count DESC`
	var rows []errorByRouteRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetErrorsByRoute",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                          AS service,
		       sum(duration_ms_sum)             AS total_ms,
		       sum(request_count)               AS span_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service`
	var rows []latencyBreakdownRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetLatencyBreakdown",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

func spanArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
