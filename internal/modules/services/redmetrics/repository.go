package redmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) GetFleetREDMetrics(ctx context.Context, teamID int64, startMs, endMs int64) ([]redMetricsRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                              AS service,
		       sum(request_count)                                   AS total_count,
		       sum(error_count)                                     AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		GROUP BY service`
	var rows []redMetricsRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetFleetREDMetrics",
		&rows, query, chargs.RangeArgs(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 3 {
			rows[i].P50Ms = rows[i].QS[0]
			rows[i].P95Ms = rows[i].QS[1]
			rows[i].P99Ms = rows[i].QS[2]
		}
	}
	return rows, nil
}

func (r *Repository) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error) {
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
		     AND timestamp   BETWEEN @start AND @end
		GROUP BY service
		ORDER BY total_count DESC`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("satisfiedNs", uint64(satisfiedMs*1_000_000)),
		clickhouse.Named("toleratingNs", uint64(toleratingMs*1_000_000)),
	)
	var rows []apdexRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetApdex",
		&rows, query, args...)
}

func (r *Repository) GetApdexByService(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error) {
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
		     AND timestamp   BETWEEN @start AND @end
		WHERE service   = @serviceName
		GROUP BY service
		ORDER BY total_count DESC`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("satisfiedNs", uint64(satisfiedMs*1_000_000)),
		clickhouse.Named("toleratingNs", uint64(toleratingMs*1_000_000)),
	)
	var rows []apdexRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetApdexByService",
		&rows, query, args...)
}

func (r *Repository) GetServiceREDMetrics(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*redMetricsRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT service                                              AS service,
		       sum(request_count)                                   AS total_count,
		       sum(error_count)                                     AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		WHERE service = @serviceName
		GROUP BY service`
	var rows []redMetricsRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetServiceREDMetrics",
		&rows, query, detailArgs(teamID, startMs, endMs, serviceName)...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	if len(row.QS) >= 3 {
		row.P50Ms = row.QS[0]
		row.P95Ms = row.QS[1]
		row.P99Ms = row.QS[2]
	}
	return &row, nil
}

func (r *Repository) GetOperationBaseline(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) (operationBaselineRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT sum(request_count)                                   AS span_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		WHERE service = @serviceName
		  AND name    = @operationName`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
	)
	var rows []operationBaselineRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetOperationBaseline",
		&rows, query, args...); err != nil {
		return operationBaselineRow{}, err
	}
	if len(rows) == 0 {
		return operationBaselineRow{}, nil
	}
	row := rows[0]
	if len(row.QS) >= 3 {
		row.P50Ms = row.QS[0]
		row.P95Ms = row.QS[1]
		row.P99Ms = row.QS[2]
	}
	return row, nil
}

func (r *Repository) GetServiceSaturationAggs(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, metricNames []string,
) ([]serviceMetricRow, error) {
	const query = `
		WITH service_hosts AS (
		    SELECT DISTINCT host
		    FROM observability.spans_1m
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND service     = @serviceName
		         AND host        != ''
		),
		active_fps AS (
		    SELECT fingerprint, any(service) AS service
		    FROM observability.metrics_resource AS mr
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		    WHERE (mr.service = @serviceName OR mr.host IN service_hosts)
		    GROUP BY fingerprint
		)
		SELECT
		    r.service                         AS service,
		    m.metric_name                     AS metric_name,
		    sum(m.val_sum) / sum(m.val_count) AS value
		FROM observability.metrics_1m AS m
		INNER JOIN active_fps AS r ON m.fingerprint = r.fingerprint
		PREWHERE m.team_id     = @teamID
		     AND m.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND m.metric_name IN @metricNames
		     AND m.timestamp   BETWEEN @start AND @end
		GROUP BY service, metric_name`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("metricNames", metricNames),
	)
	var rows []serviceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetServiceSaturationAggs",
		&rows, query, args...)
}

type requestRateRawRow struct {
	BucketAt     time.Time `ch:"bucket_at"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *Repository) GetRequestAndErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]requestRateRawRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       sum(request_count)    AS request_count,
		       sum(error_count)      AS error_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		GROUP BY bucket_at
		ORDER BY bucket_at ASC`
	var rows []requestRateRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetRequestAndErrorRateTimeSeries",
		&rows, query, chargs.RangeArgs(teamID, startMs, endMs)...)
}

func (r *Repository) GetFleetSaturationAggs(
	ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string,
) ([]serviceMetricRow, error) {
	// service is grouped from metrics_resource (by fingerprint); the scalar
	// rollup supplies the values.
	const query = `
		WITH fps AS (
		    SELECT fingerprint, any(service) AS service
		    FROM observability.metrics_resource AS mr
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    WHERE mr.service != ''
		    GROUP BY fingerprint
		)
		SELECT
		    r.service                         AS service,
		    m.metric_name                     AS metric_name,
		    sum(m.val_sum) / sum(m.val_count) AS value
		FROM observability.metrics_1m AS m
		INNER JOIN fps AS r ON m.fingerprint = r.fingerprint
		PREWHERE m.team_id     = @teamID
		     AND m.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND m.metric_name IN @metricNames
		     AND m.timestamp   BETWEEN @start AND @end
		GROUP BY service, metric_name`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("metricNames", metricNames),
	)
	var rows []serviceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetFleetSaturationAggs",
		&rows, query, args...)
}

// statusBucketTimeseriesRow is one (bucket, status-class) row from spans_1m.
type statusBucketTimeseriesRow struct {
	BucketAt     time.Time `ch:"bucket_at"`
	StatusBucket string    `ch:"http_status_bucket"`
	RequestCount uint64    `ch:"request_count"`
}

// latencyPercentilesTimeseriesRow holds p50/p95/p99 for one display bucket.
type latencyPercentilesTimeseriesRow struct {
	BucketAt time.Time `ch:"bucket_at"`
	QS       []float32 `ch:"qs"`
	P50Ms    float32   `ch:"p50_ms"`
	P95Ms    float32   `ch:"p95_ms"`
	P99Ms    float32   `ch:"p99_ms"`
}

// topEndpointRow combines rate/error/percentile shape for one operation.
type topEndpointRow struct {
	ServiceName   string    `ch:"service"`
	OperationName string    `ch:"operation_name"`
	SpanKind      string    `ch:"kind_string"`
	HTTPRoute     string    `ch:"http_route"`
	TotalCount    uint64    `ch:"total_count"`
	ErrorCount    uint64    `ch:"error_count"`
	QS            []float32 `ch:"qs"`
	P50Ms         float32   `ch:"p50_ms"`
	P95Ms         float32   `ch:"p95_ms"`
	P99Ms         float32   `ch:"p99_ms"`
}

func (r *Repository) GetStatusTimeSeries(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]statusBucketTimeseriesRow, error) {
	grainSQL := timebucket.DisplayGrainSQL(endMs - startMs)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         ` + serviceResourcePred(serviceName) + `
		)
		SELECT ` + grainSQL + ` AS bucket_at,
		       http_status_bucket AS http_status_bucket,
		       sum(request_count) AS request_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		WHERE 1=1
		      ` + serviceWherePred(serviceName) + `
		GROUP BY bucket_at, http_status_bucket
		ORDER BY bucket_at ASC`
	var rows []statusBucketTimeseriesRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetStatusTimeSeries",
		&rows, query, detailArgs(teamID, startMs, endMs, serviceName)...)
}

func (r *Repository) GetLatencyPercentilesTimeSeries(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]latencyPercentilesTimeseriesRow, error) {
	grainSQL := timebucket.DisplayGrainSQL(endMs - startMs)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         ` + serviceResourcePred(serviceName) + `
		)
		SELECT ` + grainSQL + ` AS bucket_at,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		WHERE 1=1
		      ` + serviceWherePred(serviceName) + `
		GROUP BY bucket_at
		ORDER BY bucket_at ASC`
	var rows []latencyPercentilesTimeseriesRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetLatencyPercentilesTimeSeries",
		&rows, query, detailArgs(teamID, startMs, endMs, serviceName)...); err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 3 {
			rows[i].P50Ms = rows[i].QS[0]
			rows[i].P95Ms = rows[i].QS[1]
			rows[i].P99Ms = rows[i].QS[2]
		}
	}
	return rows, nil
}

func (r *Repository) GetTopEndpointsCombined(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursor TopEndpointsCursor,
) ([]topEndpointRow, error) {
	var paginationFilter string
	if !cursor.IsZero() {
		paginationFilter = "AND (total_count < @cursorCount OR (total_count = @cursorCount AND operation_name > @cursorOp))"
	}

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         ` + serviceResourcePred(serviceName) + `
		)
		SELECT service                                              AS service,
		       name                                                 AS operation_name,
		       any(kind_string)                                     AS kind_string,
		       any(http_route)                                      AS http_route,
		       sum(request_count)                                   AS total_count,
		       sum(error_count)                                     AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND timestamp   BETWEEN @start AND @end
		WHERE 1=1
		      ` + serviceWherePred(serviceName) + `
		GROUP BY service, name
		HAVING 1 = 1 ` + paginationFilter + `
		ORDER BY total_count DESC, operation_name ASC
		LIMIT @limit`
	args := append(detailArgs(teamID, startMs, endMs, serviceName),
		clickhouse.Named("limit", limit),
		clickhouse.Named("cursorCount", cursor.TotalCount),
		clickhouse.Named("cursorOp", cursor.OperationName),
	)
	var rows []topEndpointRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetTopEndpointsCombined",
		&rows, query, args...); err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 3 {
			rows[i].P50Ms = rows[i].QS[0]
			rows[i].P95Ms = rows[i].QS[1]
			rows[i].P99Ms = rows[i].QS[2]
		}
	}
	return rows, nil
}

func serviceResourcePred(serviceName string) string {
	if serviceName == "" {
		return ""
	}
	return "AND service = @serviceName"
}

func serviceWherePred(serviceName string) string {
	if serviceName == "" {
		return ""
	}
	return "AND service = @serviceName"
}

func detailArgs(teamID int64, startMs, endMs int64, serviceName string) []any {
	args := chargs.RangeArgs(teamID, startMs, endMs)
	if serviceName != "" {
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	return args
}
