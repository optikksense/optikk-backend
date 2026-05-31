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
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]requestRateRawRow, error)
	GetStatusTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]statusBucketTimeseriesRow, error)
	GetLatencyPercentilesTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyPercentilesTimeseriesRow, error)
	GetTopEndpointsCombined(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursor TopEndpointsCursor) ([]topEndpointRow, error)
	GetServiceREDMetrics(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*redSummaryServiceRow, error)
	GetServiceSaturationAggs(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, metricNames []string) ([]serviceMetricRow, error)
	GetSaturationTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, metricNames []string) ([]serviceMetricTimeseriesRow, error)
	GetFleetSaturationAggs(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]serviceMetricRow, error)
	GetOperationBaseline(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) (operationBaselineRow, error)
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
		SELECT service                                              AS service,
		       sum(request_count)                                   AS total_count,
		       sum(error_count)                                     AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service`
	var rows []redSummaryServiceRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetSummary",
		&rows, query, spanArgs(teamID, startMs, endMs)...); err != nil {
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

func (r *ClickHouseRepository) GetServiceREDMetrics(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*redSummaryServiceRow, error) {
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
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @serviceName
		GROUP BY service`
	var rows []redSummaryServiceRow
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

func (r *ClickHouseRepository) GetOperationBaseline(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) (operationBaselineRow, error) {
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
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @serviceName
		  AND name    = @operationName`
	args := append(spanArgs(teamID, startMs, endMs),
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

func (r *ClickHouseRepository) GetServiceSaturationAggs(
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
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name IN @metricNames
		         AND (service = @serviceName OR host IN service_hosts)
		)
		SELECT
		    service,
		    metric_name,
		    sum(val_sum) / sum(val_count) AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND (service = @serviceName OR host IN service_hosts)
		GROUP BY service, metric_name`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("metricNames", metricNames),
	)
	var rows []serviceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetServiceSaturationAggs",
		&rows, query, args...)
}

type requestRateRawRow struct {
	TsBucket     uint32 `ch:"ts_bucket"`
	ServiceName  string `ch:"service"`
	RequestCount uint64 `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]requestRateRawRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT ts_bucket             AS ts_bucket,
		       service               AS service,
		       sum(request_count)    AS request_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY ts_bucket, service
		ORDER BY ts_bucket ASC`
	var rows []requestRateRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetRequestRateTimeSeries",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}


func (r *ClickHouseRepository) GetSaturationTimeSeries(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, metricNames []string,
) ([]serviceMetricTimeseriesRow, error) {
	grainSQL := timebucket.DisplayGrainSQL(endMs - startMs)
	query := `
		WITH service_hosts AS (
		    SELECT DISTINCT host
		    FROM observability.spans_1m
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND service     = @serviceName
		         AND host        != ''
		),
		active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name IN @metricNames
		         AND (service = @serviceName OR host IN service_hosts)
		)
		SELECT ` + grainSQL + ` AS bucket_at,
		       metric_name,
		       sum(val_sum) / sum(val_count) AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND (service = @serviceName OR host IN service_hosts)
		GROUP BY bucket_at, metric_name
		ORDER BY bucket_at ASC`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("metricNames", metricNames),
	)
	var rows []serviceMetricTimeseriesRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetSaturationTimeSeries",
		&rows, query, args...)
}

func (r *ClickHouseRepository) GetFleetSaturationAggs(
	ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string,
) ([]serviceMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name IN @metricNames
		)
		SELECT
		    service,
		    metric_name,
		    sum(val_sum) / sum(val_count) AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service, metric_name`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("metricNames", metricNames),
	)
	var rows []serviceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetFleetSaturationAggs",
		&rows, query, args...)
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
