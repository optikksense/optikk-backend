package redmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

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

func (r *ClickHouseRepository) GetStatusTimeSeries(
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
		WHERE timestamp BETWEEN @start AND @end
		      ` + serviceWherePred(serviceName) + `
		GROUP BY bucket_at, http_status_bucket
		ORDER BY bucket_at ASC`
	var rows []statusBucketTimeseriesRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetStatusTimeSeries",
		&rows, query, detailArgs(teamID, startMs, endMs, serviceName)...)
}

func (r *ClickHouseRepository) GetLatencyPercentilesTimeSeries(
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
		WHERE timestamp BETWEEN @start AND @end
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

func (r *ClickHouseRepository) GetTopEndpointsCombined(
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
		WHERE timestamp BETWEEN @start AND @end
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
	args := spanArgs(teamID, startMs, endMs)
	if serviceName != "" {
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	return args
}
