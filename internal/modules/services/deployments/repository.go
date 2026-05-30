package deployments

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs ClickHouse queries for deployment detection. Reads
// observability.spans_1m for span aggregates (request/error counts, latency
// percentiles via quantileTimingMerge). Deployments are inferred from
// service_version changes in span telemetry — no separate dimension table.
type Repository interface {
	ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error)
	ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error)
	GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error)
	GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error)
	GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error)
	GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error)
	GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error)
	GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error)
	GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketSecs(startMs, endMs int64) float64 {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return 60.0
	case h <= 12:
		return 300.0
	default:
		return 3600.0
	}
}

func (r *ClickHouseRepository) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT service                                                            AS service,
		       service_version                                                    AS version,
		       environment                                                        AS environment,
		       min(timestamp)                                                     AS first_seen,
		       max(timestamp)                                                     AS last_seen,
		       sum(request_count)                                                 AS span_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service, version, environment
		ORDER BY first_seen ASC`
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	now := time.Now()
	start := now.Add(-90 * 24 * time.Hour)
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT service                                                            AS service,
		       service_version                                                    AS version,
		       environment                                                        AS environment,
		       min(timestamp)                                                     AS first_seen,
		       max(timestamp)                                                     AS last_seen,
		       sum(request_count)                                                 AS span_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service, version, environment
		ORDER BY first_seen ASC`
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListServiceDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(start.Unix())),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(now.Unix())+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", start),
		clickhouse.Named("end", now),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	now := time.Now()
	start := now.Add(-30 * 24 * time.Hour)
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                                            AS service,
		       service_version                                                    AS version,
		       environment                                                        AS environment,
		       min(timestamp)                                                     AS first_seen,
		       max(timestamp)                                                     AS last_seen,
		       sum(request_count)                                                 AS span_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service, version, environment
		ORDER BY service ASC, first_seen DESC
		LIMIT 1 BY service`
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetLatestDeploymentsByService", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.BucketStart(start.Unix())),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(now.Unix())+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", start),
		clickhouse.Named("end", now),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                                            AS service,
		       service_version                                                    AS version,
		       environment                                                        AS environment,
		       min(timestamp)                                                     AS first_seen,
		       max(timestamp)                                                     AS last_seen,
		       sum(request_count)                                                 AS span_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service, version, environment
		ORDER BY first_seen ASC`
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetDeploysInRange", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT ts_bucket                                                   AS ts_bucket,
		       service_version                                             AS version,
		       sum(request_count) / @bucketSeconds                         AS rps
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY ts_bucket, version
		ORDER BY ts_bucket ASC, version ASC`
	var rows []versionTrafficRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetVersionTraffic", &rows, query,
		clickhouse.Named("bucketSeconds", bucketSecs(startMs, endMs)),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil {
		return nil, err
	}
	out := make([]VersionTrafficPoint, len(rows))
	for i, row := range rows {
		out[i] = VersionTrafficPoint{
			Timestamp: timebucket.BucketTime(row.TsBucket),
			Version:   row.Version,
			RPS:       row.RPS,
		}
	}
	return out, nil
}

type versionTrafficRow struct {
	TsBucket uint32  `ch:"ts_bucket"`
	Version  string  `ch:"version"`
	RPS      float64 `ch:"rps"`
}

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT sum(request_count)                                                 AS request_count,
		       sum(error_count)                                                   AS error_count,
		       quantilesTimingMerge(0.95, 0.99)(latency_state)                    AS qs
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end`
	var row impactAggRow
	err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetImpactWindow", &row, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil {
		return impactAggRow{}, err
	}
	if len(row.QS) >= 2 {
		row.P95Ms = row.QS[0]
		row.P99Ms = row.QS[1]
	}
	return row, nil
}

func (r *ClickHouseRepository) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT service_version                                             AS version,
		       environment                                                 AS environment
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND is_root = 1
		  AND service_version != ''
		GROUP BY version, environment
		ORDER BY max(timestamp) DESC
		LIMIT 1`
	var rows []activeVersionRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetActiveVersion", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil || len(rows) == 0 {
		return activeVersionRow{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT service                                                    AS service,
		       hex(status_message_hash)                                   AS group_id,
		       name                                                       AS operation_name,
		       any(sample_status_message)                                 AS status_message,
		       multiIf(http_status_bucket = '5xx', 500,
		               http_status_bucket = '4xx', 400,
		               0)                                                 AS http_status_code,
		       sum(error_count)                                           AS error_count,
		       max(timestamp)                                             AS last_occurrence,
		       any(sample_trace_id)                                       AS sample_trace_id
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service, status_message_hash, name, http_status_bucket
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT @limit`
	var rows []errorGroupAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetErrorGroupsWindow", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service = @serviceName
		)
		SELECT name                                                       AS operation_name,
		       name                                                       AS endpoint_name,
		       http_method                                                AS http_method,
		       sum(request_count)                                         AS request_count,
		       sum(error_count)                                           AS error_count,
		       quantilesTimingMerge(0.95, 0.99)(latency_state)            AS qs
		FROM observability.spans_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY name, http_method
		ORDER BY request_count DESC
		LIMIT @limit`
	var rows []endpointMetricAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetEndpointMetricsWindow", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 2 {
			rows[i].P95Ms = rows[i].QS[0]
			rows[i].P99Ms = rows[i].QS[1]
		}
	}
	return rows, nil
}
