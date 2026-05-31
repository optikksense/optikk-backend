package errors

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type Repository interface {
	ServiceErrorRateRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawServiceRateRow, error)
	ServiceErrorRateRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawServiceRateRow, error)

	ErrorVolumeRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawServiceErrorRow, error)
	ErrorVolumeRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawServiceErrorRow, error)

	ErrorGroupRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int, cursor ErrorGroupsCursor) ([]rawErrorGroupRow, error)
	ErrorGroupRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursor ErrorGroupsCursor) ([]rawErrorGroupRow, error)

	ErrorGroupDetailRow(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*rawErrorGroupDetailRow, error)
	ErrorGroupTraceRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]rawErrorGroupTraceRow, error)
	ErrorGroupTimeseriesRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]rawTimeBucketCountRow, error)

	ErrorHotspotRows(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawErrorHotspotRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// --- Service error rate ---

func (r *ClickHouseRepository) ServiceErrorRateRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawServiceRateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                  AS service,
		       ts_bucket                AS ts_bucket,
		       sum(request_count)       AS request_count,
		       sum(error_count)         AS error_count,
		       sum(duration_ms_sum)     AS duration_ms_sum
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, ts_bucket
		ORDER BY ts_bucket ASC
		LIMIT 10000`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
	}
	var rows []rawServiceRateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ServiceErrorRateAll", &rows, query, args...)
}

func (r *ClickHouseRepository) ServiceErrorRateRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawServiceRateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT service                  AS service,
		       ts_bucket                AS ts_bucket,
		       sum(request_count)       AS request_count,
		       sum(error_count)         AS error_count,
		       sum(duration_ms_sum)     AS duration_ms_sum
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, ts_bucket
		ORDER BY ts_bucket ASC
		LIMIT 10000`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
	}
	var rows []rawServiceRateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ServiceErrorRateByService", &rows, query, args...)
}

// --- Error volume ---

func (r *ClickHouseRepository) ErrorVolumeRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawServiceErrorRow, error) {
	const query = `
		SELECT service              AS service,
		       ts_bucket            AS ts_bucket,
		       sum(error_count)     AS error_count
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @start AND @end
		GROUP BY service, ts_bucket
		ORDER BY ts_bucket ASC
		LIMIT 10000`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []rawServiceErrorRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorVolumeAll", &rows, query, args...)
}

func (r *ClickHouseRepository) ErrorVolumeRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawServiceErrorRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT service              AS service,
		       ts_bucket            AS ts_bucket,
		       sum(error_count)     AS error_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, ts_bucket
		ORDER BY ts_bucket ASC
		LIMIT 10000`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
	}
	var rows []rawServiceErrorRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorVolumeByService", &rows, query, args...)
}

// --- Error groups ---

func (r *ClickHouseRepository) ErrorGroupRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int, cursor ErrorGroupsCursor) ([]rawErrorGroupRow, error) {
	var paginationFilter string
	if !cursor.IsZero() {
		paginationFilter = "AND (error_count < @cursorCount OR (error_count = @cursorCount AND error_group_id > @cursorID))"
	}

	query := `
		SELECT error_group_id                   AS error_group_id,
		       service                          AS service,
		       name                             AS operation_name,
		       any(sample_status_message)       AS status_message,
		       http_status_bucket               AS http_status_bucket,
		       sum(error_count)                 AS error_count,
		       max(timestamp)                   AS last_occurrence,
		       min(timestamp)                   AS first_occurrence,
		       any(sample_trace_id)             AS sample_trace_id
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @start AND @end
		GROUP BY error_group_id, service, name, exception_type, status_message_hash, http_status_bucket
		HAVING error_count > 0 ` + paginationFilter + `
		ORDER BY error_count DESC, error_group_id ASC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
		clickhouse.Named("cursorCount", cursor.ErrorCount),
		clickhouse.Named("cursorID", cursor.GroupID),
	}
	var rows []rawErrorGroupRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupsAll", &rows, query, args...)
}

func (r *ClickHouseRepository) ErrorGroupRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursor ErrorGroupsCursor) ([]rawErrorGroupRow, error) {
	var paginationFilter string
	if !cursor.IsZero() {
		paginationFilter = "AND (error_count < @cursorCount OR (error_count = @cursorCount AND error_group_id > @cursorID))"
	}

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT error_group_id                   AS error_group_id,
		       service                          AS service,
		       name                             AS operation_name,
		       any(sample_status_message)       AS status_message,
		       http_status_bucket               AS http_status_bucket,
		       sum(error_count)                 AS error_count,
		       max(timestamp)                   AS last_occurrence,
		       min(timestamp)                   AS first_occurrence,
		       any(sample_trace_id)             AS sample_trace_id
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY error_group_id, service, name, exception_type, status_message_hash, http_status_bucket
		HAVING error_count > 0 ` + paginationFilter + `
		ORDER BY error_count DESC, error_group_id ASC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("limit", limit),
		clickhouse.Named("cursorCount", cursor.ErrorCount),
		clickhouse.Named("cursorID", cursor.GroupID),
	}
	var rows []rawErrorGroupRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupsByService", &rows, query, args...)
}

// --- Group drill-in (always scoped by GroupIdentity) ---

func (r *ClickHouseRepository) ErrorGroupDetailRow(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*rawErrorGroupDetailRow, error) {
	const query = `
		SELECT error_group_id                       AS error_group_id,
		       service                              AS service,
		       name                                 AS operation_name,
		       any(sample_status_message)           AS status_message,
		       toUInt16OrZero(any(response_status_code)) AS http_status_code,
		       sum(error_count)                          AS error_count,
		       max(timestamp)                       AS last_occurrence,
		       min(timestamp)                       AS first_occurrence,
		       any(sample_trace_id)                 AS sample_trace_id,
		       any(exception_type)                  AS exception_type,
		       any(sample_exception_stacktrace)     AS stack_trace
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND error_group_id = @groupID
		GROUP BY error_group_id, service, name`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupID", groupID),
	}
	var row rawErrorGroupDetailRow
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupDetail", &row, query, args...); err != nil {
		return nil, err
	}
	return &row, nil
}

func (r *ClickHouseRepository) ErrorGroupTraceRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]rawErrorGroupTraceRow, error) {
	const query = `
		SELECT s.trace_id                       AS trace_id,
		       s.span_id                        AS span_id,
		       s.timestamp                      AS timestamp,
		       s.duration_nano / 1000000.0      AS duration_ms,
		       s.status_code_string             AS status_code
		FROM observability.spans s
		PREWHERE s.team_id     = @teamID
		     AND s.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE s.is_error = 1
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.error_group_id = @groupID
		ORDER BY s.timestamp DESC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupID", groupID),
		clickhouse.Named("limit", limit),
	}
	var rows []rawErrorGroupTraceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupTraces", &rows, query, args...)
}

func (r *ClickHouseRepository) ErrorGroupTimeseriesRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]rawTimeBucketCountRow, error) {
	const query = `
		SELECT ts_bucket                          AS ts_bucket,
		       sum(error_count)                   AS count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND error_group_id = @groupID
		GROUP BY ts_bucket
		HAVING count > 0
		ORDER BY ts_bucket ASC`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupID", groupID),
	}
	var rows []rawTimeBucketCountRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupTimeseries", &rows, query, args...)
}


// --- Error hotspot (no service filter) ---

func (r *ClickHouseRepository) ErrorHotspotRows(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawErrorHotspotRow, error) {
	const query = `
		SELECT service                AS service,
		       name                   AS operation_name,
		       sum(error_count)       AS error_count,
		       sum(request_count)     AS total_count
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @start AND @end
		WHERE name != ''
		GROUP BY service, name
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT 500`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []rawErrorHotspotRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorHotspot", &rows, query, args...)
}


