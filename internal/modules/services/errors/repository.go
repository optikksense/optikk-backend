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

	ErrorGroupRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]rawErrorGroupRow, error)
	ErrorGroupRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]rawErrorGroupRow, error)

	ErrorGroupDetailRow(ctx context.Context, teamID int64, startMs, endMs int64, ident GroupIdentity) (*rawErrorGroupDetailRow, error)
	ErrorGroupTraceRows(ctx context.Context, teamID int64, startMs, endMs int64, ident GroupIdentity, limit int) ([]rawErrorGroupTraceRow, error)
	ErrorGroupTimeseriesRows(ctx context.Context, teamID int64, startMs, endMs int64, ident GroupIdentity) ([]rawTimeBucketCountRow, error)

	ExceptionRateRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawExceptionRateRow, error)
	ExceptionRateRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawExceptionRateRow, error)

	ErrorHotspotRows(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawErrorHotspotRow, error)

	HTTP5xxByRouteRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawHTTP5xxRow, error)
	HTTP5xxByRouteRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawHTTP5xxRow, error)

	FingerprintRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]rawErrorFingerprintRow, error)
	FingerprintRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]rawErrorFingerprintRow, error)

	FingerprintTrendRows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]rawFingerprintTrendRow, error)
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
		       ts_bucket                AS timestamp,
		       sum(request_count)       AS request_count,
		       sum(error_count)         AS error_count,
		       sum(duration_ms_sum)     AS duration_ms_sum
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, timestamp
		ORDER BY timestamp ASC
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
		       ts_bucket                AS timestamp,
		       sum(request_count)       AS request_count,
		       sum(error_count)         AS error_count,
		       sum(duration_ms_sum)     AS duration_ms_sum
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, timestamp
		ORDER BY timestamp ASC
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
		       ts_bucket            AS timestamp,
		       sum(error_count)     AS error_count
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @start AND @end
		GROUP BY service, timestamp
		ORDER BY timestamp ASC
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
		       ts_bucket            AS timestamp,
		       sum(error_count)     AS error_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, timestamp
		ORDER BY timestamp ASC
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

func (r *ClickHouseRepository) ErrorGroupRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]rawErrorGroupRow, error) {
	const query = `
		SELECT service                          AS service,
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
		GROUP BY service, name, exception_type, status_message_hash, http_status_bucket
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	}
	var rows []rawErrorGroupRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupsAll", &rows, query, args...)
}

func (r *ClickHouseRepository) ErrorGroupRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]rawErrorGroupRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT service                          AS service,
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
		GROUP BY service, name, exception_type, status_message_hash, http_status_bucket
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("limit", limit),
	}
	var rows []rawErrorGroupRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupsByService", &rows, query, args...)
}

// --- Group drill-in (always scoped by GroupIdentity) ---

func (r *ClickHouseRepository) ErrorGroupDetailRow(ctx context.Context, teamID int64, startMs, endMs int64, ident GroupIdentity) (*rawErrorGroupDetailRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @groupServiceName
		)
		SELECT service                              AS service,
		       name                                 AS operation_name,
		       any(sample_status_message)           AS status_message,
		       toInt64(toUInt16OrZero(any(response_status_code))) AS http_status_code,
		       toInt64(sum(error_count))            AS error_count,
		       max(timestamp)                       AS last_occurrence,
		       min(timestamp)                       AS first_occurrence,
		       any(sample_trace_id)                 AS sample_trace_id,
		       any(exception_type)                  AS exception_type,
		       any(sample_exception_stacktrace)     AS stack_trace
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @groupServiceName
		  AND name = @groupOperationName
		  AND status_message_hash = cityHash64(@groupStatusMessage)
		  AND toUInt16OrZero(response_status_code) = @groupHTTPStatusCode
		GROUP BY service, name, status_message_hash`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupServiceName", ident.Service),
		clickhouse.Named("groupOperationName", ident.Operation),
		clickhouse.Named("groupStatusMessage", ident.StatusMessage),
		clickhouse.Named("groupHTTPStatusCode", ident.HTTPCode),
	}
	var row rawErrorGroupDetailRow
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupDetail", &row, query, args...); err != nil {
		return nil, err
	}
	return &row, nil
}

func (r *ClickHouseRepository) ErrorGroupTraceRows(ctx context.Context, teamID int64, startMs, endMs int64, ident GroupIdentity, limit int) ([]rawErrorGroupTraceRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @groupServiceName
		)
		SELECT s.trace_id                       AS trace_id,
		       s.span_id                        AS span_id,
		       s.timestamp                      AS timestamp,
		       s.duration_nano / 1000000.0      AS duration_ms,
		       s.status_code_string             AS status_code
		FROM observability.spans s
		PREWHERE s.team_id     = @teamID
		     AND s.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND s.fingerprint IN active_fps
		WHERE s.is_error = 1
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND s.http_status_code = @groupHTTPStatusCode
		ORDER BY s.timestamp DESC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupServiceName", ident.Service),
		clickhouse.Named("groupOperationName", ident.Operation),
		clickhouse.Named("groupStatusMessage", ident.StatusMessage),
		clickhouse.Named("groupHTTPStatusCode", ident.HTTPCode),
		clickhouse.Named("limit", limit),
	}
	var rows []rawErrorGroupTraceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupTraces", &rows, query, args...)
}

func (r *ClickHouseRepository) ErrorGroupTimeseriesRows(ctx context.Context, teamID int64, startMs, endMs int64, ident GroupIdentity) ([]rawTimeBucketCountRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @groupServiceName
		)
		SELECT toDateTime(ts_bucket)              AS timestamp,
		       toUInt64(sum(error_count))         AS count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @groupServiceName
		  AND name = @groupOperationName
		  AND status_message_hash = cityHash64(@groupStatusMessage)
		  AND toUInt16OrZero(response_status_code) = @groupHTTPStatusCode
		GROUP BY timestamp
		HAVING count > 0
		ORDER BY timestamp ASC`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupServiceName", ident.Service),
		clickhouse.Named("groupOperationName", ident.Operation),
		clickhouse.Named("groupStatusMessage", ident.StatusMessage),
		clickhouse.Named("groupHTTPStatusCode", ident.HTTPCode),
	}
	var rows []rawTimeBucketCountRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupTimeseries", &rows, query, args...)
}

// --- Exception rate by type ---

func (r *ClickHouseRepository) ExceptionRateRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawExceptionRateRow, error) {
	const query = `
		SELECT ts_bucket          AS time_bucket,
		       exception_type     AS exception_type,
		       sum(error_count)   AS event_count
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @start AND @end
		WHERE exception_type != ''
		GROUP BY time_bucket, exception_type
		ORDER BY time_bucket ASC`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	var rows []rawExceptionRateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ExceptionRateAll", &rows, query, args...)
}

func (r *ClickHouseRepository) ExceptionRateRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawExceptionRateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT ts_bucket          AS time_bucket,
		       exception_type     AS exception_type,
		       sum(error_count)   AS event_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		  AND exception_type != ''
		GROUP BY time_bucket, exception_type
		ORDER BY time_bucket ASC`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
	}
	var rows []rawExceptionRateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ExceptionRateByService", &rows, query, args...)
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

// --- HTTP 5xx by route ---

func (r *ClickHouseRepository) HTTP5xxByRouteRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawHTTP5xxRow, error) {
	const query = `
		SELECT http_route                  AS http_route,
		       service                     AS service,
		       toInt64(sum(request_count)) AS count_5xx
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND http_status_bucket = '5xx'
		GROUP BY http_route, service
		ORDER BY count_5xx DESC
		LIMIT 100`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
	}
	var rows []rawHTTP5xxRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.HTTP5xxByRouteAll", &rows, query, args...)
}

func (r *ClickHouseRepository) HTTP5xxByRouteRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawHTTP5xxRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT http_route                  AS http_route,
		       service                     AS service,
		       toInt64(sum(request_count)) AS count_5xx
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND http_status_bucket = '5xx'
		GROUP BY http_route, service
		ORDER BY count_5xx DESC
		LIMIT 100`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
	}
	var rows []rawHTTP5xxRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.HTTP5xxByRouteByService", &rows, query, args...)
}

// --- Fingerprint list ---

func (r *ClickHouseRepository) FingerprintRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]rawErrorFingerprintRow, error) {
	const query = `
		SELECT toString(status_message_hash)        AS fingerprint,
		       service                              AS service,
		       name                                 AS operation_name,
		       exception_type                       AS exception_type,
		       any(sample_status_message)           AS status_message,
		       min(timestamp)                       AS first_seen,
		       max(timestamp)                       AS last_seen,
		       sum(error_count)                     AS cnt,
		       any(sample_trace_id)                 AS sample_trace_id
		FROM observability.spans_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @start AND @end
		GROUP BY service, name, exception_type, status_message_hash, http_status_bucket
		HAVING cnt > 0
		ORDER BY cnt DESC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	}
	var rows []rawErrorFingerprintRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.FingerprintsAll", &rows, query, args...)
}

func (r *ClickHouseRepository) FingerprintRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]rawErrorFingerprintRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT toString(status_message_hash)        AS fingerprint,
		       service                              AS service,
		       name                                 AS operation_name,
		       exception_type                       AS exception_type,
		       any(sample_status_message)           AS status_message,
		       min(timestamp)                       AS first_seen,
		       max(timestamp)                       AS last_seen,
		       sum(error_count)                     AS cnt,
		       any(sample_trace_id)                 AS sample_trace_id
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		GROUP BY service, name, exception_type, status_message_hash, http_status_bucket
		HAVING cnt > 0
		ORDER BY cnt DESC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("limit", limit),
	}
	var rows []rawErrorFingerprintRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.FingerprintsByService", &rows, query, args...)
}

// --- Fingerprint trend (serviceName mandatory at handler boundary) ---

func (r *ClickHouseRepository) FingerprintTrendRows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]rawFingerprintTrendRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT ts_bucket                AS ts,
		       sum(error_count)         AS cnt
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE ts_bucket BETWEEN @start AND @end
		  AND name = @operationName
		  AND exception_type = @exceptionType
		  AND status_message_hash = cityHash64(@statusMessage)
		GROUP BY ts
		HAVING cnt > 0
		ORDER BY ts ASC`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("exceptionType", exceptionType),
		clickhouse.Named("statusMessage", statusMessage),
	}
	var rows []rawFingerprintTrendRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.FingerprintTrend", &rows, query, args...)
}
