package errors

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

// --- Service error rate ---

func (r *Repository) ServiceErrorRateRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawServiceRateRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                  AS service,
		       ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       sum(request_count)       AS request_count,
		       sum(error_count)         AS error_count,
		       sum(duration_ms_sum)     AS duration_ms_sum
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service, bucket_at
		ORDER BY bucket_at ASC
		LIMIT 10000`
	args := chargs.RangeArgs(teamID, startMs, endMs)
	var rows []rawServiceRateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ServiceErrorRateAll", &rows, query, args...)
}

func (r *Repository) ServiceErrorRateRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawServiceRateRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT service                  AS service,
		       ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       sum(request_count)       AS request_count,
		       sum(error_count)         AS error_count,
		       sum(duration_ms_sum)     AS duration_ms_sum
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service, bucket_at
		ORDER BY bucket_at ASC
		LIMIT 10000`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
	)
	var rows []rawServiceRateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ServiceErrorRateByService", &rows, query, args...)
}

// --- Error volume ---

func (r *Repository) ErrorVolumeRowsAll(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawServiceErrorRow, error) {
	query := `
		SELECT service              AS service,
		       ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       sum(error_count)     AS error_count
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service, bucket_at
		ORDER BY bucket_at ASC
		LIMIT 10000`
	args := chargs.RangeArgs(teamID, startMs, endMs)
	var rows []rawServiceErrorRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorVolumeAll", &rows, query, args...)
}

func (r *Repository) ErrorVolumeRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]rawServiceErrorRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND service = @serviceName
		)
		SELECT service              AS service,
		       ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       sum(error_count)     AS error_count
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY service, bucket_at
		ORDER BY bucket_at ASC
		LIMIT 10000`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
	)
	var rows []rawServiceErrorRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorVolumeByService", &rows, query, args...)
}

// --- Error groups ---

func (r *Repository) ErrorGroupRowsAll(ctx context.Context, teamID int64, startMs, endMs int64, limit int, cursor ErrorGroupsCursor) ([]rawErrorGroupRow, error) {
	var paginationFilter string
	if !cursor.IsZero() {
		paginationFilter = "AND (error_count < @cursorCount OR (error_count = @cursorCount AND error_group_id > @cursorID))"
	}

	query := `
		SELECT error_group_id                   AS error_group_id,
		       service                          AS service,
		       name                             AS operation_name,
		       http_status_bucket               AS http_status_bucket,
		       sum(error_count)                 AS error_count,
		       max(timestamp)                   AS last_occurrence,
		       min(timestamp)                   AS first_occurrence
		FROM observability.spans_errors_1m
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY error_group_id, service, name, http_status_bucket
		HAVING error_count > 0 ` + paginationFilter + `
		ORDER BY error_count DESC, error_group_id ASC
		LIMIT @limit`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("limit", limit),
		clickhouse.Named("cursorCount", cursor.ErrorCount),
		clickhouse.Named("cursorID", cursor.GroupID),
	)
	var rows []rawErrorGroupRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupsAll", &rows, query, args...)
}

func (r *Repository) ErrorGroupRowsByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursor ErrorGroupsCursor) ([]rawErrorGroupRow, error) {
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
		       http_status_bucket               AS http_status_bucket,
		       sum(error_count)                 AS error_count,
		       max(timestamp)                   AS last_occurrence,
		       min(timestamp)                   AS first_occurrence
		FROM observability.spans_errors_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY error_group_id, service, name, http_status_bucket
		HAVING error_count > 0 ` + paginationFilter + `
		ORDER BY error_count DESC, error_group_id ASC
		LIMIT @limit`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("limit", limit),
		clickhouse.Named("cursorCount", cursor.ErrorCount),
		clickhouse.Named("cursorID", cursor.GroupID),
	)
	var rows []rawErrorGroupRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupsByService", &rows, query, args...)
}

// ErrorGroupSamples returns status message and trace ID exemplars for groups.
func (r *Repository) ErrorGroupSamples(ctx context.Context, teamID int64, startMs, endMs int64, groupIDs []string) ([]rawErrorGroupSampleRow, error) {
	const query = `
		SELECT error_group_id                    AS error_group_id,
		       argMax(status_message, timestamp) AS status_message,
		       argMax(trace_id, timestamp)       AS sample_trace_id
		FROM observability.spans
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE is_error = 1
		  AND timestamp BETWEEN @start AND @end
		  AND error_group_id IN @groupIDs
		GROUP BY error_group_id`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("groupIDs", groupIDs),
	)
	var rows []rawErrorGroupSampleRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupSamples", &rows, query, args...)
}

// --- Group drill-in (always scoped by GroupIdentity) ---

func (r *Repository) ErrorGroupDetailRow(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*rawErrorGroupDetailRow, error) {
	const query = `
		SELECT error_group_id                       AS error_group_id,
		       service                              AS service,
		       name                                 AS operation_name,
		       toUInt16OrZero(any(response_status_code)) AS http_status_code,
		       sum(error_count)                          AS error_count,
		       max(timestamp)                       AS last_occurrence,
		       min(timestamp)                       AS first_occurrence,
		       any(exception_type)                  AS exception_type
		FROM observability.spans_errors_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND error_group_id = @groupID
		GROUP BY error_group_id, service, name`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("groupID", groupID),
	)
	var row rawErrorGroupDetailRow
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupDetail", &row, query, args...); err != nil {
		return nil, err
	}
	return &row, nil
}

func (r *Repository) ErrorGroupTraceRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int, cursor ErrorTracesCursor) ([]rawErrorGroupTraceRow, error) {
	var paginationFilter string
	if !cursor.IsZero() {
		paginationFilter = "AND (s.timestamp < @cursorTs OR (s.timestamp = @cursorTs AND s.span_id > @cursorSpan))"
	}

	query := `
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
		  AND s.error_group_id = @groupID ` + paginationFilter + `
		ORDER BY s.timestamp DESC, s.span_id ASC
		LIMIT @limit`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupID", groupID),
		clickhouse.Named("limit", limit),
		clickhouse.Named("cursorTs", cursor.Timestamp),
		clickhouse.Named("cursorSpan", cursor.SpanID),
	}
	var rows []rawErrorGroupTraceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupTraces", &rows, query, args...)
}

func (r *Repository) ErrorGroupTimeseriesRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]rawTimeBucketCountRow, error) {
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       sum(error_count)                   AS count
		FROM observability.spans_errors_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND error_group_id = @groupID
		GROUP BY bucket_at
		HAVING count > 0
		ORDER BY bucket_at ASC`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)),
		clickhouse.Named("groupID", groupID),
	}
	var rows []rawTimeBucketCountRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupTimeseries", &rows, query, args...)
}

// ErrorGroupLatestOccurrenceRow returns the latest error span of the group.
func (r *Repository) ErrorGroupLatestOccurrenceRow(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*rawErrorLatestOccurrenceRow, error) {
	const query = `
		SELECT s.trace_id                  AS trace_id,
		       s.span_id                   AS span_id,
		       s.timestamp                 AS timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.exception_message         AS exception_message,
		       s.exception_stacktrace      AS exception_stacktrace,
		       s.http_method               AS http_method,
		       s.http_route                AS http_route,
		       s.response_status_code      AS response_status_code,
		       s.service_version           AS service_version,
		       s.environment               AS environment,
		       s.pod                       AS pod,
		       s.host                      AS host
		FROM observability.spans s
		PREWHERE s.team_id     = @teamID
		     AND s.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE s.is_error = 1
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.error_group_id = @groupID
		ORDER BY s.timestamp DESC
		LIMIT 1`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("groupID", groupID),
	)
	var row rawErrorLatestOccurrenceRow
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupLatestOccurrence", &row, query, args...); err != nil {
		return nil, err
	}
	return &row, nil
}

// ErrorGroupFacetRows returns error distribution across a single tag dimension.
func (r *Repository) ErrorGroupFacetRows(ctx context.Context, teamID int64, startMs, endMs int64, groupID, column string) ([]rawErrorFacetRow, error) {
	query := `
		SELECT ` + column + `             AS value,
		       sum(error_count)           AS count
		FROM observability.spans_errors_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND error_group_id = @groupID
		  AND ` + column + ` != ''
		GROUP BY value
		HAVING count > 0
		ORDER BY count DESC
		LIMIT 8`
	args := append(chargs.RangeArgs(teamID, startMs, endMs),
		clickhouse.Named("groupID", groupID),
	)
	var rows []rawErrorFacetRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorGroupFacet", &rows, query, args...)
}

// --- Error hotspot (no service filter) ---

func (r *Repository) ErrorHotspotRows(ctx context.Context, teamID int64, startMs, endMs int64) ([]rawErrorHotspotRow, error) {
	query := `
		WITH error_groups AS (
		    SELECT service,
		           name,
		           argMax(error_group_id, group_error_count) AS error_group_id,
		           sum(group_error_count)                    AS error_count
		    FROM (
		        SELECT service, name, error_group_id, sum(error_count) AS group_error_count
		        FROM observability.spans_errors_1m
		        PREWHERE team_id   = @teamID
		             AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		        WHERE timestamp BETWEEN @start AND @end
		          AND name != ''
		        GROUP BY service, name, error_group_id
		    )
		    GROUP BY service, name
		),
		totals AS (
		    SELECT service, name, sum(request_count) AS total_count
		    FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    WHERE timestamp BETWEEN @start AND @end
		      AND name != ''
		    GROUP BY service, name
		)
		SELECT g.service        AS service,
		       g.name           AS operation_name,
		       g.error_group_id AS error_group_id,
		       g.error_count    AS error_count,
		       t.total_count    AS total_count
		FROM error_groups g
		LEFT JOIN totals t ON g.service = t.service AND g.name = t.name
		ORDER BY error_count DESC
		LIMIT 500`
	args := chargs.RangeArgs(teamID, startMs, endMs)
	var rows []rawErrorHotspotRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ErrorHotspot", &rows, query, args...)
}
