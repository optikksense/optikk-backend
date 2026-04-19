package errors

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	utils "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

// Error-group drill-in (detail + traces + timeseries) requires resolving the
// groupID back to its identity fields. Each call used to aggregate the full
// group list independently; cache per (teamID, rounded time window) so one
// CH aggregation serves all three follow-up calls for up to 60 s.
const groupResolveCacheTTL = 60 * time.Second

type groupResolveEntry struct {
	groups    []errorGroupRow
	expiresAt time.Time
}

var (
	groupResolveMu    sync.Mutex
	groupResolveStore = make(map[string]groupResolveEntry)
)

func groupResolveKey(teamID int64, startMs, endMs int64) string {
	// Bucket the window to the minute so slight timestamp drift still hits.
	return fmt.Sprintf("%d|%d|%d", teamID, startMs/60_000, endMs/60_000)
}

func errorBucketExpr(startMs, endMs int64) string {
	return utils.ExprForColumnTime(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetServiceErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]serviceErrorRateRow, error)
	GetErrorVolume(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorVolumeRow, error)
	GetLatencyDuringErrorWindows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyErrorRow, error)
	GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error)
	GetErrorGroupDetail(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*errorGroupDetailRow, error)
	GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]errorGroupTraceRow, error)
	GetErrorGroupTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]errorGroupTSRow, error)

	// Migrated from errortracking
	GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRatePointDTO, error)
	GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotCellDTO, error)
	GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteDTO, error)

	// Migrated from errorfingerprint
	ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorFingerprintDTO, error)
	GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]fingerprintTrendPointDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// errorGroupRow is the DTO for scanning error group rows from ClickHouse.
// GroupID is computed in Go from the other fields, so we use a DTO (Rule B).
type errorGroupRow struct {
	ServiceName     string    `ch:"service_name"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      int64     `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	SampleTraceID   string    `ch:"sample_trace_id"`
}

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	query := `
		SELECT s.service_name AS service_name,
		       s.name         AS operation_name,
		       s.status_message,
		       toUInt16OrZero(s.response_status_code) AS http_status_code,
		       toInt64(COUNT(*)) as error_count,
		       MAX(s.timestamp) as last_occurrence,
		       MIN(s.timestamp) as first_occurrence,
		       (groupArray(s.trace_id) as trace_ids)[1] as sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `) AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY s.service_name, s.name, s.status_message, toUInt16OrZero(s.response_status_code)
	           ORDER BY error_count DESC LIMIT @limit`
	args = append(args, clickhouse.Named("limit", limit))

	var rows []errorGroupRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// resolveGroupID finds the error group matching the given groupID hash.
// Results are cached briefly so the detail/traces/timeseries calls that
// follow a drill-in share a single backing CH aggregation.
func (r *ClickHouseRepository) resolveGroupID(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (service, operation, statusMessage string, httpCode int, err error) {
	groups, err := r.cachedErrorGroups(ctx, teamID, startMs, endMs)
	if err != nil {
		return "", "", "", 0, err
	}
	for _, g := range groups {
		code := int(g.HTTPStatusCode)
		if ErrorGroupID(g.ServiceName, g.OperationName, g.StatusMessage, code) == groupID {
			return g.ServiceName, g.OperationName, g.StatusMessage, code, nil
		}
	}
	return "", "", "", 0, fmt.Errorf("error group %s not found", groupID)
}

// cachedErrorGroups returns a short-TTL cached group list for the given
// window, fetching from ClickHouse only on miss/expiry.
func (r *ClickHouseRepository) cachedErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorGroupRow, error) {
	key := groupResolveKey(teamID, startMs, endMs)

	groupResolveMu.Lock()
	entry, ok := groupResolveStore[key]
	groupResolveMu.Unlock()
	if ok && time.Now().Before(entry.expiresAt) {
		return entry.groups, nil
	}

	groups, err := r.GetErrorGroups(ctx, teamID, startMs, endMs, "", 500)
	if err != nil {
		return nil, err
	}

	groupResolveMu.Lock()
	groupResolveStore[key] = groupResolveEntry{groups: groups, expiresAt: time.Now().Add(groupResolveCacheTTL)}
	// Drop stale entries so the map doesn't grow unbounded across windows.
	if len(groupResolveStore) > 256 {
		now := time.Now()
		for k, e := range groupResolveStore {
			if now.After(e.expiresAt) {
				delete(groupResolveStore, k)
			}
		}
	}
	groupResolveMu.Unlock()

	return groups, nil
}

// errorGroupDetailRow is the DTO for GetErrorGroupDetail.
type errorGroupDetailRow struct {
	ServiceName     string    `ch:"service_name"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      int64     `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	SampleTraceID   string    `ch:"sample_trace_id"`
	ExceptionType   string    `ch:"exception_type"`
	StackTrace      string    `ch:"stack_trace"`
}

func (r *ClickHouseRepository) GetErrorGroupDetail(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*errorGroupDetailRow, error) {
	svc, op, msg, code, err := r.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.service_name, s.name AS operation_name, s.status_message,
		       toUInt16OrZero(s.response_status_code) AS http_status_code,
		       toInt64(COUNT(*)) AS error_count,
		       MAX(s.timestamp) AS last_occurrence,
		       MIN(s.timestamp) AS first_occurrence,
		       (groupArray(s.trace_id))[1] AS sample_trace_id,
		       any(s.exception_type) AS exception_type,
		       any(s.exception_stacktrace) AS stack_trace
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND toUInt16OrZero(s.response_status_code) = @groupHTTPStatusCode
		GROUP BY s.service_name, s.name, s.status_message, toUInt16OrZero(s.response_status_code)`

	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", code),
	)

	var row errorGroupDetailRow
	if err := r.db.QueryRow(database.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return nil, err
	}

	return &row, nil
}

// errorGroupTraceRow is the DTO for GetErrorGroupTraces.
type errorGroupTraceRow struct {
	TraceID    string    `ch:"trace_id"`
	SpanID     string    `ch:"span_id"`
	Timestamp  time.Time `ch:"timestamp"`
	DurationMs float64   `ch:"duration_ms"`
	StatusCode string    `ch:"status_code"`
}

func (r *ClickHouseRepository) GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]errorGroupTraceRow, error) {
	svc, op, msg, code, err := r.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.trace_id, s.span_id, s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status_code
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND toUInt16OrZero(s.response_status_code) = @groupHTTPStatusCode
		ORDER BY s.timestamp DESC
		LIMIT @limit`

	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", code),
		clickhouse.Named("limit", limit),
	)

	var rows []errorGroupTraceRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// errorGroupTSRow is the DTO for GetErrorGroupTimeseries.
type errorGroupTSRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	ErrorCount int64     `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorGroupTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]errorGroupTSRow, error) {
	svc, op, msg, code, err := r.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       toInt64(COUNT(*)) AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (`+ErrorCondition()+`)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND toUInt16OrZero(s.response_status_code) = @groupHTTPStatusCode
		GROUP BY timestamp
		ORDER BY timestamp ASC`, bucket)

	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", code),
	)

	var rows []errorGroupTSRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// serviceErrorRateRow is the DTO for GetServiceErrorRate.
type serviceErrorRateRow struct {
	ServiceName  string    `ch:"service_name"`
	Timestamp    time.Time `ch:"timestamp"`
	RequestCount int64     `ch:"request_count"`
	ErrorCount   int64     `ch:"error_count"`
	ErrorRate    float64   `ch:"error_rate"`
	AvgLatency   float64   `ch:"avg_latency"`
}

func (r *ClickHouseRepository) GetServiceErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]serviceErrorRateRow, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       timestamp,
		       request_count,
		       error_count,
		       if(request_count > 0, error_count*100.0/request_count, 0) AS error_rate,
		       avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       toInt64(count())                 AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		ORDER BY timestamp ASC
		LIMIT 10000`, bucket)

	var rows []serviceErrorRateRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// errorVolumeRow is the DTO for GetErrorVolume.
type errorVolumeRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	ErrorCount  int64     `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorVolume(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorVolumeRow, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name, timestamp, error_count
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		WHERE error_count > 0
		ORDER BY timestamp ASC
		LIMIT 10000`, bucket)

	var rows []errorVolumeRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// latencyErrorRow is the DTO for GetLatencyDuringErrorWindows.
type latencyErrorRow struct {
	ServiceName  string    `ch:"service_name"`
	Timestamp    time.Time `ch:"timestamp"`
	RequestCount int64     `ch:"request_count"`
	ErrorCount   int64     `ch:"error_count"`
	AvgLatency   float64   `ch:"avg_latency"`
}

func (r *ClickHouseRepository) GetLatencyDuringErrorWindows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyErrorRow, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name, timestamp, request_count, error_count, avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       toInt64(count())                 AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		WHERE error_count > 0
		ORDER BY timestamp ASC
		LIMIT 10000`, bucket)

	var rows []latencyErrorRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// --- Migrated from errortracking ---

func (r *ClickHouseRepository) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRatePointDTO, error) {
	bucket := utils.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.exception_type AS exception_type,
		       toInt64(count()) AS event_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.exception_type != '' AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY time_bucket, exception_type ORDER BY time_bucket ASC`

	var rows []exceptionRatePointDTO
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}

func (r *ClickHouseRepository) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotCellDTO, error) {
	var rows []errorHotspotCellDTO
	err := r.db.Select(database.OverviewCtx(ctx), &rows, `
		SELECT s.service_name AS service_name,
		       s.name AS operation_name,
		       toInt64(count())                                                                    AS total_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.service_name, s.name
		ORDER BY error_rate DESC
		LIMIT 500
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteDTO, error) {
	query := `
		SELECT s.mat_http_route AS http_route,
		       s.service_name   AS service_name,
		       toInt64(count()) AS count_5xx
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND toUInt16OrZero(s.response_status_code) >= 500`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY http_route, s.service_name ORDER BY count_5xx DESC LIMIT 100`

	var rows []http5xxByRouteDTO
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}

// --- Migrated from errorfingerprint ---

func (r *ClickHouseRepository) ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorFingerprintDTO, error) {
	query := `
		SELECT hex(cityHash64(s.service_name, s.name, s.mat_exception_type, s.status_message)) AS fingerprint,
		       s.service_name,
		       s.name AS operation_name,
		       s.mat_exception_type AS exception_type,
		       s.status_message,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       count() AS cnt,
		       any(s.trace_id) AS sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN @start AND @end
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		utils.SpansBucketStart(startMs / 1000),
		utils.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += `
		GROUP BY s.service_name, s.name, s.mat_exception_type, s.status_message
		ORDER BY cnt DESC
		LIMIT ?`
	args = append(args, limit)

	var rows []errorFingerprintDTO
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}

func (r *ClickHouseRepository) GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]fingerprintTrendPointDTO, error) {
	bucket := utils.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS ts,
		       count() AS cnt
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN @start AND @end
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)
		  AND s.service_name = ?
		  AND s.name = ?
		  AND s.mat_exception_type = ?
		  AND s.status_message = ?
		GROUP BY ts
		ORDER BY ts ASC
	`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		utils.SpansBucketStart(startMs / 1000),
		utils.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		serviceName,
		operationName,
		exceptionType,
		statusMessage,
	}

	var rows []fingerprintTrendPointDTO
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}
