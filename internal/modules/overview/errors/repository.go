// Package errors powers the Overview > Errors panels. Aggregate methods
// (GetServiceErrorRate, GetErrorVolume, GetExceptionRateByType, GetErrorHotspot,
// ListFingerprints, GetFingerprintTrend) read `spans_rollup` or
// `spans_error_fingerprint`. Drill-down methods (GetErrorGroupDetail,
// GetErrorGroupTraces, GetErrorGroupTimeseries, GetHTTP5xxByRoute) stay on
// raw `observability.signoz_index_v3` because they fetch per-span fields — status_message,
// trace_id, exception_stacktrace, mat_http_route — that the error-fingerprint
// rollup carries only as state (sample trace_id + status_message hash).
// Each drill-down is bounded by group_id → exception_type + status_message_hash
// narrowing, keeping raw scans cheap. Permanent raw.
package errors

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		utils "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const (
	serviceNameFilter            = " AND s.service_name = @serviceName"
	spansRollupPrefix            = "observability.signoz_index_v3"
	errorFingerprintRollupPrefix = "observability.signoz_index_v3"
)

// httpStatusBucketToCode maps the rollup's coarse http_status_bucket
// ('4xx' | '5xx' | 'err' | 'other') back to a representative integer so
// downstream DTOs (ErrorGroup.HTTPStatusCode) remain populated. The exact
// response_status_code is not preserved by the rollup — this is a fidelity
// loss documented in the Phase-5/6 cascade plan.
func httpStatusBucketToCode(bucket string) int {
	switch bucket {
	case "4xx":
		return 400
	case "5xx":
		return 500
	default:
		return 0
	}
}

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

// errorGroupRawRow scans the rollup aggregate shape before we cast/derive
// the DTO fields the service layer expects (string status message, uint16
// http_status_code).
type errorGroupRawRow struct {
	ServiceName      string    `ch:"service_name"`
	OperationName    string    `ch:"operation_name"`
	StatusMessage    string    `ch:"status_message"`
	HTTPStatusBucket string    `ch:"http_status_bucket"`
	ErrorCount       uint64    `ch:"error_count"`
	LastOccurrence   time.Time `ch:"last_occurrence"`
	FirstOccurrence  time.Time `ch:"first_occurrence"`
	SampleTraceID    string    `ch:"sample_trace_id"`
}

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT service_name,
		       operation_name,
		       any(status_message) AS status_message,
		       http_status_bucket,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)           AS error_count,
		       max(timestamp)             AS last_occurrence,
		       min(timestamp)            AS first_occurrence,
		       any(trace_id)       AS sample_trace_id
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := errFingerprintRollupParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY service_name, operation_name, exception_type, status_message_hash, http_status_bucket
	           ORDER BY error_count DESC
	           LIMIT @limit`
	args = append(args, clickhouse.Named("limit", limit))

	var raw []errorGroupRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorGroups", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]errorGroupRow, len(raw))
	for i, row := range raw {
		rows[i] = errorGroupRow{
			ServiceName:     row.ServiceName,
			OperationName:   row.OperationName,
			StatusMessage:   row.StatusMessage,
			HTTPStatusCode:  uint16(httpStatusBucketToCode(row.HTTPStatusBucket)), //nolint:gosec // domain-bounded
			ErrorCount:      int64(row.ErrorCount),                                //nolint:gosec // domain-bounded
			LastOccurrence:  row.LastOccurrence,
			FirstOccurrence: row.FirstOccurrence,
			SampleTraceID:   row.SampleTraceID,
		}
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
		       any(s.trace_id) AS sample_trace_id,
		       any(s.exception_type) AS exception_type,
		       any(s.exception_stacktrace) AS stack_trace
		FROM observability.signoz_index_v3 s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND toUInt16OrZero(s.response_status_code) = @groupHTTPStatusCode
		GROUP BY s.service_name, s.name, s.status_message, toUInt16OrZero(s.response_status_code)`

	args := append(dbutil.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", code),
	)

	var row errorGroupDetailRow
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorSummary", &row, query, args...); err != nil {
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
		FROM observability.signoz_index_v3 s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND toUInt16OrZero(s.response_status_code) = @groupHTTPStatusCode
		ORDER BY s.timestamp DESC
		LIMIT @limit`

	args := append(dbutil.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", code),
		clickhouse.Named("limit", limit),
	)

	var rows []errorGroupTraceRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorGroupTraces", &rows, query, args...); err != nil {
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
		FROM observability.signoz_index_v3 s
		WHERE s.team_id = @teamID AND (`+ErrorCondition()+`)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND toUInt16OrZero(s.response_status_code) = @groupHTTPStatusCode
		GROUP BY timestamp
		ORDER BY timestamp ASC`, bucket)

	args := append(dbutil.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", code),
	)

	var rows []errorGroupTSRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorGroupTimeseries", &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// serviceErrorRateRow is the DTO for GetServiceErrorRate. Sourced from the
// spans_rollup_1m root-span rollup; error_rate + avg_latency are derived
// Go-side from the merged state columns.
type serviceErrorRateRow struct {
	ServiceName  string    `ch:"service_name"`
	Timestamp    time.Time `ch:"timestamp"`
	RequestCount int64     `ch:"request_count"`
	ErrorCount   int64     `ch:"error_count"`
	ErrorRate    float64   `ch:"error_rate"`
	AvgLatency   float64   `ch:"avg_latency"`
}

type serviceErrorRateRawRow struct {
	ServiceName   string    `ch:"service_name"`
	Timestamp     time.Time `ch:"timestamp"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
}

func (r *ClickHouseRepository) GetServiceErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]serviceErrorRateRow, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT service_name,
		       toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       count()   AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)     AS error_count,
		       sum(duration_nano / 1000000.0) AS duration_ms_sum
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := append(rollupBaseParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service_name, timestamp
		ORDER BY timestamp ASC
		LIMIT 10000`

	var raw []serviceErrorRateRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetServiceErrorRate", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]serviceErrorRateRow, len(raw))
	for i, row := range raw {
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
		rate := 0.0
		if total > 0 {
			rate = float64(errs) * 100.0 / float64(total)
		}
		avg := 0.0
		if row.RequestCount > 0 {
			avg = row.DurationMsSum / float64(row.RequestCount)
		}
		rows[i] = serviceErrorRateRow{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: total,
			ErrorCount:   errs,
			ErrorRate:    rate,
			AvgLatency:   avg,
		}
	}
	return rows, nil
}

// errorVolumeRow is the DTO for GetErrorVolume.
type errorVolumeRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	ErrorCount  int64     `ch:"error_count"`
}

type errorVolumeRawRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	ErrorCount  uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorVolume(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorVolumeRow, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT service_name,
		       toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400) AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := append(rollupBaseParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service_name, timestamp
		ORDER BY timestamp ASC
		LIMIT 10000`

	var raw []errorVolumeRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorVolume", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]errorVolumeRow, 0, len(raw))
	for _, row := range raw {
		if row.ErrorCount == 0 {
			continue
		}
		rows = append(rows, errorVolumeRow{
			ServiceName: row.ServiceName,
			Timestamp:   row.Timestamp,
			ErrorCount:  int64(row.ErrorCount), //nolint:gosec // domain-bounded
		})
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
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT service_name,
		       toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       count()   AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)     AS error_count,
		       sum(duration_nano / 1000000.0) AS duration_ms_sum
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := append(rollupBaseParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service_name, timestamp
		ORDER BY timestamp ASC
		LIMIT 10000`

	var raw []serviceErrorRateRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetLatencyDuringErrorWindows", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]latencyErrorRow, 0, len(raw))
	for _, row := range raw {
		if row.ErrorCount == 0 {
			continue
		}
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
		avg := 0.0
		if row.RequestCount > 0 {
			avg = row.DurationMsSum / float64(row.RequestCount)
		}
		rows = append(rows, latencyErrorRow{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: total,
			ErrorCount:   errs,
			AvgLatency:   avg,
		})
	}
	return rows, nil
}

func rollupIntervalMinutesFor(startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return 1
	case hours <= 24:
		return 5
	case hours <= 168:
		return 60
	default:
		return 1440
	}
}

// queryIntervalMinutes mirrors overview/overview.queryIntervalMinutes — returns
// max(tierStep, dashboardStep) so the query-time step is never finer than the
// rollup tier's native resolution.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	dashStep := rollupIntervalMinutesFor(startMs, endMs)
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

// errFingerprintRollupParams returns the named params used by the fingerprint
// rollup reads: teamID + start/end.
func errFingerprintRollupParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func rollupBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// --- Migrated from errortracking ---

// exceptionRateRawRow scans the time-bucketed exception-type counts from the
// fingerprint rollup; the UInt64 count is cast down to int64 for the DTO.
type exceptionRateRawRow struct {
	Timestamp     time.Time `ch:"time_bucket"`
	ExceptionType string    `ch:"exception_type"`
	Count         uint64    `ch:"event_count"`
}

func (r *ClickHouseRepository) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRatePointDTO, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       exception_type,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                         AS event_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND exception_type != ''`, table)
	args := append(errFingerprintRollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY time_bucket, exception_type ORDER BY time_bucket ASC`

	var raw []exceptionRateRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetExceptionRateByType", &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]exceptionRatePointDTO, len(raw))
	for i, row := range raw {
		rows[i] = exceptionRatePointDTO{
			Timestamp:     row.Timestamp,
			ExceptionType: row.ExceptionType,
			Count:         int64(row.Count), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

// errorHotspotRawRow scans the fingerprint-rollup error counts joined with
// the spans rollup's request totals; error_rate is derived Go-side.
type errorHotspotRawRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	ErrorCount    uint64 `ch:"error_count"`
	TotalCount    uint64 `ch:"total_count"`
}

func (r *ClickHouseRepository) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotCellDTO, error) {
	spansTable := "observability.signoz_index_v3"

	// spans_red already carries request_count + error_count per
	// (service, operation, http_status_bucket). Aggregate directly from that
	// rollup so the hotspot panel avoids a full grouped join at read time.
	query := fmt.Sprintf(`
		SELECT service_name,
		       operation_name,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400) AS error_count,
		       count() AS total_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND operation_name != ''
		GROUP BY service_name, operation_name
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT 500
	`, spansTable)

	var raw []errorHotspotRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorHotspot", &raw, query, rollupBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]errorHotspotCellDTO, len(raw))
	for i, row := range raw {
		total := int64(row.TotalCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)  //nolint:gosec // domain-bounded
		rate := 0.0
		if total > 0 {
			rate = float64(errs) * 100.0 / float64(total)
		}
		rows[i] = errorHotspotCellDTO{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			ErrorRate:     rate,
			ErrorCount:    errs,
			TotalCount:    total,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteDTO, error) {
	query := `
		SELECT s.mat_http_route AS http_route,
		       s.service_name   AS service_name,
		       toInt64(count()) AS count_5xx
		FROM observability.signoz_index_v3 s
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
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetHTTP5xxByRoute", &rows, query, args...)
}

// --- Migrated from errorfingerprint ---

// errorFingerprintRawRow scans the rollup aggregate shape; Count is UInt64
// in the rollup and is cast to int64 for the DTO below.
type errorFingerprintRawRow struct {
	Fingerprint   string    `ch:"fingerprint"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	ExceptionType string    `ch:"exception_type"`
	StatusMessage string    `ch:"status_message"`
	FirstSeen     time.Time `ch:"first_seen"`
	LastSeen      time.Time `ch:"last_seen"`
	Count         uint64    `ch:"cnt"`
	SampleTraceID string    `ch:"sample_trace_id"`
}

func (r *ClickHouseRepository) ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorFingerprintDTO, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT toString(status_message_hash)   AS fingerprint,
		       service_name,
		       operation_name,
		       exception_type,
		       any(status_message) AS status_message,
		       min(timestamp)            AS first_seen,
		       max(timestamp)             AS last_seen,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)           AS cnt,
		       any(trace_id)       AS sample_trace_id
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := errFingerprintRollupParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service_name, operation_name, exception_type, status_message_hash, http_status_bucket
		ORDER BY cnt DESC
		LIMIT @limit`
	args = append(args, clickhouse.Named("limit", limit))

	var raw []errorFingerprintRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.ListFingerprints", &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]errorFingerprintDTO, len(raw))
	for i, row := range raw {
		rows[i] = errorFingerprintDTO{
			Fingerprint:   row.Fingerprint,
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			ExceptionType: row.ExceptionType,
			StatusMessage: row.StatusMessage,
			FirstSeen:     row.FirstSeen,
			LastSeen:      row.LastSeen,
			Count:         int64(row.Count), //nolint:gosec // domain-bounded
			SampleTraceID: row.SampleTraceID,
		}
	}
	return rows, nil
}

// fingerprintTrendRawRow scans the rollup aggregate shape before casting the
// UInt64 count to int64 for the DTO.
type fingerprintTrendRawRow struct {
	Timestamp time.Time `ch:"ts"`
	Count     uint64    `ch:"cnt"`
}

func (r *ClickHouseRepository) GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]fingerprintTrendPointDTO, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS ts,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                        AS cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_name = @serviceName
		  AND operation_name = @operationName
		  AND exception_type = @exceptionType
		  AND status_message_hash = cityHash64(@statusMessage)
		GROUP BY ts
		ORDER BY ts ASC
	`, table)
	args := append(errFingerprintRollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("exceptionType", exceptionType),
		clickhouse.Named("statusMessage", statusMessage),
	)

	var raw []fingerprintTrendRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetFingerprintTrend", &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]fingerprintTrendPointDTO, len(raw))
	for i, row := range raw {
		rows[i] = fingerprintTrendPointDTO{
			Timestamp: row.Timestamp,
			Count:     int64(row.Count), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}
