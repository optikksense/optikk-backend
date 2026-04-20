package errors

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	utils "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
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
	GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRateRawRow, error)
	GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotRawRow, error)
	GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteRawRow, error)

	// Migrated from errorfingerprint
	ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorFingerprintRawRow, error)
	GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]fingerprintTrendRawRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// errorGroupRow is the CH scan target for GetErrorGroups. Counts come back
// as uint64 (CH's native count() return type); the service layer casts
// them to int64 at the API boundary.
type errorGroupRow struct {
	ServiceName     string    `ch:"service_name"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      uint64    `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	SampleTraceID   string    `ch:"sample_trace_id"`
}

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorGroupRow, error) {
	query := `
		SELECT s.service_name    AS service_name,
		       s.name             AS operation_name,
		       s.status_message   AS status_message,
		       s.http_status_code AS http_status_code,
		       count()            AS error_count,
		       MAX(s.timestamp)   AS last_occurrence,
		       MIN(s.timestamp)   AS first_occurrence,
		       any(s.trace_id)    AS sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `) AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY s.service_name, s.name, s.status_message, s.http_status_code
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

// errorGroupDetailRow is the CH scan target for GetErrorGroupDetail.
type errorGroupDetailRow struct {
	ServiceName     string    `ch:"service_name"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      uint64    `ch:"error_count"`
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
		SELECT s.service_name       AS service_name,
		       s.name                AS operation_name,
		       s.status_message      AS status_message,
		       s.http_status_code    AS http_status_code,
		       count()               AS error_count,
		       MAX(s.timestamp)      AS last_occurrence,
		       MIN(s.timestamp)      AS first_occurrence,
		       any(s.trace_id)       AS sample_trace_id,
		       any(s.exception_type) AS exception_type,
		       any(s.exception_stacktrace) AS stack_trace
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND s.http_status_code = @groupHTTPStatusCode
		GROUP BY s.service_name, s.name, s.status_message, s.http_status_code`

	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", uint16(code)), //nolint:gosec // HTTP status codes fit UInt16
	)

	var row errorGroupDetailRow
	if err := r.db.QueryRow(database.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return nil, err
	}

	return &row, nil
}

// errorGroupTraceRow is the CH scan target for GetErrorGroupTraces.
// duration_ms stays raw nanoseconds; service divides by 1e6 in Go.
type errorGroupTraceRow struct {
	TraceID       string    `ch:"trace_id"`
	SpanID        string    `ch:"span_id"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationNanos int64     `ch:"duration_nano"`
	StatusCode    string    `ch:"status_code"`
}

func (r *ClickHouseRepository) GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]errorGroupTraceRow, error) {
	svc, op, msg, code, err := r.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.trace_id           AS trace_id,
		       s.span_id             AS span_id,
		       s.timestamp           AS timestamp,
		       s.duration_nano       AS duration_nano,
		       s.status_code_string  AS status_code
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND s.http_status_code = @groupHTTPStatusCode
		ORDER BY s.timestamp DESC
		LIMIT @limit`

	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", uint16(code)), //nolint:gosec // HTTP status codes fit UInt16
		clickhouse.Named("limit", limit),
	)

	var rows []errorGroupTraceRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// errorGroupTSRow is the CH scan target for GetErrorGroupTimeseries.
type errorGroupTSRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	ErrorCount uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorGroupTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]errorGroupTSRow, error) {
	svc, op, msg, code, err := r.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       count() AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND (`+ErrorCondition()+`)
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.service_name = @groupServiceName
		  AND s.name = @groupOperationName
		  AND s.status_message = @groupStatusMessage
		  AND s.http_status_code = @groupHTTPStatusCode
		GROUP BY timestamp
		ORDER BY timestamp ASC`, bucket)

	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("groupServiceName", svc),
		clickhouse.Named("groupOperationName", op),
		clickhouse.Named("groupStatusMessage", msg),
		clickhouse.Named("groupHTTPStatusCode", uint16(code)), //nolint:gosec // HTTP status codes fit UInt16
	)

	var rows []errorGroupTSRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// serviceErrorRateRow is the CH scan target for the total leg of
// GetServiceErrorRate: per-(bucket, service) request count + latency sum.
// Service layer merges this with errorLegRow to compute error_rate and
// avg_latency.
type serviceErrorRateRow struct {
	ServiceName      string    `ch:"service_name"`
	Timestamp        time.Time `ch:"timestamp"`
	RequestCount     uint64    `ch:"request_count"`
	DurationNanosSum uint64    `ch:"duration_nano_sum"`
	// Populated by the service layer after merging the error leg.
	ErrorCount uint64
}

// errorLegRow is the per-(bucket, service) error count used by
// GetServiceErrorRate / GetErrorVolume / GetLatencyDuringErrorWindows to
// compose ratios in Go.
type errorLegRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	ErrorCount  uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) getBucketedTotals(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]serviceErrorRateRow, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT s.service_name    AS service_name,
		       %s                AS timestamp,
		       count()           AS request_count,
		       sum(s.duration_nano) AS duration_nano_sum
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s ORDER BY timestamp ASC LIMIT 10000`, bucket)

	var rows []serviceErrorRateRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) getBucketedErrors(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorLegRow, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT s.service_name AS service_name,
		       %s             AS timestamp,
		       count()        AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND (`+ErrorCondition()+`) AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s ORDER BY timestamp ASC LIMIT 10000`, bucket)

	var rows []errorLegRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetServiceErrorRate returns per-(bucket, service) rows with totals plus
// error counts. Previously a combinator-driven aggregate produced both
// legs in one scan; now two narrow-WHERE scans run in parallel and the
// service merges + divides in Go.
func (r *ClickHouseRepository) GetServiceErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]serviceErrorRateRow, error) {
	var (
		totals []serviceErrorRateRow
		errs   []errorLegRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		v, err := r.getBucketedTotals(gctx, teamID, startMs, endMs, serviceName)
		totals = v
		return err
	})
	g.Go(func() error {
		v, err := r.getBucketedErrors(gctx, teamID, startMs, endMs, serviceName)
		errs = v
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[errKey(e.ServiceName, e.Timestamp)] = e.ErrorCount
	}
	for i := range totals {
		totals[i].ErrorCount = errIdx[errKey(totals[i].ServiceName, totals[i].Timestamp)]
	}
	return totals, nil
}

func errKey(svc string, ts time.Time) string {
	return fmt.Sprintf("%s|%d", svc, ts.UnixNano())
}

// errorVolumeRow is the CH scan target for GetErrorVolume.
type errorVolumeRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	ErrorCount  uint64    `ch:"error_count"`
}

// GetErrorVolume returns non-zero error buckets. The previous combinator+
// HAVING pattern collapses into a plain WHERE filter on the error
// condition, leaving the aggregate as a pure count().
func (r *ClickHouseRepository) GetErrorVolume(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorVolumeRow, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT s.service_name AS service_name,
		       %s             AS timestamp,
		       count()        AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND (`+ErrorCondition()+`) AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s ORDER BY timestamp ASC LIMIT 10000`, bucket)

	var rows []errorVolumeRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// latencyErrorRow carries the merged totals + error count + latency sum
// per (bucket, service). Non-zero-error filter happens in the service layer.
type latencyErrorRow struct {
	ServiceName      string    `ch:"service_name"`
	Timestamp        time.Time `ch:"timestamp"`
	RequestCount     uint64    `ch:"request_count"`
	DurationNanosSum uint64    `ch:"duration_nano_sum"`
	ErrorCount       uint64
}

// GetLatencyDuringErrorWindows returns totals + error counts per
// (bucket, service). Previously a combinator-driven aggregate produced
// error counts + average latency in one scan; same split-scan pattern as
// GetServiceErrorRate now.
func (r *ClickHouseRepository) GetLatencyDuringErrorWindows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]latencyErrorRow, error) {
	var (
		totals []serviceErrorRateRow
		errs   []errorLegRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		v, err := r.getBucketedTotals(gctx, teamID, startMs, endMs, serviceName)
		totals = v
		return err
	})
	g.Go(func() error {
		v, err := r.getBucketedErrors(gctx, teamID, startMs, endMs, serviceName)
		errs = v
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[errKey(e.ServiceName, e.Timestamp)] = e.ErrorCount
	}
	out := make([]latencyErrorRow, 0, len(totals))
	for _, t := range totals {
		out = append(out, latencyErrorRow{
			ServiceName:      t.ServiceName,
			Timestamp:        t.Timestamp,
			RequestCount:     t.RequestCount,
			DurationNanosSum: t.DurationNanosSum,
			ErrorCount:       errIdx[errKey(t.ServiceName, t.Timestamp)],
		})
	}
	return out, nil
}

// --- Migrated from errortracking ---

// exceptionRateRawRow is the CH scan target for GetExceptionRateByType.
type exceptionRateRawRow struct {
	Timestamp     time.Time `ch:"time_bucket"`
	ExceptionType string    `ch:"exception_type"`
	Count         uint64    `ch:"event_count"`
}

func (r *ClickHouseRepository) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRateRawRow, error) {
	bucket := utils.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.exception_type AS exception_type,
		       count() AS event_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.exception_type != '' AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY time_bucket, exception_type ORDER BY time_bucket ASC`

	var rows []exceptionRateRawRow
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}

// errorHotspotRawRow is the CH scan target for GetErrorHotspot. error_rate
// is derived in Go from error_count / total_count so SELECT stays pure.
type errorHotspotRawRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	TotalCount    uint64 `ch:"total_count"`
	ErrorCount    uint64 `ch:"error_count"`
}

// hotspotErrorLegRow is the per-(service, operation) error-only count used
// to merge into the total scan for GetErrorHotspot.
type hotspotErrorLegRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	ErrorCount    uint64 `ch:"error_count"`
}

// GetErrorHotspot returns per-(service, operation) totals + error counts.
// Previously both legs plus the ratio shipped from SQL via combinators
// and conditionals; we now run a totals scan and an error-only scan in
// parallel and merge + divide in Go.
func (r *ClickHouseRepository) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotRawRow, error) {
	args := database.SpanBaseParams(teamID, startMs, endMs)

	var (
		totals []errorHotspotRawRow
		errs   []hotspotErrorLegRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Totals scan — ErrorCount is zeroed post-scan and overwritten from
		// the error-leg map. We alias count() a second time so the struct tag
		// resolution still finds error_count, then zero it in Go.
		totalsQuery := `
			SELECT s.service_name AS service_name,
			       s.name          AS operation_name,
			       count()         AS total_count,
			       count()         AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name, s.name
			ORDER BY total_count DESC
			LIMIT 500`
		if err := r.db.Select(database.OverviewCtx(gctx), &totals, totalsQuery, args...); err != nil {
			return fmt.Errorf("error hotspot totals: %w", err)
		}
		for i := range totals {
			totals[i].ErrorCount = 0
		}
		return nil
	})
	g.Go(func() error {
		errQuery := `
			SELECT s.service_name AS service_name,
			       s.name          AS operation_name,
			       count()         AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND (` + ErrorCondition() + `) AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name, s.name`
		return r.db.Select(database.OverviewCtx(gctx), &errs, errQuery, args...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.ServiceName+"|"+e.OperationName] = e.ErrorCount
	}
	for i := range totals {
		totals[i].ErrorCount = errIdx[totals[i].ServiceName+"|"+totals[i].OperationName]
	}
	return totals, nil
}

// http5xxByRouteRawRow is the CH scan target for GetHTTP5xxByRoute.
type http5xxByRouteRawRow struct {
	HTTPRoute   string `ch:"http_route"`
	ServiceName string `ch:"service_name"`
	Count       uint64 `ch:"count_5xx"`
}

func (r *ClickHouseRepository) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteRawRow, error) {
	query := `
		SELECT s.mat_http_route AS http_route,
		       s.service_name   AS service_name,
		       count()          AS count_5xx
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.http_status_code >= 500`
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY http_route, s.service_name ORDER BY count_5xx DESC LIMIT 100`

	var rows []http5xxByRouteRawRow
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}

// --- Migrated from errorfingerprint ---

// errorFingerprintRawRow is the CH scan target for ListFingerprints.
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

func (r *ClickHouseRepository) ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]errorFingerprintRawRow, error) {
	query := `
		SELECT hex(cityHash64(s.service_name, s.name, s.mat_exception_type, s.status_message)) AS fingerprint,
		       s.service_name       AS service_name,
		       s.name               AS operation_name,
		       s.mat_exception_type AS exception_type,
		       s.status_message     AS status_message,
		       min(s.timestamp)     AS first_seen,
		       max(s.timestamp)     AS last_seen,
		       count()              AS cnt,
		       any(s.trace_id)      AS sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND (` + ErrorCondition() + `)`
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY s.service_name, s.name, s.mat_exception_type, s.status_message
		ORDER BY cnt DESC
		LIMIT @limit`
	args = append(args, clickhouse.Named("limit", limit))

	var rows []errorFingerprintRawRow
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}

// fingerprintTrendRawRow is the CH scan target for GetFingerprintTrend.
type fingerprintTrendRawRow struct {
	Timestamp time.Time `ch:"ts"`
	Count     uint64    `ch:"cnt"`
}

func (r *ClickHouseRepository) GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]fingerprintTrendRawRow, error) {
	bucket := utils.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s     AS ts,
		       count() AS cnt
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND (`+ErrorCondition()+`)
		  AND s.service_name = @serviceName
		  AND s.name = @operationName
		  AND s.mat_exception_type = @exceptionType
		  AND s.status_message = @statusMessage
		GROUP BY ts
		ORDER BY ts ASC
	`, bucket)
	args := append(database.SpanBaseParams(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("exceptionType", exceptionType),
		clickhouse.Named("statusMessage", statusMessage),
	)

	var rows []fingerprintTrendRawRow
	return rows, r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
}
