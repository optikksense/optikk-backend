package overview

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

func overviewBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]requestRateRow, error)
	GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorRateRow, error)
	GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]p95LatencyRow, error)
	GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricRow, error)
	GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricRow, error)
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (serviceMetricRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// requestRateRow is the DTO returned to the service layer.
type requestRateRow struct {
	Timestamp    time.Time
	ServiceName  string
	RequestCount int64
}

// requestRateScanRow is the CH scan target with native uint64 count() output.
type requestRateScanRow struct {
	Timestamp    time.Time `ch:"time_bucket"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]requestRateRow, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s             AS time_bucket,
		       s.service_name AS service_name,
		       count()        AS request_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+`
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	var rows []requestRateScanRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	out := make([]requestRateRow, len(rows))
	for i, row := range rows {
		out[i] = requestRateRow{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: int64(row.RequestCount), //nolint:gosec // domain-bounded
		}
	}
	return out, nil
}

// errorRateRow is the DTO returned to the service layer. ErrorRate is computed
// in Go from RequestCount / ErrorCount after the split-scan merge.
type errorRateRow struct {
	Timestamp    time.Time
	ServiceName  string
	RequestCount int64
	ErrorCount   int64
	ErrorRate    float64
}

// errorRateTotalsRow is the CH scan target for the totals leg of GetErrorRate.
type errorRateTotalsRow struct {
	Timestamp    time.Time `ch:"time_bucket"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
}

// errorRateErrorsRow is the CH scan target for the error-only leg of GetErrorRate.
type errorRateErrorsRow struct {
	Timestamp   time.Time `ch:"time_bucket"`
	ServiceName string    `ch:"service_name"`
	ErrorCount  uint64    `ch:"error_count"`
}

func errorRateKey(svc string, ts time.Time) string {
	return fmt.Sprintf("%s|%d", svc, ts.UnixNano())
}

func (r *ClickHouseRepository) GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorRateRow, error) {
	bucket := overviewBucketExpr(startMs, endMs)

	var (
		totals []errorRateTotalsRow
		errs   []errorRateErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s             AS time_bucket,
			       s.service_name AS service_name,
			       count()        AS request_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+`
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`, bucket)
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		query += fmt.Sprintf(` GROUP BY %s, s.service_name
			ORDER BY time_bucket ASC, service_name ASC
			LIMIT 10000`, bucket)
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, query, args...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s             AS time_bucket,
			       s.service_name AS service_name,
			       count()        AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND (`+ErrorCondition()+`)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`, bucket)
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		query += fmt.Sprintf(` GROUP BY %s, s.service_name LIMIT 10000`, bucket)
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, query, args...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[errorRateKey(e.ServiceName, e.Timestamp)] = e.ErrorCount
	}
	out := make([]errorRateRow, len(totals))
	for i, t := range totals {
		reqCount := int64(t.RequestCount)                                      //nolint:gosec // domain-bounded
		errCount := int64(errIdx[errorRateKey(t.ServiceName, t.Timestamp)])    //nolint:gosec // domain-bounded
		var rate float64
		if reqCount > 0 {
			rate = float64(errCount) * 100.0 / float64(reqCount)
		}
		out[i] = errorRateRow{
			Timestamp:    t.Timestamp,
			ServiceName:  t.ServiceName,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    rate,
		}
	}
	return out, nil
}

// p95LatencyRow is the DTO for GetP95Latency. Filled entirely by the service
// layer from sketch.Querier.PercentilesTimeseries — no SQL executed here.
type p95LatencyRow struct {
	Timestamp   time.Time
	ServiceName string
	P95         float64
}

// GetP95Latency is a deliberate no-op at the repository level: the service
// calls sketch.Querier.PercentilesTimeseries(SpanLatencyService, …, step, 0.95)
// and assembles rows there. Kept on the Repository interface so existing
// callers compile; returns an empty slice so any accidental use is safe.
func (r *ClickHouseRepository) GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]p95LatencyRow, error) {
	_ = ctx
	_ = teamID
	_ = startMs
	_ = endMs
	_ = serviceName
	return nil, nil
}

// serviceMetricRow is the DTO returned to the service layer.
type serviceMetricRow struct {
	ServiceName  string
	RequestCount int64
	ErrorCount   int64
	AvgLatency   float64
	P50Latency   float64
	P95Latency   float64
	P99Latency   float64
}

// servicesTotalsRow is the CH scan target for the totals leg of GetServices:
// volume + latency sum per service. Avg = LatencyNanosSum / 1e6 / RequestCount
// is computed in Go.
type servicesTotalsRow struct {
	ServiceName      string  `ch:"service_name"`
	RequestCount     uint64  `ch:"request_count"`
	DurationNanosSum float64 `ch:"duration_nano_sum"`
}

// servicesErrorsRow is the CH scan target for the error-only leg of GetServices.
type servicesErrorsRow struct {
	ServiceName string `ch:"service_name"`
	ErrorCount  uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricRow, error) {
	// Percentiles come from sketch.Querier (SpanLatencyService) — see service.go.
	// CH returns only count/errors/avg; sketch merge in Go fills p50/p95/p99.
	var (
		totals []servicesTotalsRow
		errs   []servicesErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := `
			SELECT s.service_name       AS service_name,
			       count()              AS request_count,
			       sum(s.duration_nano) AS duration_nano_sum
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
			ORDER BY request_count DESC`
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	})

	g.Go(func() error {
		query := `
			SELECT s.service_name AS service_name,
			       count()        AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND (` + ErrorCondition() + `)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.ServiceName] = e.ErrorCount
	}

	out := make([]serviceMetricRow, len(totals))
	for i, t := range totals {
		reqCount := int64(t.RequestCount)      //nolint:gosec // domain-bounded
		errCount := int64(errIdx[t.ServiceName]) //nolint:gosec // domain-bounded
		var avgMs float64
		if t.RequestCount > 0 {
			avgMs = (t.DurationNanosSum / 1_000_000.0) / float64(t.RequestCount)
		}
		out[i] = serviceMetricRow{
			ServiceName:  t.ServiceName,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			AvgLatency:   avgMs,
		}
	}
	return out, nil
}

// endpointMetricRow is the DTO for GetTopEndpoints.
type endpointMetricRow struct {
	ServiceName   string
	OperationName string
	EndpointName  string
	HTTPMethod    string
	RequestCount  int64
	ErrorCount    int64
	AvgLatency    float64
	P50Latency    float64
	P95Latency    float64
	P99Latency    float64
}

// endpointTotalsRow is the CH scan target for the totals leg of GetTopEndpoints.
// EndpointName is resolved from (mat_http_route, mat_http_target, name) in Go
// so the emitted SELECT stays free of fallback-resolution builtins.
type endpointTotalsRow struct {
	ServiceName      string  `ch:"service_name"`
	OperationName    string  `ch:"operation_name"`
	HTTPRoute        string  `ch:"http_route"`
	HTTPTarget       string  `ch:"http_target"`
	HTTPMethod       string  `ch:"http_method"`
	RequestCount     uint64  `ch:"request_count"`
	DurationNanosSum float64 `ch:"duration_nano_sum"`
}

// endpointErrorsRow is the CH scan target for the error-only leg.
type endpointErrorsRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	HTTPRoute     string `ch:"http_route"`
	HTTPTarget    string `ch:"http_target"`
	HTTPMethod    string `ch:"http_method"`
	ErrorCount    uint64 `ch:"error_count"`
}

func endpointNameFromParts(route, target, opName string) string {
	if route != "" {
		return route
	}
	if target != "" {
		return target
	}
	return opName
}

func endpointKey(svc, op, route, target, method string) string {
	return svc + "|" + op + "|" + route + "|" + target + "|" + method
}

func (r *ClickHouseRepository) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricRow, error) {
	// Percentiles come from sketch.Querier (SpanLatencyEndpoint) — see service.go.
	// Two parallel scans (totals + errors) replace the former combinator aggregate.
	// Go reduces the top-100 set on (service, op, http_route, http_target, http_method)
	// ordered by request_count DESC so the result stays stable when Go resolves
	// endpoint_name = route -> target -> op with plain Go fallbacks.
	var (
		totals []endpointTotalsRow
		errs   []endpointErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := `
			SELECT s.service_name       AS service_name,
			       s.name                AS operation_name,
			       s.mat_http_route      AS http_route,
			       s.mat_http_target     AS http_target,
			       s.http_method         AS http_method,
			       count()               AS request_count,
			       sum(s.duration_nano)  AS duration_nano_sum
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		query += `
			GROUP BY service_name, operation_name, http_route, http_target, http_method
			ORDER BY request_count DESC
			LIMIT 100`
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, query, args...)
	})

	g.Go(func() error {
		query := `
			SELECT s.service_name   AS service_name,
			       s.name            AS operation_name,
			       s.mat_http_route  AS http_route,
			       s.mat_http_target AS http_target,
			       s.http_method     AS http_method,
			       count()           AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND (` + ErrorCondition() + `)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		query += `
			GROUP BY service_name, operation_name, http_route, http_target, http_method`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, query, args...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[endpointKey(e.ServiceName, e.OperationName, e.HTTPRoute, e.HTTPTarget, e.HTTPMethod)] = e.ErrorCount
	}

	out := make([]endpointMetricRow, len(totals))
	for i, t := range totals {
		reqCount := int64(t.RequestCount) //nolint:gosec // domain-bounded
		errCount := int64(errIdx[endpointKey(t.ServiceName, t.OperationName, t.HTTPRoute, t.HTTPTarget, t.HTTPMethod)]) //nolint:gosec // domain-bounded
		var avgMs float64
		if t.RequestCount > 0 {
			avgMs = (t.DurationNanosSum / 1_000_000.0) / float64(t.RequestCount)
		}
		out[i] = endpointMetricRow{
			ServiceName:   t.ServiceName,
			OperationName: t.OperationName,
			EndpointName:  endpointNameFromParts(t.HTTPRoute, t.HTTPTarget, t.OperationName),
			HTTPMethod:    t.HTTPMethod,
			RequestCount:  reqCount,
			ErrorCount:    errCount,
			AvgLatency:    avgMs,
		}
	}
	return out, nil
}

// summaryTotalsRow is the CH scan target for the totals leg of GetSummary.
type summaryTotalsRow struct {
	RequestCount     uint64  `ch:"request_count"`
	DurationNanosSum float64 `ch:"duration_nano_sum"`
}

// summaryErrorsRow is the CH scan target for the error-only leg of GetSummary.
type summaryErrorsRow struct {
	ErrorCount uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (serviceMetricRow, error) {
	// Percentiles come from sketch.Querier (merged SpanLatencyService across
	// all services for the tenant) — see service.go.
	var (
		totals summaryTotalsRow
		errs   summaryErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := `
			SELECT count()              AS request_count,
			       sum(s.duration_nano) AS duration_nano_sum
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, dbutil.SpanBaseParams(teamID, startMs, endMs)...).ScanStruct(&totals)
	})

	g.Go(func() error {
		query := `
			SELECT count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND (` + ErrorCondition() + `)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, dbutil.SpanBaseParams(teamID, startMs, endMs)...).ScanStruct(&errs)
	})

	if err := g.Wait(); err != nil {
		return serviceMetricRow{}, err
	}

	var avgMs float64
	if totals.RequestCount > 0 {
		avgMs = (totals.DurationNanosSum / 1_000_000.0) / float64(totals.RequestCount)
	}
	return serviceMetricRow{
		ServiceName:  "",
		RequestCount: int64(totals.RequestCount), //nolint:gosec // domain-bounded
		ErrorCount:   int64(errs.ErrorCount),     //nolint:gosec // domain-bounded
		AvgLatency:   avgMs,
	}, nil
}
