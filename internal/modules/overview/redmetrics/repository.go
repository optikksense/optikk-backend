package redmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
	"golang.org/x/sync/errgroup"
)

// errorPredicate is the plain-comparison WHERE clause used across RED metrics
// for "is this span an error?" — `has_error` is a materialized Bool column and
// `http_status_code` is the UInt16 alias of response_status_code. Neither form
// uses a banned cast or combinator.
const errorPredicate = "(has_error = true OR http_status_code >= 400)"

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error)
	GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error)
	GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error)
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]p95LatencyTSRow, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error)
	GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error)
	GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// summaryTotalRow is the per-service totals scan target.
type summaryTotalRow struct {
	ServiceName string `ch:"service_name"`
	TotalCount  uint64 `ch:"total_count"`
}

// summaryErrorRow is the per-service error-only leg.
type summaryErrorRow struct {
	ServiceName string `ch:"service_name"`
	ErrorCount  uint64 `ch:"error_count"`
}

// GetSummary runs a per-service totals scan and a per-service error-only scan
// in parallel, then merges them in Go. Percentiles are zeroed here and filled
// by the service layer from sketch.Querier.
func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error) {
	var totals []summaryTotalRow
	var errs []summaryErrorRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		q := `
			SELECT service_name, count() AS total_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ` + rootspan.Condition("s") + ` AND s.timestamp BETWEEN @start AND @end
			GROUP BY service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, q, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	})
	g.Go(func() error {
		q := `
			SELECT service_name, count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ` + rootspan.Condition("s") + ` AND s.timestamp BETWEEN @start AND @end AND ` + errorPredicate + `
			GROUP BY service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, q, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.ServiceName] = e.ErrorCount
	}

	rows := make([]redSummaryServiceRow, 0, len(totals))
	for _, t := range totals {
		rows = append(rows, redSummaryServiceRow{
			ServiceName: t.ServiceName,
			TotalCount:  int64(t.TotalCount),          //nolint:gosec // count fits int64
			ErrorCount:  int64(errIdx[t.ServiceName]), //nolint:gosec // count fits int64
		})
	}
	return rows, nil
}

// apdexCountRow scans a per-service count() for one Apdex band.
type apdexCountRow struct {
	ServiceName string `ch:"service_name"`
	Count       uint64 `ch:"cnt"`
}

// GetApdex runs four parallel narrow-WHERE scans (satisfied / tolerating /
// frustrated / total) and merges them per-service. Each band's duration cutoff
// stays in WHERE as a plain comparison; aggregates are pure count() only.
func (r *ClickHouseRepository) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error) {
	baseArgs := dbutil.SpanBaseParams(teamID, startMs, endMs)
	baseArgs = append(baseArgs,
		clickhouse.Named("satisfiedMs", satisfiedMs),
		clickhouse.Named("toleratingMs", toleratingMs),
		clickhouse.Named("serviceName", serviceName),
	)

	runCountScan := func(gctx context.Context, where string, out *[]apdexCountRow) error {
		q := `
			SELECT s.service_name AS service_name, count() AS cnt
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ` + rootspan.Condition("s") + ` AND s.timestamp BETWEEN @start AND @end
			  AND (@serviceName = '' OR s.service_name = @serviceName)` + where + `
			GROUP BY s.service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), out, q, baseArgs...)
	}

	var satisfied, tolerating, frustrated, total []apdexCountRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return runCountScan(gctx, ` AND s.duration_nano / 1000000.0 <= @satisfiedMs`, &satisfied)
	})
	g.Go(func() error {
		return runCountScan(gctx, ` AND s.duration_nano / 1000000.0 > @satisfiedMs AND s.duration_nano / 1000000.0 <= @toleratingMs`, &tolerating)
	})
	g.Go(func() error {
		return runCountScan(gctx, ` AND s.duration_nano / 1000000.0 > @toleratingMs`, &frustrated)
	})
	g.Go(func() error {
		return runCountScan(gctx, ``, &total)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	idx := func(rows []apdexCountRow) map[string]uint64 {
		m := make(map[string]uint64, len(rows))
		for _, r := range rows {
			m[r.ServiceName] = r.Count
		}
		return m
	}
	sIdx, tIdx, fIdx := idx(satisfied), idx(tolerating), idx(frustrated)

	out := make([]apdexRow, 0, len(total))
	for _, t := range total {
		out = append(out, apdexRow{
			ServiceName: t.ServiceName,
			Satisfied:   int64(sIdx[t.ServiceName]), //nolint:gosec // count fits int64
			Tolerating:  int64(tIdx[t.ServiceName]), //nolint:gosec // count fits int64
			Frustrated:  int64(fIdx[t.ServiceName]), //nolint:gosec // count fits int64
			TotalCount:  int64(t.Count),             //nolint:gosec // count fits int64
		})
	}
	// Match prior ORDER BY total_count DESC.
	sortByTotalDesc(out)
	return out, nil
}

func sortByTotalDesc(rows []apdexRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && rows[j].TotalCount > rows[j-1].TotalCount; j-- {
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}
}

// slowOperationRawRow is the raw per-(service, operation) scan target.
type slowOperationRawRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	SpanCount     uint64 `ch:"span_count"`
}

func (r *ClickHouseRepository) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error) {
	// Percentiles come from sketch.Querier (SpanLatencyEndpoint w/ prefix
	// service|operation|) — see service.go. Ordering by span_count as a
	// reasonable proxy at the SQL layer; the final p95-based sort + limit
	// is applied in service.go after percentiles are filled in.
	var raw []slowOperationRawRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, `
		SELECT service_name,
		       name     AS operation_name,
		       count()  AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY service_name, operation_name
		ORDER BY span_count DESC
		LIMIT @limit
	`, append(dbutil.SpanBaseParams(teamID, startMs, endMs), clickhouse.Named("limit", limit))...)
	if err != nil {
		return nil, err
	}
	rows := make([]slowOperationRow, len(raw))
	for i, r := range raw {
		rows[i] = slowOperationRow{
			ServiceName:   r.ServiceName,
			OperationName: r.OperationName,
			SpanCount:     int64(r.SpanCount), //nolint:gosec // count fits int64
		}
	}
	return rows, nil
}

// topErrorOpsTotalRow is the per-(service, operation) totals leg.
type topErrorOpsTotalRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	TotalCount    uint64 `ch:"total_count"`
}

// topErrorOpsErrorRow is the per-(service, operation) error-only leg. Same
// grouping key as the totals leg; the counts diverge by WHERE predicate.
type topErrorOpsErrorRow struct {
	ServiceName   string `ch:"service_name"`
	OperationName string `ch:"operation_name"`
	ErrorCount    uint64 `ch:"error_count"`
}

// GetTopErrorOperations runs a totals scan and an error-only scan in parallel,
// then merges + computes the error rate in Go. The final LIMIT is applied
// after merge (we overshoot the error leg to keep the top-N stable).
func (r *ClickHouseRepository) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error) {
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)

	var totals []topErrorOpsTotalRow
	var errs []topErrorOpsErrorRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Totals per (service, operation) — no error filter here so the final
		// error-rate denominator is the true traffic volume.
		q := `
			SELECT service_name,
			       name    AS operation_name,
			       count() AS total_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ` + rootspan.Condition("s") + ` AND s.timestamp BETWEEN @start AND @end
			GROUP BY service_name, operation_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, q, args...)
	})
	g.Go(func() error {
		q := `
			SELECT service_name,
			       name    AS operation_name,
			       count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND ` + rootspan.Condition("s") + ` AND s.timestamp BETWEEN @start AND @end
			  AND ` + errorPredicate + `
			GROUP BY service_name, operation_name
			ORDER BY error_count DESC
			LIMIT @limit`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, q, append(args, clickhouse.Named("limit", limit))...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	totalIdx := make(map[string]uint64, len(totals))
	for _, t := range totals {
		totalIdx[t.ServiceName+"|"+t.OperationName] = t.TotalCount
	}

	// Keep only (service, op) pairs that have errors — matches the original
	// HAVING-style filter produced by adding the error predicate to the main
	// query's WHERE.
	out := make([]errorOperationRow, 0, len(errs))
	for _, e := range errs {
		total := totalIdx[e.ServiceName+"|"+e.OperationName]
		var rate float64
		if total > 0 {
			rate = float64(e.ErrorCount) / float64(total)
		}
		out = append(out, errorOperationRow{
			ServiceName:   e.ServiceName,
			OperationName: e.OperationName,
			TotalCount:    int64(total),        //nolint:gosec // count fits int64
			ErrorCount:    int64(e.ErrorCount), //nolint:gosec // count fits int64
			ErrorRate:     rate,
		})
	}
	return out, nil
}

// serviceRatePointRaw scans (bucket, service, count) — repo converts to
// per-second RPS in Go.
type serviceRatePointRaw struct {
	Timestamp    time.Time `ch:"timestamp"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	var raw []serviceRatePointRaw
	err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, fmt.Sprintf(`
		SELECT %s            AS timestamp,
		       service_name,
		       count()       AS request_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC
	`, bucket), dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	if err != nil {
		return nil, err
	}
	rows := make([]ServiceRatePoint, len(raw))
	for i, r := range raw {
		rows[i] = ServiceRatePoint{
			Timestamp:   r.Timestamp,
			ServiceName: r.ServiceName,
			RPS:         float64(r.RequestCount) / 60.0,
		}
	}
	return rows, nil
}

// errorTSTotalRow is the per-(bucket, service) totals leg.
type errorTSTotalRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
}

// errorTSErrorRow is the per-(bucket, service) error-only leg.
type errorTSErrorRow struct {
	Timestamp   time.Time `ch:"timestamp"`
	ServiceName string    `ch:"service_name"`
	ErrorCount  uint64    `ch:"error_count"`
}

// GetErrorRateTimeSeries runs a totals scan and an error-only scan in parallel,
// then merges + computes the error percentage in Go.
func (r *ClickHouseRepository) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)

	var totals []errorTSTotalRow
	var errs []errorTSErrorRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT %s            AS timestamp,
			       service_name,
			       count()       AS request_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
			GROUP BY timestamp, service_name
			ORDER BY timestamp ASC
		`, bucket)
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, q, args...)
	})
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT %s            AS timestamp,
			       service_name,
			       count()       AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
			  AND `+errorPredicate+`
			GROUP BY timestamp, service_name
			ORDER BY timestamp ASC
		`, bucket)
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, q, args...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type key struct {
		ts  int64
		svc string
	}
	errIdx := make(map[key]uint64, len(errs))
	for _, e := range errs {
		errIdx[key{e.Timestamp.UnixNano(), e.ServiceName}] = e.ErrorCount
	}

	out := make([]ServiceErrorRatePoint, 0, len(totals))
	for _, t := range totals {
		errCount := errIdx[key{t.Timestamp.UnixNano(), t.ServiceName}]
		var pct float64
		if t.RequestCount > 0 {
			pct = float64(errCount) * 100.0 / float64(t.RequestCount)
		}
		out = append(out, ServiceErrorRatePoint{
			Timestamp:    t.Timestamp,
			ServiceName:  t.ServiceName,
			RequestCount: int64(t.RequestCount), //nolint:gosec // count fits int64
			ErrorCount:   int64(errCount),       //nolint:gosec // count fits int64
			ErrorPct:     pct,
		})
	}
	return out, nil
}

// p95LatencyTSRawRow is the raw scan target; repo converts uint64 → int64
// for the public p95LatencyTSRow.
type p95LatencyTSRawRow struct {
	Timestamp   time.Time `ch:"timestamp"`
	ServiceName string    `ch:"service_name"`
	SpanCount   uint64    `ch:"span_count"`
}

// GetP95LatencyTimeSeries returns the list of (service_name, bucket) pairs that
// have any root-span traffic. Percentile values are attached in service.go from
// the sketch timeseries. Keeping the CH query as the coverage source means
// services that exist in spans but not yet in the sketch still appear (with
// zero percentiles) rather than silently disappearing.
func (r *ClickHouseRepository) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]p95LatencyTSRow, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	var raw []p95LatencyTSRawRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, fmt.Sprintf(`
		SELECT %s           AS timestamp,
		       service_name,
		       count()      AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC
	`, bucket), dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	if err != nil {
		return nil, err
	}
	rows := make([]p95LatencyTSRow, len(raw))
	for i, r := range raw {
		rows[i] = p95LatencyTSRow{
			Timestamp:   r.Timestamp,
			ServiceName: r.ServiceName,
			SpanCount:   int64(r.SpanCount), //nolint:gosec // count fits int64
		}
	}
	return rows, nil
}

// spanKindPointRaw scans (timestamp, kind_string, span_count) into uint64 so
// the repo can convert to the public int64 field without SQL casts.
type spanKindPointRaw struct {
	Timestamp  time.Time `ch:"timestamp"`
	KindString string    `ch:"kind_string"`
	SpanCount  uint64    `ch:"span_count"`
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	var raw []spanKindPointRaw
	err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, fmt.Sprintf(`
		SELECT %s             AS timestamp,
		       kind_string,
		       count()        AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, kind_string
		ORDER BY timestamp ASC
	`, bucket), dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	if err != nil {
		return nil, err
	}
	rows := make([]SpanKindPoint, len(raw))
	for i, r := range raw {
		rows[i] = SpanKindPoint{
			Timestamp:  r.Timestamp,
			KindString: r.KindString,
			SpanCount:  int64(r.SpanCount), //nolint:gosec // count fits int64
		}
	}
	return rows, nil
}

// errByRouteTotalRow is the per-(bucket, route) totals leg.
type errByRouteTotalRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	HttpRoute    string    `ch:"http_route"`
	RequestCount uint64    `ch:"request_count"`
}

// errByRouteErrorRow is the per-(bucket, route) error-only leg.
type errByRouteErrorRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	HttpRoute  string    `ch:"http_route"`
	ErrorCount uint64    `ch:"error_count"`
}

// GetErrorsByRoute runs a totals scan and an error-only scan in parallel, then
// merges per-(bucket, http_route) in Go. Keeping the two legs separate keeps
// the SELECT list free of combinators.
func (r *ClickHouseRepository) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)

	var totals []errByRouteTotalRow
	var errs []errByRouteErrorRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT %s              AS timestamp,
			       mat_http_route  AS http_route,
			       count()         AS request_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			  AND mat_http_route != ''
			GROUP BY timestamp, http_route
			ORDER BY timestamp ASC
		`, bucket)
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, q, args...)
	})
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT %s              AS timestamp,
			       mat_http_route  AS http_route,
			       count()         AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			  AND mat_http_route != ''
			  AND `+errorPredicate+`
			GROUP BY timestamp, http_route
			ORDER BY timestamp ASC
		`, bucket)
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, q, args...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type key struct {
		ts    int64
		route string
	}
	errIdx := make(map[key]uint64, len(errs))
	for _, e := range errs {
		errIdx[key{e.Timestamp.UnixNano(), e.HttpRoute}] = e.ErrorCount
	}

	out := make([]ErrorByRoutePoint, 0, len(totals))
	for _, t := range totals {
		errCount := errIdx[key{t.Timestamp.UnixNano(), t.HttpRoute}]
		out = append(out, ErrorByRoutePoint{
			Timestamp:    t.Timestamp,
			HttpRoute:    t.HttpRoute,
			RequestCount: int64(t.RequestCount), //nolint:gosec // count fits int64
			ErrorCount:   int64(errCount),       //nolint:gosec // count fits int64
		})
	}
	// Preserve prior secondary ORDER BY: within a timestamp, largest error
	// counts first. Timestamps are already ascending from the totals scan.
	sortErrorByRoute(out)
	return out, nil
}

func sortErrorByRoute(rows []ErrorByRoutePoint) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			a, b := rows[j-1], rows[j]
			// Ascending timestamp, then descending error count.
			if a.Timestamp.Before(b.Timestamp) {
				break
			}
			if a.Timestamp.Equal(b.Timestamp) && a.ErrorCount >= b.ErrorCount {
				break
			}
			rows[j-1], rows[j] = b, a
		}
	}
}

// latencyBreakdownRawRow is the raw sum+count scan target for the latency
// breakdown; the repo converts uint64 span_count → int64 for downstream use.
type latencyBreakdownRawRow struct {
	ServiceName string  `ch:"service_name"`
	TotalMsSum  float64 `ch:"total_ms_sum"`
	SpanCount   uint64  `ch:"span_count"`
}

func (r *ClickHouseRepository) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error) {
	// Mean replaced by sum + count; service.go divides to get the per-service mean.
	var raw []latencyBreakdownRawRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, `
		SELECT
			service_name,
			sum(duration_nano / 1000000.0) AS total_ms_sum,
			count()                        AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY service_name
	`, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	if err != nil {
		return nil, err
	}
	rows := make([]latencyBreakdownRow, len(raw))
	for i, r := range raw {
		rows[i] = latencyBreakdownRow{
			ServiceName: r.ServiceName,
			TotalMsSum:  r.TotalMsSum,
			SpanCount:   int64(r.SpanCount), //nolint:gosec // count fits int64
		}
	}
	return rows, nil
}
