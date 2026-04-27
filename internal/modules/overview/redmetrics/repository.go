package redmetrics

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error)
	GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error)
	GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error)
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error)
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

// queryIntervalMinutes chooses a dashboard step based on window width, then
// coarsens to the tier's native step if needed. Routes through
// 1 for the tier floor.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	return dashStep
}

func rollupParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service,
		       count()                                            AS total_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                              AS error_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) AS p50_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY service`, table)

	var raw []struct {
		ServiceName string  `ch:"service"`
		TotalCount  uint64  `ch:"total_count"`
		ErrorCount  uint64  `ch:"error_count"`
		P50Ms       float64 `ch:"p50_ms"`
		P95Ms       float64 `ch:"p95_ms"`
		P99Ms       float64 `ch:"p99_ms"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetSummary", &raw, query, rollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}

	rows := make([]redSummaryServiceRow, len(raw))
	for i, row := range raw {
		rows[i] = redSummaryServiceRow{
			ServiceName: row.ServiceName,
			TotalCount:  row.TotalCount,
			ErrorCount:  row.ErrorCount,
			P50Ms:       utils.SanitizeFloat(row.P50Ms),
			P95Ms:       utils.SanitizeFloat(row.P95Ms),
			P99Ms:       utils.SanitizeFloat(row.P99Ms),
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service,
		       count()                                            AS request_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) AS p50_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end`, table)
	args := rollupParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += ` AND service = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service
		ORDER BY request_count DESC`

	var raw []struct {
		ServiceName  string  `ch:"service"`
		RequestCount uint64  `ch:"request_count"`
		P50Ms        float64 `ch:"p50_ms"`
		P95Ms        float64 `ch:"p95_ms"`
		P99Ms        float64 `ch:"p99_ms"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetApdex", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]apdexRow, len(raw))
	for i, row := range raw {
		p50 := utils.SanitizeFloat(row.P50Ms)
		p95 := utils.SanitizeFloat(row.P95Ms)
		p99 := utils.SanitizeFloat(row.P99Ms)
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		// Approximate apdex buckets from percentile tuple vs thresholds.
		// p50 below satisfied → roughly half is satisfied; p95 above tolerating → ~5% frustrated, etc.
		satisfiedFrac := percentileBelow(p50, p95, p99, satisfiedMs)
		toleratingFrac := percentileBelow(p50, p95, p99, toleratingMs) - satisfiedFrac
		if toleratingFrac < 0 {
			toleratingFrac = 0
		}
		frustratedFrac := 1.0 - satisfiedFrac - toleratingFrac
		if frustratedFrac < 0 {
			frustratedFrac = 0
		}
		rows[i] = apdexRow{
			ServiceName: row.ServiceName,
			Satisfied:   int64(float64(total) * satisfiedFrac),
			Tolerating:  int64(float64(total) * toleratingFrac),
			Frustrated:  int64(float64(total) * frustratedFrac),
			TotalCount:  total,
		}
	}
	return rows, nil
}

// percentileBelow estimates P(duration <= threshold) from a 3-point percentile
// sketch (p50, p95, p99). Uses piecewise-linear interpolation between
// percentile anchors — enough fidelity for Apdex bucket estimation.
func percentileBelow(p50, p95, p99, threshold float64) float64 {
	switch {
	case threshold <= 0:
		return 0
	case threshold >= p99:
		return 1.0
	case threshold >= p95:
		if p99 <= p95 {
			return 0.95
		}
		return 0.95 + 0.04*(threshold-p95)/(p99-p95)
	case threshold >= p50:
		if p95 <= p50 {
			return 0.5
		}
		return 0.5 + 0.45*(threshold-p50)/(p95-p50)
	default:
		if p50 <= 0 {
			return 0.5
		}
		return 0.5 * threshold / p50
	}
}

// slowOpsCandidatePoolMultiplier is the oversample factor used to build the
// candidate pool in GetTopSlowOperations. Operations ranked below
// `limit * multiplier` by traffic cannot be in the final top-N by p95 for any
// reasonable dashboard — they are dropped before the expensive tDigest merge
// so ClickHouse does not compute percentiles for every cardinality group.
const slowOpsCandidatePoolMultiplier = 20

func (r *ClickHouseRepository) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error) {
	table := "observability.spans"
	// Two-step: the CTE picks the top-K by request count using only the cheap
	// `sum` state column, then the outer query computes tDigest percentiles
	// for that bounded candidate set. Avoids ORDER BY on a computed tDigest
	// quantile, which would otherwise force per-group percentile computation
	// across every (service, operation) pair in the rollup.
	query := fmt.Sprintf(`
		WITH candidates AS (
		    SELECT service,
		           operation_name,
		           count() AS span_count
		    FROM %[1]s
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @start AND @end
		    GROUP BY service, operation_name
		    ORDER BY span_count DESC
		    LIMIT @candidateLimit
		)
		SELECT s.service,
		       s.operation_name,
		       sum(s.request_count)                                            AS span_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(s.latency_ms_digest)[1]) AS p50_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(s.latency_ms_digest)[2]) AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(s.latency_ms_digest)[3]) AS p99_ms
		FROM %[1]s s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket BETWEEN @start AND @end
		  AND (s.service, s.operation_name) IN (SELECT service, operation_name FROM candidates)
		GROUP BY s.service, s.operation_name
		ORDER BY p95_ms DESC
		LIMIT @limit`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("limit", limit),
		clickhouse.Named("candidateLimit", limit*slowOpsCandidatePoolMultiplier),
	)

	var raw []struct {
		ServiceName   string  `ch:"service"`
		OperationName string  `ch:"operation_name"`
		SpanCount     uint64  `ch:"span_count"`
		P50Ms         float64 `ch:"p50_ms"`
		P95Ms         float64 `ch:"p95_ms"`
		P99Ms         float64 `ch:"p99_ms"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetTopSlowOperations", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]slowOperationRow, len(raw))
	for i, row := range raw {
		rows[i] = slowOperationRow{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			SpanCount:     row.SpanCount,
			P50Ms:         utils.SanitizeFloat(row.P50Ms),
			P95Ms:         utils.SanitizeFloat(row.P95Ms),
			P99Ms:         utils.SanitizeFloat(row.P99Ms),
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service,
		       operation_name,
		       count()                                                         AS total_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                                           AS err_count,
		       toFloat64(err_count) / nullIf(toFloat64(total_count), 0) AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY service, operation_name
		HAVING err_count > 0
		ORDER BY err_count DESC
		LIMIT @limit`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("limit", limit),
	)

	var raw []struct {
		ServiceName   string   `ch:"service"`
		OperationName string   `ch:"operation_name"`
		TotalCount    uint64   `ch:"total_count"`
		ErrorCount    uint64   `ch:"err_count"`
		ErrorRate     *float64 `ch:"error_rate"`
	}
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "redmetrics.GetTopErrorOperations", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]errorOperationRow, 0, len(raw))
	for _, row := range raw {
		rate := 0.0
		if row.ErrorRate != nil && !math.IsNaN(*row.ErrorRate) {
			rate = *row.ErrorRate
		}
		rows = append(rows, errorOperationRow{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			TotalCount:    int64(row.TotalCount), //nolint:gosec // domain-bounded
			ErrorCount:    int64(row.ErrorCount), //nolint:gosec // domain-bounded
			ErrorRate:     rate,
		})
	}
	return rows, nil
}

type requestRateRawRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	ServiceName  string    `ch:"service"`
	RequestCount uint64    `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	table := "observability.spans"
	tierStep := int64(1)
	intervalMin := queryIntervalMinutes(tierStep, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS timestamp,
		       service,
		       count() AS request_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY timestamp, service
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMin),
	)

	var raw []requestRateRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetRequestRateTimeSeries", &raw, query, args...); err != nil {
		return nil, err
	}

	intervalSec := float64(intervalMin * 60)
	rows := make([]ServiceRatePoint, len(raw))
	for i, row := range raw {
		rps := 0.0
		if intervalSec > 0 {
			rps = float64(row.RequestCount) / intervalSec
		}
		rows[i] = ServiceRatePoint{
			Timestamp:   row.Timestamp,
			ServiceName: row.ServiceName,
			RPS:         rps,
		}
	}
	return rows, nil
}

type errorRateRawRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	ServiceName  string    `ch:"service"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS timestamp,
		       service,
		       count() AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)   AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY timestamp, service
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []errorRateRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetErrorRateTimeSeries", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]ServiceErrorRatePoint, len(raw))
	for i, row := range raw {
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
		pct := 0.0
		if total > 0 {
			pct = float64(errs) * 100.0 / float64(total)
		}
		rows[i] = ServiceErrorRatePoint{
			Timestamp:    row.Timestamp,
			ServiceName:  row.ServiceName,
			RequestCount: total,
			ErrorCount:   errs,
			ErrorPct:     pct,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS timestamp,
		       service,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_ms
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY timestamp, service
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []struct {
		Timestamp   time.Time `ch:"timestamp"`
		ServiceName string    `ch:"service"`
		P95Ms       float64   `ch:"p95_ms"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetP95LatencyTimeSeries", &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]ServiceLatencyPoint, len(raw))
	for i, row := range raw {
		rows[i] = ServiceLatencyPoint{
			Timestamp:   row.Timestamp,
			ServiceName: row.ServiceName,
			P95Ms:       utils.SanitizeFloat(row.P95Ms),
		}
	}
	return rows, nil
}

// GetSpanKindBreakdown queries raw spans — `kind_string` is not a rollup
// dimension. Bounded by time range + team_id part-pruning; grouping count is
// tiny (handful of distinct kinds × bucket count), so scan stays cheap.
type spanKindRawRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	KindString string    `ch:"kind_string"`
	SpanCount  uint64    `ch:"span_count"`
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS timestamp,
		       kind_string,
		       toInt64(count()) AS span_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY timestamp, kind_string
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []spanKindRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetSpanKindBreakdown", &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]SpanKindPoint, len(raw))
	for i, row := range raw {
		rows[i] = SpanKindPoint{
			Timestamp:  row.Timestamp,
			KindString: row.KindString,
			SpanCount:  int64(row.SpanCount), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

type errorByRouteRawRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	HttpRoute    string    `ch:"http_route"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	// `endpoint` in the rollup is coalesce(route, target, name) for root spans.
	// Close enough to mat_http_route for the errors-by-route panel; excludes
	// empty endpoints.
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS timestamp,
		       operation_name                                               AS http_route,
		       count()                                      AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                        AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND operation_name != ''
		GROUP BY timestamp, http_route
		ORDER BY timestamp ASC, error_count DESC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []errorByRouteRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetErrorsByRoute", &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]ErrorByRoutePoint, len(raw))
	for i, row := range raw {
		rows[i] = ErrorByRoutePoint{
			Timestamp:    row.Timestamp,
			HttpRoute:    row.HttpRoute,
			RequestCount: int64(row.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:   int64(row.ErrorCount),   //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service,
		       sum(duration_nano / 1000000.0) AS total_ms,
		       count()   AS span_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY service`, table)

	var raw []struct {
		ServiceName string  `ch:"service"`
		TotalMs     float64 `ch:"total_ms"`
		SpanCount   uint64  `ch:"span_count"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "redmetrics.GetLatencyBreakdown", &raw, query, rollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]latencyBreakdownRow, len(raw))
	for i, row := range raw {
		rows[i] = latencyBreakdownRow{
			ServiceName: row.ServiceName,
			TotalMs:     utils.SanitizeFloat(row.TotalMs),
			SpanCount:   int64(row.SpanCount), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}
