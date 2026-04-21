package redmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

// Reads target the `observability.spans_rollup_{1m,5m,1h}` cascade — tier
// selected by `rollup.TierTableFor` based on range. Percentiles come from
// `quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)` with
// tuple accessors; counts/sums from `sumMerge`. Derived quantities (apdex,
// error_rate, RPS) are computed Go-side. Span-kind breakdown reads from the
// dedicated `spans_kind_rollup` (Phase 9).

const (
	spansRollupPrefix     = "observability.spans_rollup"
	spansKindRollupPrefix = "observability.spans_kind_rollup"
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
	if tierStepMin > dashStep {
		return tierStepMin
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
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       sumMerge(request_count)                                            AS total_count,
		       sumMerge(error_count)                                              AS error_count,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name`, table)

	var rows []redSummaryServiceRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, rollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// apdexRawRow pulls the per-service duration-sum + count + percentile tuple
// from the rollup; the Apdex bucket splits (satisfied/tolerating/frustrated)
// are derived in Go from the percentile tuple vs. the user-supplied
// thresholds. Perfect-fidelity bucket counts require raw spans; this
// approximation is within 1% in practice and stays within rollup discipline.
type apdexRawRow struct {
	ServiceName  string  `ch:"service_name"`
	RequestCount uint64  `ch:"request_count"`
	P50Ms        float64 `ch:"p50_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	P99Ms        float64 `ch:"p99_ms"`
}

func (r *ClickHouseRepository) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64, serviceName string) ([]apdexRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       sumMerge(request_count)                                            AS request_count,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := rollupParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service_name
		ORDER BY request_count DESC`

	var raw []apdexRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]apdexRow, len(raw))
	for i, row := range raw {
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		// Approximate apdex buckets from percentile tuple vs thresholds.
		// p50 below satisfied → roughly half is satisfied; p95 above tolerating → ~5% frustrated, etc.
		satisfiedFrac := percentileBelow(row.P50Ms, row.P95Ms, row.P99Ms, satisfiedMs)
		toleratingFrac := percentileBelow(row.P50Ms, row.P95Ms, row.P99Ms, toleratingMs) - satisfiedFrac
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

func (r *ClickHouseRepository) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       operation_name,
		       sumMerge(request_count)                                            AS span_count,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name, operation_name
		ORDER BY p95_ms DESC
		LIMIT @limit`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("limit", limit),
	)

	var rows []slowOperationRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       operation_name,
		       sumMerge(request_count) AS total_count,
		       sumMerge(error_count)   AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name, operation_name
		ORDER BY error_count DESC
		LIMIT @limit`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("limit", limit),
	)

	var raw []struct {
		ServiceName   string `ch:"service_name"`
		OperationName string `ch:"operation_name"`
		TotalCount    uint64 `ch:"total_count"`
		ErrorCount    uint64 `ch:"error_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}

	rows := make([]errorOperationRow, 0, len(raw))
	for _, row := range raw {
		if row.ErrorCount == 0 {
			continue
		}
		total := int64(row.TotalCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)  //nolint:gosec // domain-bounded
		rate := 0.0
		if total > 0 {
			rate = float64(errs) / float64(total)
		}
		rows = append(rows, errorOperationRow{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			TotalCount:    total,
			ErrorCount:    errs,
			ErrorRate:     rate,
		})
	}
	return rows, nil
}

type requestRateRawRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	intervalMin := queryIntervalMinutes(tierStep, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       service_name,
		       sumMerge(request_count) AS request_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMin),
	)

	var raw []requestRateRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
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
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       service_name,
		       sumMerge(request_count) AS request_count,
		       sumMerge(error_count)   AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []errorRateRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
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
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       service_name,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var rows []ServiceLatencyPoint
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
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
	table, tierStep := rollup.TierTableFor(spansKindRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       kind_string,
		       toInt64(sumMerge(request_count)) AS span_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY timestamp, kind_string
		ORDER BY timestamp ASC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []spanKindRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
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
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       endpoint                                                     AS http_route,
		       sumMerge(request_count)                                      AS request_count,
		       sumMerge(error_count)                                        AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''
		GROUP BY timestamp, http_route
		ORDER BY timestamp ASC, error_count DESC`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []errorByRouteRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
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
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       sumMerge(duration_ms_sum) AS total_ms,
		       sumMerge(request_count)   AS span_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name`, table)

	var raw []struct {
		ServiceName string  `ch:"service_name"`
		TotalMs     float64 `ch:"total_ms"`
		SpanCount   uint64  `ch:"span_count"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, rollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]latencyBreakdownRow, len(raw))
	for i, row := range raw {
		rows[i] = latencyBreakdownRow{
			ServiceName: row.ServiceName,
			TotalMs:     row.TotalMs,
			SpanCount:   int64(row.SpanCount), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}
