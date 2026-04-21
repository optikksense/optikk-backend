package overview

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

// Reads target the `observability.spans_rollup_{1m,5m,1h}` cascade — Phase 6
// adds the `_5m` and `_1h` tiers so long-range queries scan a few hundred
// coarser rollup rows instead of 10k+ 1-minute buckets. `rollup.TierTableFor`
// picks the tier by range; `@intervalMin` defines the query-time step (>= the
// tier's native step when the dashboard wants coarser buckets).
//
// SQL discipline: only `quantilesTDigestWeightedMerge`, `sumMerge`,
// `toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))`, plain column
// references, and `@name` bindings.
const (
	serviceNameFilter = " AND service_name = @serviceName"
	spansRollupPrefix = "observability.spans_rollup"
)

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

// queryIntervalMinutes returns the step (in minutes) for the query-time
// `toStartOfInterval` group-by. Returns max(tierStep, dashboardStep) so the
// step is never finer than the tier's native resolution. The dashboard steps
// 1/5/60/1440 match the pre-Phase-6 `intervalMinutesFor` shape.
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

// rollupParams returns the named parameters common to rollup reads: teamID +
// DateTime-aligned start/end. The rollup table's primary key is
// (team_id, bucket_ts, ...), so bucket_ts filtering drives part pruning.
func rollupParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// requestRateRow is the DTO for GetRequestRate.
type requestRateRow struct {
	Timestamp    time.Time `ch:"time_bucket"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]requestRateRow, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       service_name,
		       sumMerge(request_count) AS request_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY time_bucket, service_name
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`

	var rows []requestRateRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// errorRateRow is the DTO for GetErrorRate. `ErrorRate` is derived in the
// service from RequestCount / ErrorCount — no SQL-side conditional division.
type errorRateRow struct {
	Timestamp    time.Time `ch:"time_bucket"`
	ServiceName  string    `ch:"service_name"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorRateRow, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       service_name,
		       sumMerge(request_count) AS request_count,
		       sumMerge(error_count)   AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY time_bucket, service_name
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`

	var rows []errorRateRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// p95LatencyRow is the DTO for GetP95Latency.
type p95LatencyRow struct {
	Timestamp   time.Time `ch:"time_bucket"`
	ServiceName string    `ch:"service_name"`
	P95         float64   `ch:"p95"`
}

func (r *ClickHouseRepository) GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]p95LatencyRow, error) {
	table, tierStep := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       service_name,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY time_bucket, service_name
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`

	var rows []p95LatencyRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// serviceMetricRow is the DTO for GetServices + GetSummary.
type serviceMetricRow struct {
	ServiceName   string  `ch:"service_name"`
	RequestCount  uint64  `ch:"request_count"`
	ErrorCount    uint64  `ch:"error_count"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
	P50Latency    float64 `ch:"p50_latency"`
	P95Latency    float64 `ch:"p95_latency"`
	P99Latency    float64 `ch:"p99_latency"`
}

func (r *ClickHouseRepository) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       sumMerge(request_count)                                            AS request_count,
		       sumMerge(error_count)                                              AS error_count,
		       sumMerge(duration_ms_sum)                                          AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_latency
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name
		ORDER BY request_count DESC`, table)

	var rows []serviceMetricRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, rollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// endpointMetricRow is the DTO for GetTopEndpoints.
type endpointMetricRow struct {
	ServiceName   string  `ch:"service_name"`
	OperationName string  `ch:"operation_name"`
	EndpointName  string  `ch:"endpoint"`
	HTTPMethod    string  `ch:"http_method"`
	RequestCount  uint64  `ch:"request_count"`
	ErrorCount    uint64  `ch:"error_count"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
	P50Latency    float64 `ch:"p50_latency"`
	P95Latency    float64 `ch:"p95_latency"`
	P99Latency    float64 `ch:"p99_latency"`
}

func (r *ClickHouseRepository) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       operation_name,
		       endpoint,
		       http_method,
		       sumMerge(request_count)                                            AS request_count,
		       sumMerge(error_count)                                              AS error_count,
		       sumMerge(duration_ms_sum)                                          AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_latency
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''`, table)
	args := rollupParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY service_name, operation_name, endpoint, http_method
		ORDER BY request_count DESC
		LIMIT 100`

	var rows []endpointMetricRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (serviceMetricRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT '' AS service_name,
		       sumMerge(request_count)                                            AS request_count,
		       sumMerge(error_count)                                              AS error_count,
		       sumMerge(duration_ms_sum)                                          AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_latency
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)

	var row serviceMetricRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, rollupParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return serviceMetricRow{}, err
	}
	return row, nil
}
