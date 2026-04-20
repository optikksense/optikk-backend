package overview

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Reads target `observability.spans_rollup_1m` — an AggregatingMergeTree
// populated by the `spans_to_rollup_1m` MV at ingest. State columns store the
// t-digest + sum states; `quantileTDigestMerge(...)` and `sumMerge(...)` merge
// them server-side at query time. Query cost is O(buckets_in_range × dims),
// not O(raw_spans_in_range) — ~100–1000× fewer rows scanned vs. the prior
// quantileTDigest-on-raw shape.
//
// SQL discipline in this file: only `quantileTDigestMerge`, `sumMerge`,
// `toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))`, plain
// column references, and `@name` bindings. No `quantileTDigest(raw)`, no
// `sumIf`/`countIf`/`avgIf`/`avg(col)`, no `toInt64`/`toFloat64`/`toUInt*`
// casts, no `if`/`multiIf`/`CASE`, no `coalesce`/`nullIf` (the coalesce that
// produces `endpoint` happens at MV time, once per ingest, not per query).
const serviceNameFilter = " AND service_name = @serviceName"

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

// intervalMinutesFor returns the adaptive step for bucket aggregation matching
// the prior `timebucket.ExprForColumnTime` semantics. The rollup table is
// 1-minute granular; callers re-aggregate to coarser steps via
// `toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))` at read time.
func intervalMinutesFor(startMs, endMs int64) int64 {
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
	query := `
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       service_name,
		       sumMerge(request_count) AS request_count
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMinutesFor(startMs, endMs)),
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
	query := `
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       service_name,
		       sumMerge(request_count) AS request_count,
		       sumMerge(error_count)   AS error_count
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMinutesFor(startMs, endMs)),
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
	// quantileTDigestMerge indexes into the quantilesTDigestWeightedState by position:
	// the MV stores (0.5, 0.95, 0.99); index 2 is p95. (CH returns a tuple; we pick
	// element 2 with `.2` — 1-based.) Using the positional accessor avoids the need
	// for a second `quantileTDigestMerge` with a different arg.
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       service_name,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`)
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMinutesFor(startMs, endMs)),
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

// serviceMetricRow is the DTO for GetServices + GetSummary. The three
// percentile columns come from a single `quantilesTDigestWeightedMerge` tuple
// (CH returns the quantiles in one pass; Go destructures via three separate
// fields with tuple-element accessors in the SELECT).
type serviceMetricRow struct {
	ServiceName    string  `ch:"service_name"`
	RequestCount   uint64  `ch:"request_count"`
	ErrorCount     uint64  `ch:"error_count"`
	DurationMsSum  float64 `ch:"duration_ms_sum"`
	P50Latency     float64 `ch:"p50_latency"`
	P95Latency     float64 `ch:"p95_latency"`
	P99Latency     float64 `ch:"p99_latency"`
}

func (r *ClickHouseRepository) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricRow, error) {
	query := `
		SELECT service_name,
		       sumMerge(request_count)                                            AS request_count,
		       sumMerge(error_count)                                              AS error_count,
		       sumMerge(duration_ms_sum)                                          AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_latency
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name
		ORDER BY request_count DESC`

	var rows []serviceMetricRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, rollupParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// endpointMetricRow is the DTO for GetTopEndpoints. Endpoint string is
// coalesced at MV time (route → target → name), so the rollup stores it
// directly — no runtime fallback here.
type endpointMetricRow struct {
	ServiceName    string  `ch:"service_name"`
	OperationName  string  `ch:"operation_name"`
	EndpointName   string  `ch:"endpoint"`
	HTTPMethod     string  `ch:"http_method"`
	RequestCount   uint64  `ch:"request_count"`
	ErrorCount     uint64  `ch:"error_count"`
	DurationMsSum  float64 `ch:"duration_ms_sum"`
	P50Latency     float64 `ch:"p50_latency"`
	P95Latency     float64 `ch:"p95_latency"`
	P99Latency     float64 `ch:"p99_latency"`
}

func (r *ClickHouseRepository) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricRow, error) {
	query := `
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
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND endpoint != ''`
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
	query := `
		SELECT '' AS service_name,
		       sumMerge(request_count)                                            AS request_count,
		       sumMerge(error_count)                                              AS error_count,
		       sumMerge(duration_ms_sum)                                          AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99_latency
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`

	var row serviceMetricRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, rollupParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return serviceMetricRow{}, err
	}
	return row, nil
}
