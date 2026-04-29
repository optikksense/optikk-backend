package slo

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// BurnDownPoint represents a single point on the error budget burn-down chart.
type BurnDownPoint struct {
	Timestamp               string  `json:"timestamp"`
	ErrorBudgetRemainingPct float64 `json:"error_budget_remaining_pct"`
	CumulativeErrorCount    int64   `json:"cumulative_error_count"`
	CumulativeRequestCount  int64   `json:"cumulative_request_count"`
}

// BurnRate holds the current fast and slow burn rates.
type BurnRate struct {
	FastBurnRate    float64 `json:"fast_burn_rate"`
	SlowBurnRate    float64 `json:"slow_burn_rate"`
	FastWindow      string  `json:"fast_window"`
	SlowWindow      string  `json:"slow_window"`
	BudgetRemaining float64 `json:"budget_remaining_pct"`
}

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (SummaryRow, error)
	GetSummaryByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (SummaryRow, error)
	GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeSliceRow, error)
	GetTimeSeriesByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSliceRow, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64) ([]BurnDownRow, error)
	GetBurnDownByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownRow, error)
	ErrorRateForWindow(ctx context.Context, teamID int64, sinceMs, untilMs int64) (WindowCountsRow, error)
	ErrorRateForWindowByService(ctx context.Context, teamID int64, sinceMs, untilMs int64, serviceName string) (WindowCountsRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type SummaryRow struct {
	RequestCount  uint64  `ch:"request_count"`
	ErrorCount    uint64  `ch:"error_count"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
	P95LatencyMs  float32 `ch:"p95_latency_ms"`
}

type TimeSliceRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
}

type BurnDownRow struct {
	TimeBucket   time.Time `ch:"time_bucket"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

type WindowCountsRow struct {
	RequestCount uint64 `ch:"request_count"`
	ErrorCount   uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (SummaryRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count,
		       sum(duration_nano / 1000000.0)                                       AS duration_ms_sum,
		       quantileTiming(0.95)(duration_nano / 1000000.0)                      AS p95_latency_ms
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end`
	var row SummaryRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "slo.GetSummary",
		&row, query, spanArgs(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetSummaryByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (SummaryRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count,
		       sum(duration_nano / 1000000.0)                                       AS duration_ms_sum,
		       quantileTiming(0.95)(duration_nano / 1000000.0)                      AS p95_latency_ms
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @serviceName`
	args := append(spanArgs(teamID, startMs, endMs), clickhouse.Named("serviceName", serviceName))
	var row SummaryRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "slo.GetSummaryByService",
		&row, query, args...)
}

func (r *ClickHouseRepository) GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeSliceRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket)                                                AS time_bucket,
		       count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count,
		       sum(duration_nano / 1000000.0)                                       AS duration_ms_sum
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY ts_bucket
		ORDER BY time_bucket ASC`
	var rows []TimeSliceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slo.GetTimeSeries",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetTimeSeriesByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSliceRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT toDateTime(ts_bucket)                                                AS time_bucket,
		       count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count,
		       sum(duration_nano / 1000000.0)                                       AS duration_ms_sum
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @serviceName
		GROUP BY ts_bucket
		ORDER BY time_bucket ASC`
	args := append(spanArgs(teamID, startMs, endMs), clickhouse.Named("serviceName", serviceName))
	var rows []TimeSliceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slo.GetTimeSeriesByService",
		&rows, query, args...)
}

func (r *ClickHouseRepository) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64) ([]BurnDownRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket)                                                AS time_bucket,
		       count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY ts_bucket
		ORDER BY time_bucket ASC`
	var rows []BurnDownRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slo.GetBurnDown",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetBurnDownByService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT toDateTime(ts_bucket)                                                AS time_bucket,
		       count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @serviceName
		GROUP BY ts_bucket
		ORDER BY time_bucket ASC`
	args := append(spanArgs(teamID, startMs, endMs), clickhouse.Named("serviceName", serviceName))
	var rows []BurnDownRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slo.GetBurnDownByService",
		&rows, query, args...)
}

func (r *ClickHouseRepository) ErrorRateForWindow(ctx context.Context, teamID int64, sinceMs, untilMs int64) (WindowCountsRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end`
	var row WindowCountsRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "slo.ErrorRateForWindow",
		&row, query, spanArgs(teamID, sinceMs, untilMs)...)
}

func (r *ClickHouseRepository) ErrorRateForWindowByService(ctx context.Context, teamID int64, sinceMs, untilMs int64, serviceName string) (WindowCountsRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT count()                                                              AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)    AS error_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service = @serviceName`
	args := append(spanArgs(teamID, sinceMs, untilMs), clickhouse.Named("serviceName", serviceName))
	var row WindowCountsRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "slo.ErrorRateForWindowByService",
		&row, query, args...)
}

func spanArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
