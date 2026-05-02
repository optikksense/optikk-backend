package systems

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Repository interface {
	GetDetectedSystems(ctx context.Context, teamID, startMs, endMs int64) ([]detectedSystemRawDTO, error)
	GetSystemSummariesRaw(ctx context.Context, teamID, startMs, endMs int64) ([]systemSummaryRawDTO, error)
	GetActiveConnectionsBySystem(ctx context.Context, teamID, startMs, endMs int64) (map[string]int64, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type detectedSystemRawDTO struct {
	DBSystem      string    `ch:"db_system"`
	SpanCount     int64     `ch:"span_count"`
	ErrorCount    int64     `ch:"error_count"`
	AvgLatencyMs  float64   `ch:"avg_latency_ms"`
	ServerAddress string    `ch:"server_address"`
	LastSeen      time.Time `ch:"last_seen"`
}

type systemSummaryRawDTO struct {
	DBSystem      string    `ch:"db_system"`
	QueryCount    int64     `ch:"query_count"`
	ErrorCount    int64     `ch:"error_count"`
	AvgLatencyMs  float64   `ch:"avg_latency_ms"`
	P95Ms         float64   `ch:"p95_ms"`
	ServerAddress string    `ch:"server_address"`
	LastSeen      time.Time `ch:"last_seen"`
}

type connRawRow struct {
	DBSystem string  `ch:"db_system"`
	Avg      float64 `ch:"avg_used"`
}

func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID, startMs, endMs int64) ([]detectedSystemRawDTO, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_system                                                                       AS db_system,
		       toInt64(sum(request_count))                                                     AS span_count,
		       toInt64(sum(error_count))                                                       AS error_count,
		       toFloat64(sum(duration_ms_sum) / nullIf(toFloat64(sum(request_count)), 0))      AS avg_latency_ms,
		       any(server_address)                                                             AS server_address,
		       max(timestamp)                                                                  AS last_seen
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''
		GROUP BY db_system
		ORDER BY span_count DESC`

	args := filter.SpanArgs(teamID, startMs, endMs)
	var rows []detectedSystemRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "systems.GetDetectedSystems", &rows, query, args...)
}

func (r *ClickHouseRepository) GetSystemSummariesRaw(ctx context.Context, teamID, startMs, endMs int64) ([]systemSummaryRawDTO, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_system                                                                         AS db_system,
		       toInt64(sum(request_count))                                                       AS query_count,
		       toInt64(sum(error_count))                                                         AS error_count,
		       toFloat64(sum(duration_ms_sum) / nullIf(toFloat64(sum(request_count)), 0))        AS avg_latency_ms,
		       quantileTimingMerge(0.95)(latency_state)                                          AS p95_ms,
		       any(server_address)                                                               AS server_address,
		       max(timestamp)                                                                    AS last_seen
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''
		GROUP BY db_system
		ORDER BY query_count DESC`

	args := filter.SpanArgs(teamID, startMs, endMs)
	var rows []systemSummaryRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "systems.GetSystemSummariesRaw", &rows, query, args...)
}

func (r *ClickHouseRepository) GetActiveConnectionsBySystem(ctx context.Context, teamID, startMs, endMs int64) (map[string]int64, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT attributes.'db.system'::String   AS db_system,
		       toFloat64(sum(val_sum) / sum(val_count))            AS avg_used
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    = @metricName
		WHERE timestamp BETWEEN @start AND @end
		  AND attributes.'db.client.connection.state'::String = 'used'
		  AND attributes.'db.system'::String != ''
		GROUP BY db_system`

	args := filter.MetricArgs(teamID, startMs, endMs, filter.MetricDBConnectionCount)
	var rows []connRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "systems.GetActiveConnectionsBySystem", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.DBSystem] = int64(r.Avg + 0.5)
	}
	return out, nil
}
