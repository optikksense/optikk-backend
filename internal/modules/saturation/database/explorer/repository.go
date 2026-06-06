package explorer

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Repository interface {
	GetSystemSummariesRaw(ctx context.Context, teamID, startMs, endMs int64) ([]systemSummaryRawDTO, error)
	GetActiveConnectionsBySystem(ctx context.Context, teamID, startMs, endMs int64) (map[string]int64, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type systemSummaryRawDTO struct {
	DBSystem      string    `ch:"db_system"`
	QueryCount    uint64    `ch:"query_count"`
	ErrorCount    uint64    `ch:"error_count"`
	AvgLatencyMs  float64   `ch:"avg_latency_ms"`
	P95Ms         float32   `ch:"p95_ms"`
	ServerAddress string    `ch:"server_address"`
	LastSeen      time.Time `ch:"last_seen"`
}

type connRawRow struct {
	DBSystem string  `ch:"db_system"`
	Avg      float64 `ch:"avg_used"`
}

func (r *ClickHouseRepository) GetSystemSummariesRaw(ctx context.Context, teamID, startMs, endMs int64) ([]systemSummaryRawDTO, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_system                                                                         AS db_system,
		       sum(request_count)                                                                AS query_count,
		       sum(error_count)                                                                  AS error_count,
		       sum(duration_ms_sum) / nullIf(sum(request_count), 0)                              AS avg_latency_ms,
		       quantileTimingMerge(0.95)(latency_state)                                          AS p95_ms,
		       any(server_address)                                                               AS server_address,
		       max(timestamp)                                                                    AS last_seen
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		     AND timestamp BETWEEN @start AND @end
		WHERE db_system != ''
		GROUP BY db_system
		ORDER BY query_count DESC`

	args := filter.SpanArgs(teamID, startMs, endMs)
	var rows []systemSummaryRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "datastoreSystems.GetSystemSummariesRaw", &rows, query, args...)
}

// GetActiveConnectionsBySystem returns active connections by database system.
func (r *ClickHouseRepository) GetActiveConnectionsBySystem(ctx context.Context, teamID, startMs, endMs int64) (map[string]int64, error) {
	const query = `
		SELECT db_system,
		       ifNotFinite(sum(val_sum) / sum(val_count), 0) AS avg_used
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		     AND timestamp   BETWEEN @start AND @end
		WHERE db_connection_state = 'used'
		  AND db_system != ''
		GROUP BY db_system`

	args := filter.MetricArgs(teamID, startMs, endMs, filter.MetricDBConnectionCount)
	var rows []connRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "datastoreSystems.GetActiveConnectionsBySystem", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.DBSystem] = int64(r.Avg + 0.5)
	}
	return out, nil
}
