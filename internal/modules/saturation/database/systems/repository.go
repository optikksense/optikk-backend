package systems

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)

	// last_seen is derived from bucket_ts (the rollup's minute-aligned bucket) —
	// within one-minute precision of the raw max(timestamp) it replaced.
	query := fmt.Sprintf(`
		SELECT
		    db_system                                                                   AS db_system,
		    toInt64(sumMerge(hist_count))                                               AS span_count,
		    toInt64(sumMergeIf(hist_count, notEmpty(error_type)))                       AS error_count,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) * 1000  AS avg_latency_ms,
		    toInt64(sumMerge(hist_count))                                               AS query_count,
		    any(server_address)                                                         AS server_address,
		    max(bucket_ts)                                                              AS last_seen
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND notEmpty(db_system)
		GROUP BY db_system
		ORDER BY span_count DESC
	`, table)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var dtos []detectedSystemDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "systems.GetDetectedSystems", &dtos, query, args...); err != nil {
		return nil, err
	}

	out := make([]DetectedSystem, len(dtos))
	for i, d := range dtos {
		out[i] = DetectedSystem{
			DBSystem:	d.DBSystem,
			SpanCount:	d.SpanCount,
			ErrorCount:	d.ErrorCount,
			AvgLatencyMs:	d.AvgLatencyMs,
			QueryCount:	d.QueryCount,
			ServerAddress:	d.ServerAddress,
			LastSeen:	d.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}
