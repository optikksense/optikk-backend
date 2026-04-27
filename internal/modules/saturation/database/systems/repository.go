package systems

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error)
	GetSystemSummaries(ctx context.Context, teamID int64, startMs, endMs int64) ([]SystemSummary, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	table := "observability.metrics"

	// last_seen is derived from ts_bucket (the rollup's minute-aligned bucket) —
	// within one-minute precision of the raw max(timestamp) it replaced.
	query := fmt.Sprintf(`
		SELECT
		    db_system                                                                   AS db_system,
		    toInt64(sum(hist_count))                                               AS span_count,
		    toInt64(sumMergeIf(hist_count, notEmpty(error_type)))                       AS error_count,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) * 1000  AS avg_latency_ms,
		    toInt64(sum(hist_count))                                               AS query_count,
		    any(server_address)                                                         AS server_address,
		    max(ts_bucket)                                                              AS last_seen
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND notEmpty(db_system)
		GROUP BY db_system
		ORDER BY span_count DESC
	`, table)

	args := shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration)

	var dtos []detectedSystemDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "systems.GetDetectedSystems", &dtos, query, args...); err != nil {
		return nil, err
	}

	out := make([]DetectedSystem, len(dtos))
	for i, d := range dtos {
		out[i] = DetectedSystem{
			DBSystem:      d.DBSystem,
			SpanCount:     d.SpanCount,
			ErrorCount:    d.ErrorCount,
			AvgLatencyMs:  d.AvgLatencyMs,
			QueryCount:    d.QueryCount,
			ServerAddress: d.ServerAddress,
			LastSeen:      d.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemSummaries(ctx context.Context, teamID int64, startMs, endMs int64) ([]SystemSummary, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
			db_system                                                                    AS db_system,
			toInt64(sumMergeIf(hist_count, metric_name = @metricName))                  AS query_count,
			toInt64(sumMergeIf(hist_count, metric_name = @metricName AND error_type != '')) AS error_count,
			if(sum(hist_count) > 0,
			   (toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_digest)[1]) * 1000.0),
			   0.0)                                                                      AS avg_latency_ms,
			toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_digest)[2]) * 1000.0 AS p95_latency_ms,
			toInt64(round(toFloat64(sumMergeIf(value_sum, metric_name = @connMetric AND db_connection_state = 'used')))) AS active_connections,
			''                                                                           AS server_address,
			max(ts_bucket)                                                               AS last_seen
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND notEmpty(db_system)
		GROUP BY db_system
		ORDER BY query_count DESC
	`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", shared.MetricDBOperationDuration),
		clickhouse.Named("connMetric", shared.MetricDBConnectionCount),
	}
	var dtos []systemSummaryDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "systems.GetSystemSummaries", &dtos, query, args...); err != nil {
		return nil, err
	}
	out := make([]SystemSummary, len(dtos))
	for i, dto := range dtos {
		out[i] = SystemSummary{
			DBSystem:          dto.DBSystem,
			QueryCount:        dto.QueryCount,
			ErrorCount:        dto.ErrorCount,
			AvgLatencyMs:      dto.AvgLatencyMs,
			P95LatencyMs:      dto.P95LatencyMs,
			ActiveConnections: dto.ActiveConnections,
			ServerAddress:     dto.ServerAddress,
			LastSeen:          dto.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}
