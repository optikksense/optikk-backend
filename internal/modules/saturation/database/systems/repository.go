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
	GetSystemSummaries(ctx context.Context, teamID int64, startMs, endMs int64) ([]SystemSummary, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	tier := rollup.For(shared.DBHistRollupPrefix, startMs, endMs)
	table := tier.Table

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
	table := rollup.For(shared.DBHistRollupPrefix, startMs, endMs).Table
	query := fmt.Sprintf(`
		WITH ops AS (
			SELECT
				db_system AS db_system,
				toInt64(sumMerge(hist_count)) AS query_count,
				if(sumMerge(hist_count) > 0, (sumMerge(hist_sum) / toFloat64(sumMerge(hist_count))) * 1000.0, 0.0) AS avg_latency_ms,
				toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) * 1000.0 AS p95_latency_ms,
				any(server_address) AS server_address,
				max(bucket_ts) AS last_seen
			FROM %s
			WHERE team_id = @teamID
			  AND bucket_ts BETWEEN @start AND @end
			  AND metric_name = @metricName
			  AND notEmpty(db_system)
			GROUP BY db_system
		),
		errors AS (
			SELECT
				db_system AS db_system,
				toInt64(sumMerge(hist_count)) AS error_count
			FROM %s
			WHERE team_id = @teamID
			  AND bucket_ts BETWEEN @start AND @end
			  AND metric_name = @metricName
			  AND error_type != ''
			  AND notEmpty(db_system)
			GROUP BY db_system
		),
		connections AS (
			SELECT
				db_system AS db_system,
				toInt64(round(toFloat64(sumMerge(value_sum)))) AS active_connections
			FROM %s
			WHERE team_id = @teamID
			  AND bucket_ts BETWEEN @start AND @end
			  AND metric_name = @connMetric
			  AND db_connection_state = 'used'
			  AND notEmpty(db_system)
			GROUP BY db_system
		)
		SELECT
			ops.db_system AS db_system,
			ops.query_count AS query_count,
			coalesce(errors.error_count, toInt64(0)) AS error_count,
			ops.avg_latency_ms AS avg_latency_ms,
			ops.p95_latency_ms AS p95_latency_ms,
			coalesce(connections.active_connections, toInt64(0)) AS active_connections,
			ops.server_address AS server_address,
			ops.last_seen AS last_seen
		FROM ops
		LEFT JOIN errors ON ops.db_system = errors.db_system
		LEFT JOIN connections ON ops.db_system = connections.db_system
		ORDER BY ops.query_count DESC
	`, table, table, table)
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
