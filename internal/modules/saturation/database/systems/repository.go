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
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	serverAttr := shared.AttrString(shared.AttrServerAddress)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	// Percentiles + avg come from sketch.Querier (DbOpLatency) in the service
	// layer. SQL emits only raw counts + sum/count for in-Go avg.
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                             AS db_system,
		    toInt64(sum(hist_count))                                                       AS span_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                       AS error_count,
		    sum(hist_sum)                                                                  AS latency_sum,
		    toInt64(sum(hist_count))                                                       AS latency_count,
		    toInt64(sum(hist_count))                                                       AS query_count,
		    any(%s)                                                                        AS server_address,
		    max(timestamp)                                                                 AS last_seen
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		GROUP BY db_system
		ORDER BY span_count DESC
	`,
		systemAttr,
		errorAttr,
		serverAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		systemAttr,
	)

	var dtos []detectedSystemDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, shared.BaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}

	out := make([]DetectedSystem, len(dtos))
	for i, d := range dtos {
		out[i] = DetectedSystem{
			DBSystem:      d.DBSystem,
			SpanCount:     d.SpanCount,
			ErrorCount:    d.ErrorCount,
			LatencySum:    d.LatencySum,
			LatencyCount:  d.LatencyCount,
			QueryCount:    d.QueryCount,
			ServerAddress: d.ServerAddress,
			LastSeen:      d.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}
