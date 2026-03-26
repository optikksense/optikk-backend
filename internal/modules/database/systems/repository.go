package systems

import (
	"context"
	"fmt"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/database/internal/shared"
)

type Repository interface {
	GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	serverAttr := shared.AttrString(shared.AttrServerAddress)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                             AS db_system,
		    toInt64(sum(hist_count))                                                       AS span_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                       AS error_count,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS avg_latency_ms,
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
	if err := r.db.Select(ctx, &dtos, query, shared.BaseParams(teamID, startMs, endMs)...); err != nil {
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
