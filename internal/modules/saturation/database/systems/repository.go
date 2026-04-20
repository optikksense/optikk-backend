package systems

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

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

type detectedErrorLegDTO struct {
	DBSystem   string `ch:"db_system"`
	ErrorCount uint64 `ch:"error_count"`
}

// GetDetectedSystems emits per-system counters + raw latency sum/count.
// Percentiles + avg latency come from the service (DbOpLatency sketch and
// in-Go sum/count divide). The previous `sumIf` error count is split into a
// second parallel scan keyed by db_system — both aggregates stay pure
// `sum(hist_count)` under plain WHERE filters.
func (r *ClickHouseRepository) GetDetectedSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	serverAttr := shared.AttrString(shared.AttrServerAddress)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	params := shared.BaseParams(teamID, startMs, endMs)

	totalsQuery := fmt.Sprintf(`
		SELECT
		    %s                                    AS db_system,
		    sum(hist_count)                       AS span_count,
		    sum(hist_sum)                         AS latency_sum,
		    sum(hist_count)                       AS latency_count,
		    sum(hist_count)                       AS query_count,
		    any(%s)                               AS server_address,
		    max(timestamp)                        AS last_seen
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
		serverAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		systemAttr,
	)

	errorsQuery := fmt.Sprintf(`
		SELECT
		    %s              AS db_system,
		    sum(hist_count) AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  AND notEmpty(%s)
		GROUP BY db_system
	`,
		systemAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		systemAttr,
		errorAttr,
	)

	var (
		totals []detectedSystemDTO
		errs   []detectedErrorLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, totalsQuery, params...)
	})
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, errorsQuery, params...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.DBSystem] = e.ErrorCount
	}

	out := make([]DetectedSystem, len(totals))
	for i, d := range totals {
		out[i] = DetectedSystem{
			DBSystem:      d.DBSystem,
			SpanCount:     int64(d.SpanCount),  //nolint:gosec // tenant-scoped count fits int64
			ErrorCount:    int64(errIdx[d.DBSystem]), //nolint:gosec // tenant-scoped count fits int64
			LatencySum:    d.LatencySum,
			LatencyCount:  int64(d.LatencyCount), //nolint:gosec // tenant-scoped count fits int64
			QueryCount:    int64(d.QueryCount),   //nolint:gosec // tenant-scoped count fits int64
			ServerAddress: d.ServerAddress,
			LastSeen:      d.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}
