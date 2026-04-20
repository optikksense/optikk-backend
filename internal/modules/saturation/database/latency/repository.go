package latency

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetLatencyBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByServer(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyHeatmapSamples(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapSample, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// latencyDTO scans sum/count per (time_bucket, group_by). CH returns count as
// UInt64 natively — scan into uint64 and convert to int64 at the DTO boundary
// in the service layer. Percentile columns are placeholder-less; the service
// fills them from the DbOpLatency sketch where the dim-prefix supports it
// (currently only by-system — see service.go).
type latencyDTO struct {
	TimeBucket   string  `ch:"time_bucket"`
	GroupBy      string  `ch:"group_by"`
	LatencySum   float64 `ch:"latency_sum"`
	LatencyCount uint64  `ch:"latency_count"`
}

func (r *ClickHouseRepository) latencySeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                AS time_bucket,
		    %s                AS group_by,
		    sum(hist_sum)     AS latency_sum,
		    sum(hist_count)   AS latency_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(groupAttr),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []latencyDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	rows := make([]LatencyTimeSeries, len(dtos))
	for i, d := range dtos {
		rows[i] = LatencyTimeSeries{
			TimeBucket:   d.TimeBucket,
			GroupBy:      d.GroupBy,
			LatencySum:   d.LatencySum,
			LatencyCount: int64(d.LatencyCount), //nolint:gosec // bounded by ingest window
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLatencyBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetLatencyByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetLatencyByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetLatencyByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBNamespace, f)
}

func (r *ClickHouseRepository) GetLatencyByServer(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrServerAddress, f)
}

// GetLatencyHeatmapSamples returns raw (time_bucket, avg_sec, count) samples
// without any server-side bucket classification. The service layer classifies
// each sample into a latency bucket label in Go. The repository previously
// used a conditional chain + null fallback in SELECT; both are banned in
// this codebase. Filtering `hist_count > 0` in WHERE keeps the plain
// division safe.
func (r *ClickHouseRepository) GetLatencyHeatmapSamples(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapSample, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                     AS time_bucket,
		    hist_sum / hist_count  AS avg_sec,
		    hist_count             AS count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND hist_count > 0
		  %s
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []latencyHeatmapSampleDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	out := make([]LatencyHeatmapSample, len(dtos))
	for i, d := range dtos {
		out[i] = LatencyHeatmapSample{
			TimeBucket: d.TimeBucket,
			AvgSec:     d.AvgSec,
			Count:      int64(d.Count), //nolint:gosec // bounded by ingest window
		}
	}
	return out, nil
}
