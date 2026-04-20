package system

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetSystemLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]LatencyTimeSeries, error)
	GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error)
	GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error)
	GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type latencyDTO struct {
	TimeBucket   string   `ch:"time_bucket"`
	GroupBy      string   `ch:"group_by"`
	LatencySum   float64  `ch:"latency_sum"`
	LatencyCount uint64   `ch:"latency_count"`
	P50Ms        *float64 `ch:"p50_ms"`
	P95Ms        *float64 `ch:"p95_ms"`
	P99Ms        *float64 `ch:"p99_ms"`
}

type collectionRowDTO struct {
	CollectionName string   `ch:"collection_name"`
	LatencySum     float64  `ch:"latency_sum"`
	LatencyCount   uint64   `ch:"latency_count"`
	P99Ms          *float64 `ch:"p99_ms"`
	OpsRaw         uint64   `ch:"ops_raw"`
}

type opsRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	GroupBy    string `ch:"group_by"`
	OpCount    uint64 `ch:"op_count"`
}

type errorRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	GroupBy    string `ch:"group_by"`
	ErrorCount uint64 `ch:"error_count"`
}

type namespaceRawDTO struct {
	Namespace string `ch:"namespace"`
	SpanCount uint64 `ch:"span_count"`
}

// GetSystemLatency emits raw sum/count + p* placeholders for a given
// db.system. Percentiles are attached by the service from the DbOpLatency
// sketch keyed on operation (middle of dim tuple) — see service.go comment
// for the limitation on per-operation percentiles.
func (r *ClickHouseRepository) GetSystemLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                           AS time_bucket,
		    %s                           AS group_by,
		    sum(hist_sum)                AS latency_sum,
		    sum(hist_count)              AS latency_count,
		    CAST(0 AS Nullable(Float64)) AS p50_ms,
		    CAST(0 AS Nullable(Float64)) AS p95_ms,
		    CAST(0 AS Nullable(Float64)) AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @dbSystem
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	params = append(params, fargs...)
	var dtos []latencyDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}
	rows := make([]LatencyTimeSeries, len(dtos))
	for i, d := range dtos {
		rows[i] = LatencyTimeSeries{
			TimeBucket:   d.TimeBucket,
			GroupBy:      d.GroupBy,
			P50Ms:        d.P50Ms,
			P95Ms:        d.P95Ms,
			P99Ms:        d.P99Ms,
			LatencySum:   d.LatencySum,
			LatencyCount: int64(d.LatencyCount), //nolint:gosec // tenant-scoped histogram count fits int64
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    %s              AS group_by,
		    sum(hist_count) AS op_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @dbSystem
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	params = append(params, fargs...)
	var dtos []opsRawDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}
	out := make([]OpsTimeSeries, len(dtos))
	for i, d := range dtos {
		rate := float64(d.OpCount) / bucketSec
		out[i] = OpsTimeSeries{
			TimeBucket: d.TimeBucket,
			GroupBy:    d.GroupBy,
			OpsPerSec:  &rate,
		}
	}
	return out, nil
}

// GetSystemTopCollectionsByLatency & ...ByVolume emit raw sum/count + a p99
// placeholder that the service fills from the DbOpLatency sketch via a
// per-collection dim prefix (`<system>|<any-op>|<collection>|`). Since dims
// key on operation before collection, we can't prefix-scan to one
// collection cleanly; the service loads the full tenant map and filters in
// Go — acceptable at the ~20-row limit these endpoints return. ops_per_sec
// is computed Go-side from ops_raw / bucket_width_seconds.
func (r *ClickHouseRepository) GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	return r.topCollections(ctx, teamID, startMs, endMs, dbSystem, "latency_count DESC")
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	return r.topCollections(ctx, teamID, startMs, endMs, dbSystem, "ops_raw DESC")
}

func (r *ClickHouseRepository) topCollections(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem, orderBy string) ([]SystemCollectionRow, error) {
	collAttr := shared.AttrString(shared.AttrDBCollectionName)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                           AS collection_name,
		    sum(hist_sum)                AS latency_sum,
		    sum(hist_count)              AS latency_count,
		    CAST(0 AS Nullable(Float64)) AS p99_ms,
		    sum(hist_count)              AS ops_raw
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @dbSystem
		  AND notEmpty(%s)
		GROUP BY collection_name
		ORDER BY %s
		LIMIT 20
	`,
		collAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		collAttr,
		orderBy,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	var dtos []collectionRowDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}
	rows := make([]SystemCollectionRow, len(dtos))
	for i, d := range dtos {
		rate := float64(d.OpsRaw) / bucketSec
		rows[i] = SystemCollectionRow{
			CollectionName: d.CollectionName,
			P99Ms:          d.P99Ms,
			OpsPerSec:      &rate,
			LatencySum:     d.LatencySum,
			LatencyCount:   int64(d.LatencyCount), //nolint:gosec // tenant-scoped histogram count fits int64
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    %s              AS group_by,
		    sum(hist_count) AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @dbSystem
		  AND notEmpty(%s)
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		shared.AttrString(shared.AttrErrorType),
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	var dtos []errorRawDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}
	out := make([]ErrorTimeSeries, len(dtos))
	for i, d := range dtos {
		rate := float64(d.ErrorCount) / bucketSec
		out[i] = ErrorTimeSeries{
			TimeBucket:   d.TimeBucket,
			GroupBy:      d.GroupBy,
			ErrorsPerSec: &rate,
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	nsAttr := shared.AttrString(shared.AttrDBNamespace)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS namespace,
		    sum(hist_count) AS span_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @dbSystem
		  AND notEmpty(%s)
		GROUP BY namespace
		ORDER BY span_count DESC
	`,
		nsAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		nsAttr,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	var dtos []namespaceRawDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}
	out := make([]SystemNamespace, len(dtos))
	for i, d := range dtos {
		out[i] = SystemNamespace{
			Namespace: d.Namespace,
			SpanCount: int64(d.SpanCount), //nolint:gosec // tenant-scoped histogram count fits int64
		}
	}
	return out, nil
}
