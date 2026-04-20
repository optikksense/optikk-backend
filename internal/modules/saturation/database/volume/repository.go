package volume

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetOpsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
	GetOpsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
	GetOpsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
	GetReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ReadWritePoint, error)
	GetOpsByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) opsSeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]OpsTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	groupExpr := shared.AttrString(groupAttr)

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
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, groupExpr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []opsRawDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
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

func (r *ClickHouseRepository) GetOpsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetOpsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetOpsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetOpsByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBNamespace, f)
}

// readWriteLegDTO is the per-bucket hist_count aggregate for a fixed
// read/write operation set. The service merges the two legs in Go to
// produce ReadWritePoint.
type readWriteLegDTO struct {
	TimeBucket string `ch:"time_bucket"`
	N          uint64 `ch:"n"`
}

// GetReadVsWrite runs two parallel scans — one filtered to read ops, one to
// write ops — and merges in Go. Replaces the prior `sumIf(hist_count, upper(op) IN (...))`
// combinator with a plain WHERE filter + sum().
func (r *ClickHouseRepository) GetReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ReadWritePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	opAttr := shared.AttrString(shared.AttrDBOperationName)
	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	readQuery := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    sum(hist_count) AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND upper(%s) IN ('SELECT', 'FIND', 'GET')
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		opAttr,
		fc,
	)

	writeQuery := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    sum(hist_count) AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND upper(%s) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE')
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		opAttr,
		fc,
	)

	var (
		reads  []readWriteLegDTO
		writes []readWriteLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &reads, readQuery, params...)
	})
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &writes, writeQuery, params...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	idx := make(map[string]*ReadWritePoint, len(reads)+len(writes))
	order := make([]string, 0, len(reads)+len(writes))
	for _, r := range reads {
		rate := float64(r.N) / bucketSec
		if p, ok := idx[r.TimeBucket]; ok {
			p.ReadOpsPerSec = &rate
		} else {
			p := &ReadWritePoint{TimeBucket: r.TimeBucket, ReadOpsPerSec: &rate}
			idx[r.TimeBucket] = p
			order = append(order, r.TimeBucket)
		}
	}
	for _, w := range writes {
		rate := float64(w.N) / bucketSec
		if p, ok := idx[w.TimeBucket]; ok {
			p.WriteOpsPerSec = &rate
		} else {
			p := &ReadWritePoint{TimeBucket: w.TimeBucket, WriteOpsPerSec: &rate}
			idx[w.TimeBucket] = p
			order = append(order, w.TimeBucket)
		}
	}

	out := make([]ReadWritePoint, 0, len(order))
	for _, k := range order {
		out = append(out, *idx[k])
	}
	return out, nil
}
