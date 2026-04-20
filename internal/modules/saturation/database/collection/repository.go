package collection

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]LatencyTimeSeries, error)
	GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]OpsTimeSeries, error)
	GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]ErrorTimeSeries, error)
	GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters, limit int) ([]CollectionTopQuery, error)
	GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error)
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

type queryTextTotalsDTO struct {
	QueryText            string   `ch:"query_text"`
	DBSystem             string   `ch:"db_system"`
	QueryTextFingerprint string   `ch:"query_fingerprint"`
	P99Ms                *float64 `ch:"p99_ms"`
	CallCount            uint64   `ch:"call_count"`
}

type queryTextErrorLegDTO struct {
	QueryText            string `ch:"query_text"`
	DBSystem             string `ch:"db_system"`
	QueryTextFingerprint string `ch:"query_fingerprint"`
	ErrorCount           uint64 `ch:"error_count"`
}

type readWriteLegDTO struct {
	TimeBucket string `ch:"time_bucket"`
	N          uint64 `ch:"n"`
}

func dbQueryFingerprintAttr() string { return shared.AttrString("db.query.text.fingerprint") }

// GetCollectionLatency emits counts + p* placeholders. Service fills the
// placeholders from the DbOpLatency sketch (merged across operations for
// the given collection).
func (r *ClickHouseRepository) GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                             AS time_bucket,
		    %s                             AS group_by,
		    sum(hist_sum)                  AS latency_sum,
		    sum(hist_count)                AS latency_count,
		    CAST(0 AS Nullable(Float64))   AS p50_ms,
		    CAST(0 AS Nullable(Float64))   AS p95_ms,
		    CAST(0 AS Nullable(Float64))   AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var dtos []latencyDTO
	if err := r.db.Select(database.OverviewCtx(ctx), &dtos, query, params...); err != nil {
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

func (r *ClickHouseRepository) GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]OpsTimeSeries, error) {
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
		  AND %s = @collection
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var dtos []opsRawDTO
	if err := r.db.Select(database.OverviewCtx(ctx), &dtos, query, params...); err != nil {
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

func (r *ClickHouseRepository) GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
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
		  AND %s = @collection
		  AND notEmpty(%s)
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrErrorType),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		shared.AttrString(shared.AttrErrorType),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var dtos []errorRawDTO
	if err := r.db.Select(database.OverviewCtx(ctx), &dtos, query, params...); err != nil {
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

// GetCollectionQueryTexts runs two parallel scans keyed on
// (query_text, db_system, query_fingerprint): totals and errors (WHERE
// notEmpty(error_type)). The service merges call_count + error_count in Go
// and fills P99 from the DbQueryLatency sketch.
func (r *ClickHouseRepository) GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters, limit int) ([]CollectionTopQuery, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	fpAttr := dbQueryFingerprintAttr()

	totalsQuery := fmt.Sprintf(`
		SELECT
		    %s                           AS query_text,
		    %s                           AS db_system,
		    %s                           AS query_fingerprint,
		    CAST(0 AS Nullable(Float64)) AS p99_ms,
		    sum(hist_count)              AS call_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text, db_system, query_fingerprint
		ORDER BY call_count DESC
		LIMIT %d
	`,
		queryAttr, systemAttr, fpAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		queryAttr,
		fc, limit,
	)

	errorsQuery := fmt.Sprintf(`
		SELECT
		    %s              AS query_text,
		    %s              AS db_system,
		    %s              AS query_fingerprint,
		    sum(hist_count) AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  AND notEmpty(%s)
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text, db_system, query_fingerprint
	`,
		queryAttr, systemAttr, fpAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		queryAttr,
		errorAttr,
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)

	var (
		totals []queryTextTotalsDTO
		errs   []queryTextErrorLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(database.OverviewCtx(gctx), &totals, totalsQuery, params...)
	})
	g.Go(func() error {
		return r.db.Select(database.OverviewCtx(gctx), &errs, errorsQuery, params...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[queryKey(e.QueryText, e.DBSystem, e.QueryTextFingerprint)] = e.ErrorCount
	}

	out := make([]CollectionTopQuery, len(totals))
	for i, t := range totals {
		k := queryKey(t.QueryText, t.DBSystem, t.QueryTextFingerprint)
		out[i] = CollectionTopQuery{
			QueryText:            t.QueryText,
			P99Ms:                t.P99Ms,
			CallCount:            int64(t.CallCount),        //nolint:gosec // tenant-scoped histogram count fits int64
			ErrorCount:           int64(errIdx[k]),          //nolint:gosec // tenant-scoped histogram count fits int64
			DBSystem:             t.DBSystem,
			QueryTextFingerprint: t.QueryTextFingerprint,
		}
	}
	return out, nil
}

func queryKey(queryText, dbSystem, fingerprint string) string {
	return queryText + "\x00" + dbSystem + "\x00" + fingerprint
}

// GetCollectionReadVsWrite runs two parallel scans filtered to read vs
// write ops and merges in Go. Replaces the prior `sumIf(..., upper(op) IN
// (...))` combinators.
func (r *ClickHouseRepository) GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	opAttr := shared.AttrString(shared.AttrDBOperationName)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))

	readQuery := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    sum(hist_count) AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  AND upper(%s) IN ('SELECT', 'FIND', 'GET')
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		opAttr,
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
		  AND %s = @collection
		  AND upper(%s) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE')
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		opAttr,
	)

	var (
		reads  []readWriteLegDTO
		writes []readWriteLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(database.OverviewCtx(gctx), &reads, readQuery, params...)
	})
	g.Go(func() error {
		return r.db.Select(database.OverviewCtx(gctx), &writes, writeQuery, params...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	idx := make(map[string]*ReadWritePoint, len(reads)+len(writes))
	order := make([]string, 0, len(reads)+len(writes))
	for _, rd := range reads {
		rate := float64(rd.N) / bucketSec
		p := &ReadWritePoint{TimeBucket: rd.TimeBucket, ReadOpsPerSec: &rate}
		idx[rd.TimeBucket] = p
		order = append(order, rd.TimeBucket)
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
