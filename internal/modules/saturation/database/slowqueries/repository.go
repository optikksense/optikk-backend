package slowqueries

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
	GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error)
	GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error)
	GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error)
	GetP99ByQueryText(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]P99ByQueryText, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func dbQueryFingerprintAttr() string { return shared.AttrString("db.query.text.fingerprint") }

// patternTotalsDTO is the totals leg of GetSlowQueryPatterns (includes
// P* placeholders that the service fills from DbQueryLatency sketch).
type patternTotalsDTO struct {
	QueryText            string   `ch:"query_text"`
	CollectionName       string   `ch:"collection_name"`
	DBSystem             string   `ch:"db_system"`
	QueryTextFingerprint string   `ch:"query_fingerprint"`
	P50Ms                *float64 `ch:"p50_ms"`
	P95Ms                *float64 `ch:"p95_ms"`
	P99Ms                *float64 `ch:"p99_ms"`
	LatencySum           float64  `ch:"latency_sum"`
	LatencyCount         uint64   `ch:"latency_count"`
	CallCount            uint64   `ch:"call_count"`
}

type patternErrorLegDTO struct {
	QueryText            string `ch:"query_text"`
	CollectionName       string `ch:"collection_name"`
	DBSystem             string `ch:"db_system"`
	QueryTextFingerprint string `ch:"query_fingerprint"`
	ErrorCount           uint64 `ch:"error_count"`
}

type collectionTotalsDTO struct {
	CollectionName string   `ch:"collection_name"`
	DBSystem       string   `ch:"db_system"`
	LatencySum     float64  `ch:"latency_sum"`
	LatencyCount   uint64   `ch:"latency_count"`
	P99Ms          *float64 `ch:"p99_ms"`
	OpsRaw         uint64   `ch:"ops_raw"`
}

type collectionErrorLegDTO struct {
	CollectionName string `ch:"collection_name"`
	DBSystem       string `ch:"db_system"`
	ErrorCount     uint64 `ch:"error_count"`
}

// GetSlowQueryPatterns runs a totals scan and an error-only scan in
// parallel. The previous `sumIf(hist_count, notEmpty(error_type))` is
// replaced by a second scan with `AND notEmpty(error_type)` in the WHERE
// clause; error_count is merged in Go.
func (r *ClickHouseRepository) GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)
	collAttr := shared.AttrString(shared.AttrDBCollectionName)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	fpAttr := dbQueryFingerprintAttr()

	totalsQuery := fmt.Sprintf(`
		SELECT
		    %s                           AS query_text,
		    %s                           AS collection_name,
		    %s                           AS db_system,
		    %s                           AS query_fingerprint,
		    CAST(0 AS Nullable(Float64)) AS p50_ms,
		    CAST(0 AS Nullable(Float64)) AS p95_ms,
		    CAST(0 AS Nullable(Float64)) AS p99_ms,
		    sum(hist_sum)                AS latency_sum,
		    sum(hist_count)              AS latency_count,
		    sum(hist_count)              AS call_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY query_text, collection_name, db_system, query_fingerprint
		ORDER BY latency_count DESC
		LIMIT %d
	`,
		queryAttr, collAttr, systemAttr, fpAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc, limit,
	)

	errorsQuery := fmt.Sprintf(`
		SELECT
		    %s              AS query_text,
		    %s              AS collection_name,
		    %s              AS db_system,
		    %s              AS query_fingerprint,
		    sum(hist_count) AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text, collection_name, db_system, query_fingerprint
	`,
		queryAttr, collAttr, systemAttr, fpAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		errorAttr,
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	var (
		totals []patternTotalsDTO
		errs   []patternErrorLegDTO
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
		errIdx[patternKey(e.QueryText, e.CollectionName, e.DBSystem, e.QueryTextFingerprint)] = e.ErrorCount
	}

	rows := make([]SlowQueryPattern, len(totals))
	for i, t := range totals {
		k := patternKey(t.QueryText, t.CollectionName, t.DBSystem, t.QueryTextFingerprint)
		rows[i] = SlowQueryPattern{
			QueryText:            t.QueryText,
			CollectionName:       t.CollectionName,
			P50Ms:                t.P50Ms,
			P95Ms:                t.P95Ms,
			P99Ms:                t.P99Ms,
			CallCount:            int64(t.CallCount),        //nolint:gosec // tenant-scoped histogram count fits int64
			ErrorCount:           int64(errIdx[k]),          //nolint:gosec // tenant-scoped histogram count fits int64
			DBSystem:             t.DBSystem,
			QueryTextFingerprint: t.QueryTextFingerprint,
			LatencySum:           t.LatencySum,
			LatencyCount:         int64(t.LatencyCount),     //nolint:gosec // tenant-scoped histogram count fits int64
		}
	}
	return rows, nil
}

func patternKey(queryText, collectionName, dbSystem, fingerprint string) string {
	return queryText + "\x00" + collectionName + "\x00" + dbSystem + "\x00" + fingerprint
}

// GetSlowestCollections runs totals + errors as two parallel scans and
// divides ops_per_sec / error_rate in Go. Replaces the prior SQL-side
// `sumIf(...) / nullIf(sum(...), 0)` expression.
func (r *ClickHouseRepository) GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error) {
	fc, fargs := shared.FilterClauses(f)
	collAttr := shared.AttrString(shared.AttrDBCollectionName)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	totalsQuery := fmt.Sprintf(`
		SELECT
		    %s                           AS collection_name,
		    %s                           AS db_system,
		    sum(hist_sum)                AS latency_sum,
		    sum(hist_count)              AS latency_count,
		    CAST(0 AS Nullable(Float64)) AS p99_ms,
		    sum(hist_count)              AS ops_raw
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY collection_name, db_system
		ORDER BY latency_count DESC
		LIMIT 50
	`,
		collAttr, systemAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		collAttr,
		fc,
	)

	errorsQuery := fmt.Sprintf(`
		SELECT
		    %s              AS collection_name,
		    %s              AS db_system,
		    sum(hist_count) AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  AND notEmpty(%s)
		  %s
		GROUP BY collection_name, db_system
	`,
		collAttr, systemAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		collAttr,
		errorAttr,
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	var (
		totals []collectionTotalsDTO
		errs   []collectionErrorLegDTO
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
		errIdx[e.CollectionName+"\x00"+e.DBSystem] = e.ErrorCount
	}

	rows := make([]SlowCollectionRow, len(totals))
	for i, t := range totals {
		opsRate := float64(t.OpsRaw) / bucketSec
		var errRate *float64
		if t.OpsRaw > 0 {
			v := float64(errIdx[t.CollectionName+"\x00"+t.DBSystem]) / float64(t.OpsRaw) * 100
			errRate = &v
		}
		rows[i] = SlowCollectionRow{
			CollectionName: t.CollectionName,
			P99Ms:          t.P99Ms,
			OpsPerSec:      &opsRate,
			ErrorRate:      errRate,
			DBSystem:       t.DBSystem,
			LatencySum:     t.LatencySum,
			LatencyCount:   int64(t.LatencyCount), //nolint:gosec // tenant-scoped histogram count fits int64
		}
	}
	return rows, nil
}

// slowRateRawDTO is the per-bucket raw count of rows whose per-row
// histogram mean exceeds the slow threshold.
type slowRateRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	N          uint64 `ch:"n"`
}

// GetSlowQueryRate filters rows whose (hist_sum / hist_count) > threshold
// in WHERE and sums hist_count. The per-row divide is a row-local arithmetic
// filter (not an aggregate); skipping zero-count rows via `hist_count > 0`
// keeps the divide well-defined without `nullIf`.
func (r *ClickHouseRepository) GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	thresholdSec := thresholdMs / 1000.0

	query := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    sum(hist_count) AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND hist_count > 0
		  AND (hist_sum / hist_count) > %f
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		thresholdSec,
		fc,
	)

	var dtos []slowRateRawDTO
	if err := r.db.Select(database.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	out := make([]SlowRatePoint, len(dtos))
	for i, d := range dtos {
		rate := float64(d.N) / bucketSec
		out[i] = SlowRatePoint{
			TimeBucket: d.TimeBucket,
			SlowPerSec: &rate,
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetP99ByQueryText(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]P99ByQueryText, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	fpAttr := dbQueryFingerprintAttr()

	query := fmt.Sprintf(`
		SELECT
		    %s                            AS query_text,
		    %s                            AS db_system,
		    %s                            AS query_fingerprint,
		    CAST(0 AS Nullable(Float64))  AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text, db_system, query_fingerprint
		ORDER BY sum(hist_count) DESC
		LIMIT %d
	`,
		queryAttr, systemAttr, fpAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		queryAttr,
		fc, limit,
	)

	var rows []P99ByQueryText
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}
