package errors

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const spansRawTable = "observability.signoz_index_v3"

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository	{ return &Repository{db: db} }

func (r *Repository) ErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, service string, limit int) ([]errorGroupRow, error) {
	if limit <= 0 {
		limit = 50
	}
	where := `team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end AND has_error`
	args := baseArgs(teamID, startMs, endMs)
	if service != "" {
		where += ` AND service_name = @service`
		args = append(args, clickhouse.Named("service", service))
	}
	query := fmt.Sprintf(`
		SELECT mat_exception_type AS exception_type, status_message, service_name, count() AS count
		FROM %s WHERE %s
		GROUP BY mat_exception_type, status_message, service_name
		ORDER BY count DESC LIMIT %d`, spansRawTable, where, limit)
	var rows []errorGroupRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "errors.ErrorGroups", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *Repository) Timeseries(ctx context.Context, teamID int64, startMs, endMs int64, service string) ([]timeseriesRow, error) {
	where := `team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end`
	args := baseArgs(teamID, startMs, endMs)
	if service != "" {
		where += ` AND service_name = @service`
		args = append(args, clickhouse.Named("service", service))
	}
	bucket := utils.ExprForColumnTime(startMs, endMs, "timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket, countIf(has_error) AS errors, count() AS total
		FROM %s WHERE %s GROUP BY time_bucket ORDER BY time_bucket ASC`,
		bucket, spansRawTable, where)
	var rows []timeseriesRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "errors.Timeseries", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func baseArgs(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.Unix(0, startMs*1_000_000)),
		clickhouse.Named("end", time.Unix(0, endMs*1_000_000)),
	}
}
