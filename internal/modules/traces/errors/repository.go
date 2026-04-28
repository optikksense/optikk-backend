package errors

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// ErrorGroups returns the top-N error groups (exception_type × status_message
// × service) over the filtered window.
func (r *Repository) ErrorGroups(ctx context.Context, f filter.Filters, limit int) ([]errorGroupRow, error) {
	if limit <= 0 {
		limit = 50
	}
	resourceWhere, where, args := filter.BuildClauses(f)
	args = append(args, clickhouse.Named("groupLimit", uint64(limit))) //nolint:gosec

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT attr_exception_type AS exception_type,
		       status_message,
		       service,
		       count() AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND has_error` + where + `
		GROUP BY attr_exception_type, status_message, service
		ORDER BY count DESC
		LIMIT @groupLimit`

	var rows []errorGroupRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "errors.ErrorGroups", &rows, query, args...)
}

// Timeseries returns per-bucket (errors, total) over the filtered window. The
// 5-minute ts_bucket grain is preserved on the wire.
func (r *Repository) Timeseries(ctx context.Context, f filter.Filters) ([]timeseriesRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT toString(toDateTime(ts_bucket))   AS time_bucket,
		       countIf(has_error)                AS errors,
		       count()                           AS total
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where + `
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`

	var rows []timeseriesRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "errors.Timeseries", &rows, query, args...)
}
