package errors

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/resource"
)

const spansRawTable = "observability.spans"

// resolveFingerprintsForService synthesizes a minimal querycompiler.Filters
// from a single optional service name and routes through the shared resolver.
// Returns the narrowed where + args, or empty=true when the service has no
// resolvable fingerprints in the window (caller should short-circuit).
func (r *Repository) resolveFingerprintsForService(
	ctx context.Context, teamID, startMs, endMs int64, service, where string, args []any,
) (string, []any, bool, error) {
	if service == "" {
		return where, args, false, nil
	}
	f := querycompiler.Filters{TeamID: teamID, StartMs: startMs, EndMs: endMs, Services: []string{service}}
	return resource.WithFingerprints(ctx, r.db, f, where, args)
}

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository	{ return &Repository{db: db} }

func (r *Repository) ErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, service string, limit int) ([]errorGroupRow, error) {
	if limit <= 0 {
		limit = 50
	}
	where := `team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end AND has_error`
	args := baseArgs(teamID, startMs, endMs)
	where, args, empty, err := r.resolveFingerprintsForService(ctx, teamID, startMs, endMs, service, where, args)
	if err != nil {
		return nil, err
	}
	if empty {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT attr_exception_type AS exception_type, status_message, service, count() AS count
		FROM %s WHERE %s
		GROUP BY attr_exception_type, status_message, service
		ORDER BY count DESC LIMIT %d`, spansRawTable, where, limit)
	var rows []errorGroupRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "errors.ErrorGroups", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *Repository) Timeseries(ctx context.Context, teamID int64, startMs, endMs int64, service string) ([]timeseriesRow, error) {
	where := `team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end`
	args := baseArgs(teamID, startMs, endMs)
	where, args, empty, err := r.resolveFingerprintsForService(ctx, teamID, startMs, endMs, service, where, args)
	if err != nil {
		return nil, err
	}
	if empty {
		return nil, nil
	}
	// Bucket is the stored ts_bucket (5-min grain). No CH-side bucket
	// math — see internal/infra/timebucket.
	_, _ = startMs, endMs
	query := fmt.Sprintf(`
		SELECT toDateTime(ts_bucket) AS time_bucket, countIf(has_error) AS errors, count() AS total
		FROM %s WHERE %s GROUP BY time_bucket ORDER BY time_bucket ASC`,
		spansRawTable, where)
	var rows []timeseriesRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "errors.Timeseries", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func baseArgs(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.Unix(0, startMs*1_000_000)),
		clickhouse.Named("end", time.Unix(0, endMs*1_000_000)),
	}
}
