package query

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// LogBackend evaluates log monitors against observability.logs. Match uses
// hasToken on the body when the query has free text; structured filters
// (severity, service) come from the scope tags.
type LogBackend struct {
	db clickhouse.Conn
}

func NewLogBackend(db clickhouse.Conn) *LogBackend { return &LogBackend{db: db} }

func (b *LogBackend) Scalar(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, _ models.Scope, _ models.Conditions, now time.Time) (ScalarResult, error) {
	if q.Log == nil {
		return ScalarResult{}, nil
	}
	windowSec := int64(q.Log.WindowSec)
	if windowSec <= 0 {
		windowSec = 300
	}
	endMs := now.UnixMilli()
	startMs := endMs - windowSec*1000

	const query = `
		SELECT count() AS value
		FROM observability.logs
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND (@searchTerm = '' OR hasToken(body, @searchTerm))`

	args := logArgs(m.TeamID, q.Log.Query, startMs, endMs)
	var rows []logCountRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), b.db, "alerting.log.Scalar", &rows, query, args...); err != nil {
		return ScalarResult{}, err
	}
	if len(rows) == 0 {
		return ScalarResult{HasData: false}, nil
	}
	return ScalarResult{Value: float64(rows[0].Value), HasData: true}, nil
}

func (b *LogBackend) Series(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, _ models.Scope, _ models.Conditions, windowMs int64, now time.Time) ([]Point, error) {
	if q.Log == nil {
		return nil, nil
	}
	endMs := now.UnixMilli()
	startMs := endMs - windowMs

	query := `
		SELECT ` + timebucket.DisplayGrainSQL(windowMs) + ` AS bucket,
		       count() AS value
		FROM observability.logs
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND (@searchTerm = '' OR hasToken(body, @searchTerm))
		GROUP BY bucket
		ORDER BY bucket`

	args := logArgs(m.TeamID, q.Log.Query, startMs, endMs)
	var rows []logBucketRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), b.db, "alerting.log.Series", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]Point, 0, len(rows))
	for _, r := range rows {
		out = append(out, Point{BucketMs: r.Bucket.UnixMilli(), Value: float64(r.Value)})
	}
	return out, nil
}

func logArgs(teamID int64, queryText string, startMs, endMs int64) []any {
	bs, be := bucketBounds(startMs, endMs)
	return []any{
		teamIDArg(teamID),
		clickhouse.Named("bucketStart", bs),
		clickhouse.Named("bucketEnd", be),
		clickhouse.Named("searchTerm", strings.ToLower(strings.TrimSpace(queryText))),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

type logCountRow struct {
	Value uint64 `ch:"value"`
}

type logBucketRow struct {
	Bucket time.Time `ch:"bucket"`
	Value  uint64    `ch:"value"`
}
