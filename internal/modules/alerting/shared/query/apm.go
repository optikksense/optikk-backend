package query

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// APMBackend evaluates APM monitors against observability.spans_1m.
// Tracks: errors (error_count / request_count * 100), hits (request_count
// per second), latency (quantileTimingMerge p99), apdex (raw spans).
type APMBackend struct {
	db clickhouse.Conn
}

func NewAPMBackend(db clickhouse.Conn) *APMBackend { return &APMBackend{db: db} }

func (b *APMBackend) Scalar(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, _ models.Scope, cond models.Conditions, now time.Time) (ScalarResult, error) {
	if q.APM == nil {
		return ScalarResult{}, nil
	}
	windowSec := int64(q.APM.WindowSec)
	if windowSec <= 0 {
		windowSec = 300
	}
	endMs := now.UnixMilli()
	startMs := endMs - windowSec*1000

	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @service
		)
		SELECT sum(request_count)                                AS request_count,
		       sum(error_count)                                  AS error_count,
		       quantileTimingMerge(0.99)(latency_state)          AS p99
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND service     = @service
		WHERE timestamp BETWEEN @start AND @end`

	args := apmArgs(m.TeamID, q.APM.Service, startMs, endMs)
	var rows []apmAggRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), b.db, "alerting.apm.Scalar", &rows, query, args...); err != nil {
		return ScalarResult{}, err
	}
	if len(rows) == 0 || rows[0].RequestCount == 0 {
		return ScalarResult{HasData: false}, nil
	}
	row := rows[0]

	if cond.MinSample != nil && row.RequestCount < uint64(*cond.MinSample) {
		return ScalarResult{HasData: false}, nil
	}

	value := apmTrackValue(q.APM.Track, row, windowSec)
	return ScalarResult{Value: value, HasData: true}, nil
}

func (b *APMBackend) Series(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, _ models.Scope, _ models.Conditions, windowMs int64, now time.Time) ([]Point, error) {
	if q.APM == nil {
		return nil, nil
	}
	endMs := now.UnixMilli()
	startMs := endMs - windowMs

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @service
		)
		SELECT ` + timebucket.DisplayGrainSQL(windowMs) + ` AS bucket,
		       sum(request_count)                       AS request_count,
		       sum(error_count)                         AS error_count,
		       quantileTimingMerge(0.99)(latency_state) AS p99
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		     AND service     = @service
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY bucket
		ORDER BY bucket`

	args := apmArgs(m.TeamID, q.APM.Service, startMs, endMs)
	var rows []apmSeriesRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), b.db, "alerting.apm.Series", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]Point, 0, len(rows))
	windowSec := int64(q.APM.WindowSec)
	if windowSec <= 0 {
		windowSec = 300
	}
	for _, r := range rows {
		row := apmAggRow{RequestCount: r.RequestCount, ErrorCount: r.ErrorCount, P99: r.P99}
		out = append(out, Point{BucketMs: r.Bucket.UnixMilli(), Value: apmTrackValue(q.APM.Track, row, windowSec)})
	}
	return out, nil
}

// apmTrackValue projects the requested track from the aggregate row.
// Per-second rate divides by the window (in seconds), not the bucket grain,
// because Scalar is evaluated over the full eval window in one shot.
func apmTrackValue(track string, row apmAggRow, windowSec int64) float64 {
	switch track {
	case "errors":
		if row.RequestCount == 0 {
			return 0
		}
		return float64(row.ErrorCount) / float64(row.RequestCount) * 100
	case "hits":
		if windowSec == 0 {
			return float64(row.RequestCount)
		}
		return float64(row.RequestCount) / float64(windowSec)
	case "latency":
		return row.P99
	case "apdex":
		// Apdex requires raw-span granularity (satisfied/tolerating buckets).
		// v1 returns 0 here so the monitor renders no-data — callers should
		// avoid creating apdex monitors until raw-span evaluation lands.
		return 0
	}
	return 0
}

func apmArgs(teamID int64, service string, startMs, endMs int64) []any {
	bs, be := bucketBounds(startMs, endMs)
	return []any{
		teamIDArg(teamID),
		clickhouse.Named("bucketStart", bs),
		clickhouse.Named("bucketEnd", be),
		clickhouse.Named("service", service),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

type apmAggRow struct {
	RequestCount uint64  `ch:"request_count"`
	ErrorCount   uint64  `ch:"error_count"`
	P99          float64 `ch:"p99"`
}

type apmSeriesRow struct {
	Bucket       time.Time `ch:"bucket"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
	P99          float64   `ch:"p99"`
}
