package query

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

// APMBackend evaluates APM monitors against observability.spans_1m.
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

	// Scalar evaluation stays on spans_1m regardless of window: the tracked
	// value divides counts by the exact windowSec, and the 1h tier's
	// hour-snapped edges would skew that math. The 1m tier carries the full
	// 30-day retention, so every alert window is servable.
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
	startMs, endMs = timebucket.SnapRangeForRollup(startMs, endMs)

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
		FROM ` + timebucket.SpansRollup(windowMs) + `
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
		// Apdex is not yet supported and returns 0.
		return 0
	}
	return 0
}

func apmArgs(teamID int64, service string, startMs, endMs int64) []any {
	bs, be := chargs.BucketBounds(startMs, endMs)
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
