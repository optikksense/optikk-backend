package latency

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const spansRawTable = "observability.spans"

type Repository struct{ db clickhouse.Conn }

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

func (r *Repository) Histogram(ctx context.Context, teamID int64, req HistogramRequest) (histogramRow, error) {
	where, args := baseFilter(teamID, req.StartMs, req.EndMs, req.ServiceName)
	if req.Operation != "" {
		where += ` AND name = @op`
		args = append(args, clickhouse.Named("op", req.Operation))
	}
	query := fmt.Sprintf(`SELECT
		quantile(0.50)(duration_nano)/1e6 AS p50,
		quantile(0.90)(duration_nano)/1e6 AS p90,
		quantile(0.95)(duration_nano)/1e6 AS p95,
		quantile(0.99)(duration_nano)/1e6 AS p99,
		max(duration_nano)/1e6 AS max,
		avg(duration_nano)/1e6 AS avg
		FROM %s WHERE %s`, spansRawTable, where)
	rows, err := r.db.Query(dbutil.ExplorerCtx(ctx), query, args...)
	if err != nil {
		return histogramRow{}, err
	}
	defer rows.Close()
	var out histogramRow
	if rows.Next() {
		if err := rows.Scan(&out.P50, &out.P90, &out.P95, &out.P99, &out.Max, &out.Avg); err != nil {
			return histogramRow{}, err
		}
	}
	return out, nil
}

func (r *Repository) Heatmap(ctx context.Context, teamID int64, req HeatmapRequest) ([]heatmapRow, error) {
	where, args := baseFilter(teamID, req.StartMs, req.EndMs, req.ServiceName)
	bucketExpr := utils.ExprForColumnTime(req.StartMs, req.EndMs, "timestamp")
	query := fmt.Sprintf(`SELECT %s AS time_bucket,
		round(log10(max(duration_nano, 1))*10)/10 AS bucket_ms,
		count() AS count
		FROM %s WHERE %s GROUP BY time_bucket, bucket_ms ORDER BY time_bucket ASC, bucket_ms ASC LIMIT 5000`,
		bucketExpr, spansRawTable, where,
	)
	var rows []heatmapRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func baseFilter(teamID int64, startMs, endMs int64, service string) (string, []any) {
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.Unix(0, startMs*1_000_000)),
		clickhouse.Named("end", time.Unix(0, endMs*1_000_000)),
	}
	where := `team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end`
	if service != "" {
		where += ` AND service_name = @service`
		args = append(args, clickhouse.Named("service", service))
	}
	return where, args
}
