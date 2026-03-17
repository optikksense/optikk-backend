package errortracking

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	database "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRatePointDTO, error)
	GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotCellDTO, error)
	GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]exceptionRatePointDTO, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.exception_type AS exception_type,
		       count()          AS event_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN ? AND ? AND s.exception_type != '' AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket, exception_type ORDER BY time_bucket ASC`

	var rows []exceptionRatePointDTO
	return rows, r.db.Select(ctx, &rows, query, args...)
}

func (r *ClickHouseRepository) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorHotspotCellDTO, error) {
	var rows []errorHotspotCellDTO
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       s.name AS operation_name,
		       count()                                                                             AS total_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)       AS error_count,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.service_name, s.name
		ORDER BY error_rate DESC
		LIMIT 500
	`,
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]http5xxByRouteDTO, error) {
	query := `
		SELECT s.mat_http_route AS http_route,
		       s.service_name   AS service_name,
		       count()          AS count_5xx
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end
		  AND toUInt16OrZero(s.response_status_code) >= 500`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY http_route, s.service_name ORDER BY count_5xx DESC LIMIT 100`

	var rows []http5xxByRouteDTO
	return rows, r.db.Select(ctx, &rows, query, args...)
}
